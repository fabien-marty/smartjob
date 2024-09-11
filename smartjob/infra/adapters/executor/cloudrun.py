import asyncio
import json
from dataclasses import dataclass, field
from typing import cast

import google.api_core.exceptions as gac_exceptions
from google.cloud import run_v2
from google.protobuf.duration_pb2 import Duration
from stlog import LogContext, getLogger

from smartjob.app.execution import Execution, ExecutionResult
from smartjob.app.executor import ExecutionResultFuture
from smartjob.app.utils import hex_hash
from smartjob.infra.adapters.executor.gcp import GCPExecutionResultFuture, GCPExecutor

logger = getLogger("smartjob.executor.cloudrun")


@dataclass(frozen=True)
class JobListCacheKey:
    project: str
    region: str


class CloudRunExecutionResultFuture(GCPExecutionResultFuture):
    def _get_result_from_future(self, future: asyncio.Future) -> ExecutionResult:
        try:
            task_result = future.result()
            success = False
            castedResult = cast(run_v2.Execution, task_result)
            success = castedResult.succeeded_count > 0
            return ExecutionResult._from_execution(
                self._execution, success, self.log_url
            )
        except gac_exceptions.GoogleAPICallError:
            pass
        except asyncio.CancelledError:
            self._execution.cancelled = True
        return ExecutionResult._from_execution(self._execution, False, self.log_url)


@dataclass
class CloudRunExecutorAdapter(GCPExecutor):
    job_list_cache: dict[JobListCacheKey, list[str]] = field(
        default_factory=dict, init=False
    )
    client_cache: run_v2.JobsAsyncClient | None = field(default=None, init=False)
    client_cache_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def job_hash(self, execution: Execution) -> str:
        job = execution.job
        conf = execution.config
        args: list[str] = [job.name, conf._project, conf._region, job.docker_image]
        args += [
            job.namespace,
            conf._staging_bucket,
            conf.service_account or "",
            json.dumps(conf.labels, sort_keys=True) if conf.labels else "",
        ]
        return hex_hash(*args)[:10]

    def parent_name(self, execution: Execution) -> str:
        conf = execution.config
        return f"projects/{conf._project}/locations/{conf._region}"

    def cloud_run_job_name(self, execution: Execution) -> str:
        return f"{execution.job.full_name}-{self.job_hash(execution)}"

    def full_cloud_run_job_name(self, execution: Execution) -> str:
        return (
            f"{self.parent_name(execution)}/jobs/{self.cloud_run_job_name(execution)}"
        )

    @property
    def client(self) -> run_v2.JobsAsyncClient:
        # lazy-init to circumvent some 'attached to a different loop' issues
        if self.client_cache is None:
            self.client_cache = run_v2.JobsAsyncClient()
        return self.client_cache

    async def get_job_list_cache(self, project, region) -> list[str]:
        key = JobListCacheKey(project, region)
        if key in self.job_list_cache:
            return self.job_list_cache[key]
        logger.info(
            "Fetching Cloud Run Job list (to cache)...", project=project, region=region
        )
        jobs = []
        req = run_v2.ListJobsRequest(parent=f"projects/{project}/locations/{region}")
        jobs_pager = await self.client.list_jobs(req)
        async for existing_job in jobs_pager:
            jobs.append(existing_job.name)
        self.job_list_cache[key] = jobs
        logger.info("%i existing Cloud Run Jobs found", len(jobs))
        return jobs

    def reset_job_list_cache(self, project, region):
        key = JobListCacheKey(project, region)
        if key in self.job_list_cache:
            self.job_list_cache.pop(key)

    async def create_job_if_needed(self, execution: Execution):
        job = execution.job
        config = execution.config
        async with self.client_cache_lock:
            job_list_cache = await self.get_job_list_cache(
                execution.config._project, execution.config._region
            )
            if self.full_cloud_run_job_name(execution) in job_list_cache:
                # it already exists
                logger.debug("Cloud Run Job already exists => let's reuse it!")
                return
            # it does not exist => let's create it
            volumes: list[run_v2.Volume] = []
            volume_mounts: list[run_v2.VolumeMount] = []
            launch_stage = "GA"
            volumes.append(
                run_v2.Volume(
                    name="staging",
                    gcs=run_v2.GCSVolumeSource(
                        bucket=config._staging_bucket_name, read_only=False
                    ),
                )
            )
            volume_mounts.append(
                run_v2.VolumeMount(name="staging", mount_path="/staging")
            )
            launch_stage = "BETA"
            request = run_v2.CreateJobRequest(
                parent=self.parent_name(execution),
                job_id=self.cloud_run_job_name(execution),
                job=run_v2.Job(
                    labels=execution.labels,
                    launch_stage=launch_stage,
                    template=run_v2.ExecutionTemplate(
                        task_count=1,
                        template=run_v2.TaskTemplate(
                            max_retries=config._retry_config._max_attempts_execute - 1,
                            execution_environment=run_v2.ExecutionEnvironment(
                                run_v2.ExecutionEnvironment.EXECUTION_ENVIRONMENT_GEN2
                            ),
                            containers=[
                                run_v2.Container(
                                    name="container-1",
                                    image=job.docker_image,
                                    resources=run_v2.ResourceRequirements(
                                        startup_cpu_boost=False,
                                        limits={
                                            "cpu": f"{config._cpu}",
                                            "memory": f"{config._memory_gb}Gi",
                                        },
                                    ),
                                    volume_mounts=volume_mounts,
                                )
                            ],
                            volumes=volumes,
                            service_account=config.service_account,
                        ),
                    ),
                ),
            )
            logger.info("Let's create a new Cloud Run Job...")
            operation = await self.client.create_job(request=request)
            await operation.result()
            logger.debug("Done creating Cloud Run Job")
            self.reset_job_list_cache(config._project, config._region)

    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        job = execution.job
        config = execution.config
        job_id = self.cloud_run_job_name(execution)
        full_name = self.full_cloud_run_job_name(execution)
        with LogContext.bind(
            cloudrun_job_id=job_id, project=config._project, region=config._region
        ):
            await self.create_job_if_needed(execution)
            request = run_v2.RunJobRequest(
                name=full_name,
                overrides=run_v2.RunJobRequest.Overrides(
                    task_count=1,
                    timeout=Duration(seconds=config._timeout_config.timeout_seconds),
                    container_overrides=[
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            name="container-1",
                            args=execution.overridden_args,
                            env=[
                                run_v2.EnvVar(name=k, value=v)
                                for k, v in execution.add_envs.items()
                            ],
                        )
                    ],
                ),
            )
            logger.info(
                "Let's trigger a new execution of Cloud Run Job...",
                docker_image=job.docker_image,
                overridden_args=execution.overridden_args_as_string,
                add_envs=execution.add_envs_as_string,
                timeout_s=config._timeout_config.timeout_seconds,
            )
            operation = await self.client.run_job(request=request)
            return CloudRunExecutionResultFuture(
                asyncio.create_task(operation.result()),
                execution=execution,
                storage_service=self.storage_service,
                log_url=operation.metadata.log_uri,
            )

    def get_name(self) -> str:
        return "cloudrun"

    def staging_mount_path(self, execution: Execution) -> str:
        return "/staging"
