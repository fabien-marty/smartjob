import asyncio
import datetime
import shlex
from dataclasses import dataclass
from typing import cast

import google.api_core.exceptions as gac_exceptions
from google.cloud import run_v2
from stlog import getLogger

from smartjob.app.executor import SmartJobExecutionResultFuture, SmartJobExecutorPort
from smartjob.app.job import CloudRunSmartJob, SmartJob, SmartJobExecutionResult

logger = getLogger("smartjob.executor.cloudrun")


@dataclass(frozen=True)
class JobListCacheKey:
    project: str
    region: str


class CloudRunSmartJobExecutionResultFuture(SmartJobExecutionResultFuture):
    def _get_result_from_future(
        self, future: asyncio.Future
    ) -> SmartJobExecutionResult:
        try:
            result = future.result()
            return self._get_result(result)
        except gac_exceptions.GoogleAPICallError:
            pass
        except asyncio.CancelledError:
            self.job.cancelled = True
        return SmartJobExecutionResult(
            job=self.job,
            success=False,
        )

    def _get_result(self, task_result) -> SmartJobExecutionResult:
        success = False
        castedResult = cast(run_v2.Execution, task_result)
        success = castedResult.succeeded_count > 0
        return SmartJobExecutionResult(
            job=self.job,
            success=success,
        )


class CloudRunSmartJobExecutor(SmartJobExecutorPort):
    def __init__(self):
        self.job_list_cache: dict[JobListCacheKey, list[str]] | None = None
        self.client_cache: run_v2.JobsAsyncClient | None = None
        self.client_cache_lock = asyncio.Lock()

    @property
    def client(self) -> run_v2.JobsAsyncClient:
        # lazy-init to circumvent some 'attached to a different loop' issues
        if self.client_cache is None:
            self.client_cache = run_v2.JobsAsyncClient()
        return self.client_cache

    async def get_job_list_cache(self, project, region) -> list[str]:
        key = JobListCacheKey(project, region)
        if self.job_list_cache is not None:
            if key in self.job_list_cache:
                return self.job_list_cache[key]
        else:
            self.job_list_cache = {}
        logger.info(
            "Fetching Cloud Run Job list for %s/%s (to cache)...",
            project,
            region,
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
        if self.job_list_cache is None:
            return
        key = JobListCacheKey(project, region)
        if key in self.job_list_cache:
            self.job_list_cache.pop(key)

    async def create_job_if_needed(self, job: CloudRunSmartJob):
        async with self.client_cache_lock:
            job_list_cache = await self.get_job_list_cache(job.project, job.region)
            if job.full_cloud_run_job_name in job_list_cache:
                # it already exists
                logger.debug(
                    "Cloud Run Job: %s already exists => great!",
                    job.full_cloud_run_job_name,
                )
                return
            # it does not exist => let's create it
            volumes: list[run_v2.Volume] = []
            volume_mounts: list[run_v2.VolumeMount] = []
            launch_stage = "GA"
            if job.staging_bucket_name:
                volumes.append(
                    run_v2.Volume(
                        name="staging",
                        gcs=run_v2.GCSVolumeSource(
                            bucket=job.staging_bucket_name, read_only=True
                        ),
                    )
                )
                volume_mounts.append(
                    run_v2.VolumeMount(name="staging", mount_path="/staging")
                )
                launch_stage = "BETA"
            if job.input_bucket_name:
                volumes.append(
                    run_v2.Volume(
                        name="input",
                        gcs=run_v2.GCSVolumeSource(
                            bucket=job.input_bucket_name, read_only=True
                        ),
                    )
                )
                volume_mounts.append(
                    run_v2.VolumeMount(name="input", mount_path="/input")
                )
            if job.output_bucket_name:
                volumes.append(
                    run_v2.Volume(
                        name="output",
                        gcs=run_v2.GCSVolumeSource(
                            bucket=job.output_bucket_name, read_only=False
                        ),
                    )
                )
                volume_mounts.append(
                    run_v2.VolumeMount(name="output", mount_path="/output")
                )
            request = run_v2.CreateJobRequest(
                parent=job._parent_name,
                job_id=job.cloud_run_job_name,
                job=run_v2.Job(
                    labels={
                        "smartjob": "true",
                        "smartjob.namespace": job.namespace,
                    },
                    launch_stage=launch_stage,
                    template=run_v2.ExecutionTemplate(
                        task_count=1,
                        template=run_v2.TaskTemplate(
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
                                            "cpu": job._cpuLimit,
                                            "memory": job._memoryLimit,
                                        },
                                    ),
                                    volume_mounts=volume_mounts,
                                )
                            ],
                            volumes=volumes,
                            max_retries=1,
                            service_account=job.service_account,
                        ),
                    ),
                ),
            )
            logger.info(
                "Let's create a new Cloud Run Job: %s...", job.cloud_run_job_name
            )
            operation = await self.client.create_job(request=request)
            await operation.result()
            logger.debug("Done creating Cloud Run Job: %s", job.cloud_run_job_name)
            self.reset_job_list_cache(job.project, job.region)

    async def schedule(self, job: SmartJob) -> SmartJobExecutionResultFuture:
        if not job.created:
            job.created = datetime.datetime.now(tz=datetime.timezone.utc)
        if not job.execution_id:
            job.set_execution_id()
        job = cast(CloudRunSmartJob, job)
        await self.create_job_if_needed(job)
        request = run_v2.RunJobRequest(
            name=job.full_cloud_run_job_name,
            overrides=run_v2.RunJobRequest.Overrides(
                task_count=1,
                # timeout=job.timeout_seconds,
                container_overrides=[
                    run_v2.RunJobRequest.Overrides.ContainerOverride(
                        name="container-1",
                        args=job.overridden_args,
                        env=[
                            run_v2.EnvVar(name=k, value=v)
                            for k, v in job.overridden_envs.items()
                        ],
                    )
                ],
            ),
        )
        logger.info(
            "Let's trigger a new execution of Cloud Run Job: %s...",
            job.cloud_run_job_name,
            docker_image=job.docker_image,
            overridden_args=shlex.join(job.overridden_args),
            overridden_envs=", ".join(
                [f"{x}={y}" for x, y in job.overridden_envs.items()]
            ),
            timeout_s=job.timeout_seconds,
        )
        operation = await self.client.run_job(request=request)
        job.log_url = operation.metadata.log_uri
        return CloudRunSmartJobExecutionResultFuture(operation.result(), job=job)