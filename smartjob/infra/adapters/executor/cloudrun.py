import asyncio
import json
from dataclasses import dataclass, field
from typing import cast

import google.api_core.exceptions as gac_exceptions
from google.cloud import run_v2
from google.protobuf.duration_pb2 import Duration
from grpc import StatusCode
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
    client_cache: run_v2.JobsAsyncClient | None = field(default=None, init=False)

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

    async def _create_job_if_needed(self, execution: Execution):
        job = execution.job
        config = execution.config
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
        volume_mounts.append(run_v2.VolumeMount(name="staging", mount_path="/staging"))
        launch_stage = "BETA"
        vpc_access: run_v2.VpcAccess | None = None
        if config.vpc_connector_network and config.vpc_connector_subnetwork:
            vpc_access = run_v2.VpcAccess(
                egress=run_v2.VpcAccess.VpcEgress.PRIVATE_RANGES_ONLY,
                network_interfaces=[
                    run_v2.VpcAccess.NetworkInterface(
                        network=config.vpc_connector_network,
                        subnetwork=config.vpc_connector_subnetwork,
                    )
                ],
            )

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
                        vpc_access=vpc_access,
                    ),
                ),
            ),
        )
        logger.info("Let's create a new Cloud Run Job...")
        operation = await self.client.create_job(request=request)
        await operation.result()
        logger.debug("Done creating Cloud Run Job")

    async def create_job_if_needed(self, execution: Execution):
        try:
            await self._create_job_if_needed(execution)
        except gac_exceptions.GoogleAPICallError as e:
            if e.grpc_status_code != StatusCode.ALREADY_EXISTS:
                raise
            # we can ignore this
            logger.debug("Done creating Cloud Run Job (already exists)")

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
