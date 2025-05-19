import concurrent.futures
import json
from dataclasses import dataclass, field
from typing import cast

import google.api_core.exceptions as gac_exceptions
from google.api_core import operation
from google.cloud import run_v2
from google.protobuf.duration_pb2 import Duration
from grpc import StatusCode
from stlog import LogContext, getLogger

from smartjob.app.execution import Execution
from smartjob.app.executor import (
    ExecutorPort,
    SchedulingDetails,
    _ExecutionResult,
)
from smartjob.app.utils import hex_hash

logger = getLogger("smartjob.executor.cloudrun")


@dataclass(frozen=True)
class JobListCacheKey:
    project: str
    region: str


@dataclass
class CloudRunExecutorAdapter(ExecutorPort):
    _client: run_v2.JobsClient | None = field(default=None, init=False)
    max_workers: int = 10
    _executor: concurrent.futures.ThreadPoolExecutor | None = field(
        default=None, init=False
    )

    def __post_init__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        )

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        assert self._executor is not None
        return self._executor

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
    def client(self) -> run_v2.JobsClient:
        # lazy-init to circumvent some 'attached to a different loop' issues
        if self._client is None:
            self._client = run_v2.JobsClient()
        return self._client

    def _create_job_if_needed(self, execution: Execution):
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
        if config._sidecars_container_images:
            volumes.append(
                run_v2.Volume(
                    name="sidecars",
                    empty_dir=run_v2.EmptyDirVolumeSource(
                        medium="MEMORY",
                    ),
                )
            )
            volume_mounts.append(
                run_v2.VolumeMount(name="sidecars", mount_path="/shared")
            )
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

        containers = [
            run_v2.Container(
                name="container-0",
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
        ]
        for i, sidecar_image in enumerate(config._sidecars_container_images):
            containers.append(
                run_v2.Container(
                    name=f"sidecar-{i + 1}",
                    image=sidecar_image,
                    resources=run_v2.ResourceRequirements(
                        startup_cpu_boost=False,
                        limits={
                            "cpu": "1.0",
                            "memory": "1Gi",
                        },
                    ),
                    volume_mounts=volume_mounts,
                )
            )

        logger.info("Let's check if Cloud Run Job already exists...")
        request = run_v2.GetJobRequest(name=self.full_cloud_run_job_name(execution))
        try:
            self.client.get_job(request=request)
            logger.info("Done checking Cloud Run Job")
            return
        except Exception:
            logger.info("Can't get the Cloud Run Job, let's create it!")

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
                        containers=containers,
                        volumes=volumes,
                        service_account=config.service_account,
                        vpc_access=vpc_access,
                    ),
                ),
            ),
        )
        logger.info("Let's create a new Cloud Run Job...")
        operation = self.client.create_job(request=request)
        operation.result()
        logger.debug("Done creating Cloud Run Job")

    def create_job_if_needed(self, execution: Execution):
        try:
            self._create_job_if_needed(execution)
        except gac_exceptions.GoogleAPICallError as e:
            if e.grpc_status_code != StatusCode.ALREADY_EXISTS:
                raise
            # we can ignore this
            logger.debug("Done creating Cloud Run Job (already exists)")

    def wait(
        self, execution: Execution, operation: operation.Operation, log_context: dict
    ) -> _ExecutionResult:
        """Note: executed in another thread."""
        with LogContext.bind(**log_context):
            try:
                task_result = operation.result()
                castedResult = cast(run_v2.Execution, task_result)
                success = castedResult.succeeded_count > 0
                return _ExecutionResult._from_execution(
                    execution, success, operation.metadata.log_uri
                )
            except Exception:
                logger.debug("exception catched during wait()", exc_info=True)
        return _ExecutionResult._from_execution(
            execution, False, operation.metadata.log_uri
        )

    def schedule(
        self, execution: Execution, forget: bool
    ) -> tuple[SchedulingDetails, concurrent.futures.Future[_ExecutionResult] | None]:
        job = execution.job
        config = execution.config
        job_id = self.cloud_run_job_name(execution)
        full_name = self.full_cloud_run_job_name(execution)
        with LogContext.bind(
            cloudrun_job_id=job_id, project=config._project, region=config._region
        ):
            self.create_job_if_needed(execution)
            container_overrides = [
                run_v2.RunJobRequest.Overrides.ContainerOverride(
                    name="container-0",
                    args=execution.overridden_args,
                    env=[
                        run_v2.EnvVar(name=k, value=v)
                        for k, v in execution.add_envs.items()
                    ],
                )
            ]
            for i, _ in enumerate(config._sidecars_container_images):
                container_overrides.append(
                    run_v2.RunJobRequest.Overrides.ContainerOverride(
                        name=f"sidecar-{i + 1}",
                        env=[
                            run_v2.EnvVar(name=k, value=v)
                            for k, v in execution.add_envs.items()
                        ],
                    )
                )
            request = run_v2.RunJobRequest(
                name=full_name,
                overrides=run_v2.RunJobRequest.Overrides(
                    task_count=1,
                    timeout=Duration(seconds=config._timeout_config.timeout_seconds),
                    container_overrides=container_overrides,
                ),
            )
            logger.info(
                "Let's trigger a new execution of Cloud Run Job...",
                docker_image=job.docker_image,
                overridden_args=execution.overridden_args_as_string,
                add_envs=execution.add_envs_as_string,
                timeout_s=config._timeout_config.timeout_seconds,
            )
            operation = self.client.run_job(request=request)
            scheduling_details = SchedulingDetails(
                execution_id=execution.id, log_url=operation.metadata.log_uri
            )
            if forget:
                return scheduling_details, None
            future = self.executor.submit(
                self.wait, execution, operation, LogContext.getall()
            )
            return scheduling_details, future

    def get_name(self) -> str:
        return "cloudrun"

    def staging_mount_path(self, execution: Execution) -> str:
        return "/staging"
