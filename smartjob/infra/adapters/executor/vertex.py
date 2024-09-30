import concurrent.futures
import logging
import sys
import threading
from dataclasses import dataclass, field

from google.cloud import aiplatform
from google.cloud.aiplatform_v1.types import custom_job as custom_job_v1
from google.cloud.aiplatform_v1.types import job_state as gca_job_state
from google.cloud.aiplatform_v1.types.env_var import EnvVar
from google.cloud.aiplatform_v1.types.machine_resources import DiskSpec, MachineSpec
from stlog import LogContext, getLogger

from smartjob.app.execution import Execution
from smartjob.app.executor import (
    ExecutorPort,
    SchedulingResult,
    _ExecutionResult,
)

aiplatform_mutex = threading.Lock()
aiplatform_initialized: bool = False
logger = getLogger("smartjob.executor.vertex")


def monkey_patch_aiplatform_jobs_logger():
    from google.cloud.aiplatform.jobs import _LOGGER

    for handler in _LOGGER.handlers:
        _LOGGER.removeHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.WARNING)
    _LOGGER.addHandler(handler)
    logging.getLogger("google.cloud.aiplatform.jobs").setLevel(logging.WARNING)


def init_aiplatform():
    global aiplatform_initialized
    with aiplatform_mutex:
        if not aiplatform_initialized:
            aiplatform.init()
            monkey_patch_aiplatform_jobs_logger()
            aiplatform_initialized = True


class VertexCustomJob(aiplatform.CustomJob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_created = threading.Event()

    @property
    def id(self) -> str:
        return self.resource_name.split("/")[-1]

    def get_log_url(self, project) -> str:
        self.resource_created.wait()
        return self._get_log_url(project)

    def _get_log_url(self, project) -> str:
        try:
            return f"https://console.cloud.google.com/logs/query;query=resource.labels.job_id%3D%22{self.id}%22?project={project}"
        except Exception:
            return "https://no-log-url.com/sorry"

    @property
    def success(self) -> bool:
        return self.state == gca_job_state.JobState.JOB_STATE_SUCCEEDED


@dataclass
class VertexExecutorAdapter(ExecutorPort):
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

    def init_aiplatform_if_needed(self):
        init_aiplatform()

    def run(
        self, custom_job: VertexCustomJob, execution: Execution, log_context: dict
    ) -> _ExecutionResult:
        """Note: executed in another thread."""
        self.init_aiplatform_if_needed()
        job = execution.job
        with LogContext.bind(
            **log_context
        ):  # we need to rebind here as we are now in another thread
            logger.info(
                "Let's trigger a new execution of Vertex Job...",
                docker_image=job.docker_image,
                overridden_args=execution.add_envs_as_string,
                add_envs=execution.overridden_args_as_string,
            )
            try:
                custom_job.submit(
                    disable_retries=True,
                    timeout=execution.config._timeout_config.timeout_seconds,
                    service_account=execution.config.service_account,
                )
                custom_job.resource_created.set()
                logger.debug("Resource created")
                custom_job._block_until_complete()
            except RuntimeError:
                pass
            finally:
                custom_job.resource_created.set()
            return _ExecutionResult._from_execution(
                execution,
                custom_job.success,
                custom_job._get_log_url(execution.config._project),
            )

    def schedule(
        self, execution: Execution, forget: bool
    ) -> tuple[SchedulingResult, concurrent.futures.Future[_ExecutionResult] | None]:
        job = execution.job
        config = execution.config
        vertex_name = f"{job.full_name}-{execution.id}"
        with LogContext.bind(
            vertex_name=vertex_name, project=config._project, region=config._region
        ):
            custom_job = VertexCustomJob(
                display_name=vertex_name,
                labels={
                    key.replace(".", "_"): value.replace(".", "_")
                    for key, value in execution.labels.items()
                },
                project=execution.config._project,
                location=execution.config._region,
                staging_bucket=execution.config._staging_bucket,
                worker_pool_specs=[
                    custom_job_v1.WorkerPoolSpec(
                        replica_count=1,
                        container_spec=custom_job_v1.ContainerSpec(
                            image_uri=job.docker_image,
                            args=execution.overridden_args,
                            env=[
                                EnvVar(name=k, value=v)
                                for k, v in execution.add_envs.items()
                            ],
                        ),
                        machine_spec=MachineSpec(
                            machine_type=execution.config._machine_type,
                            accelerator_type=execution.config._accelerator_type,
                            accelerator_count=execution.config._accelerator_count,
                        ),
                        disk_spec=DiskSpec(
                            boot_disk_type=execution.config._boot_disk_type,
                            boot_disk_size_gb=execution.config._boot_disk_size_gb,
                        ),
                    )
                ],
            )
            future = self.executor.submit(
                self.run, custom_job, execution, LogContext.getall()
            )
            log_url = custom_job.get_log_url(execution.config._project)
            sr = SchedulingResult._from_execution(
                execution, success=True, log_url=log_url
            )
            if forget:
                return sr, None
            return sr, future

    def staging_mount_path(self, execution: Execution) -> str:
        return f"/gcs/{execution.config._staging_bucket_name}"

    def get_name(self) -> str:
        return "vertex"
