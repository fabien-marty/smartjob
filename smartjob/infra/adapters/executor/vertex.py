import asyncio
import concurrent.futures
import datetime
import logging
import shlex
import sys
import typing
from threading import Lock

from google.cloud import aiplatform
from google.cloud.aiplatform_v1.types import custom_job as custom_job_v1
from google.cloud.aiplatform_v1.types import job_state as gca_job_state
from google.cloud.aiplatform_v1.types.env_var import EnvVar
from google.cloud.aiplatform_v1.types.machine_resources import DiskSpec, MachineSpec
from stlog import getLogger

from smartjob.app.executor import SmartJobExecutionResultFuture, SmartJobExecutorPort
from smartjob.app.job import SmartJob, SmartJobExecutionResult, VertexSmartJob

aiplatform_mutex = Lock()
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
    @property
    def id(self) -> str:
        return self.resource_name.split("/")[-1]

    def get_log_url(self, project) -> str:
        return f"https://console.cloud.google.com/logs/query;query=resource.labels.job_id%3D%22{self.id}%22?project={project}"

    @property
    def success(self) -> bool:
        return self.state == gca_job_state.JobState.JOB_STATE_SUCCEEDED


class VertexSmartJobExecutionResultFuture(SmartJobExecutionResultFuture):
    def _get_result(self, task_result) -> SmartJobExecutionResult:
        return task_result

    def _get_result_from_future(self, future) -> SmartJobExecutionResult:
        return self._get_result(future.result())


class VertexSmartJobExecutor(SmartJobExecutorPort):
    def __init__(self, max_workers: int = 10):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def init_aiplatform_if_needed(self):
        init_aiplatform()

    def sync_run(self, job: VertexSmartJob) -> SmartJobExecutionResult:
        self.init_aiplatform_if_needed()
        customJob = VertexCustomJob(
            display_name=f"{job.namespace}-{job.name}-{job.short_execution_id}",
            project=job.project,
            location=job.region,
            staging_bucket=job.staging_bucket,
            worker_pool_specs=[
                custom_job_v1.WorkerPoolSpec(
                    replica_count=1,
                    container_spec=custom_job_v1.ContainerSpec(
                        image_uri=job.docker_image,
                        args=job.overridden_args,
                        env=[
                            EnvVar(name=k, value=v)
                            for k, v in job.overridden_envs.items()
                        ],
                    ),
                    machine_spec=MachineSpec(
                        machine_type=job.machine_type,
                        accelerator_type=job.accelerator_type,
                        accelerator_count=job.accelerator_count,
                    ),
                    disk_spec=DiskSpec(
                        boot_disk_type=job.boot_disk_type,
                        boot_disk_size_gb=job.boot_disk_size_gb,
                    ),
                )
            ],
        )
        logger.info(
            "Let's trigger a new execution of Vertex Job: %s...",
            f"{job.namespace}-{job.name}-{job.short_execution_id}",
            docker_image=job.docker_image,
            overridden_args=shlex.join(job.overridden_args),
            overridden_envs=", ".join(
                [f"{x}={y}" for x, y in job.overridden_envs.items()]
            ),
        )
        try:
            customJob.submit(
                disable_retries=True,
                timeout=job.timeout_seconds,
                service_account=job.service_account,
            )
            job.log_url = customJob.get_log_url(job.project)
            customJob._block_until_complete()
        except RuntimeError:
            pass
        return SmartJobExecutionResult(
            job=job,
            success=customJob.success,
        )

    # FIXME: we can do this better
    async def _wait(
        self, future: asyncio.Future[SmartJobExecutionResult]
    ) -> SmartJobExecutionResult:
        while True:
            if future.done():
                return future.result()
            await asyncio.sleep(1)

    async def schedule(self, job: SmartJob) -> SmartJobExecutionResultFuture:
        if not job.created:
            job.created = datetime.datetime.now(tz=datetime.timezone.utc)
        if not job.execution_id:
            job.set_execution_id()
        castedJob = typing.cast(VertexSmartJob, job)
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(self.executor, self.sync_run, castedJob)
        return VertexSmartJobExecutionResultFuture(self._wait(future), job=castedJob)
