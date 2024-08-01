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

from smartjob.app.executor import SmartJobExecutorPort
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

    @property
    def log_url(self) -> str:
        return f"https://console.cloud.google.com/logs/query;query=resource.labels.job_id%3D%22{self.id}%22?project=botify-pw-experimental"

    @property
    def success(self) -> bool:
        return self.state == gca_job_state.JobState.JOB_STATE_SUCCEEDED


class VertexSmartJobExecutor(SmartJobExecutorPort):
    def __init__(self, max_workers: int = 10):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def init_aiplatform_if_needed(self):
        init_aiplatform()

    def sync_run(self, job: VertexSmartJob) -> SmartJobExecutionResult:
        self.init_aiplatform_if_needed()
        created = datetime.datetime.now(tz=datetime.timezone.utc)
        customJob = VertexCustomJob(
            display_name=f"{job.namespace}-{job.name}-{job.short_id}",
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
            f"{job.namespace}-{job.name}-{job.short_id}",
            docker_image=job.docker_image,
            overridden_args=shlex.join(job.overridden_args),
            overriden_envs=", ".join(
                [f"{x}={y}" for x, y in job.overridden_envs.items()]
            ),
        )
        try:
            customJob.run()
        except RuntimeError:
            pass
        stopped = datetime.datetime.now(tz=datetime.timezone.utc)
        return SmartJobExecutionResult(
            job=job,
            success=customJob.success,
            log_url=customJob.log_url,
            created=created,
            stopped=stopped,
        )

    async def run(self, job: SmartJob) -> SmartJobExecutionResult:
        castedJob = typing.cast(VertexSmartJob, job)
        cf_future = self.executor.submit(self.sync_run, castedJob)
        return await asyncio.wrap_future(cf_future)
