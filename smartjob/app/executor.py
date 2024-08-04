import asyncio
import datetime
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Coroutine

from stlog import LogContext, getLogger

from smartjob.app.exception import SmartJobException
from smartjob.app.file_uploader import FileUploaderPort
from smartjob.app.job import (
    DEFAULT_NAMESPACE,
    CloudRunSmartJob,
    SmartJob,
    SmartJobExecution,
    SmartJobExecutionResult,
    VertexSmartJob,
)

logger = getLogger("smartjob.executor")


class SmartJobExecutionResultFuture(ABC):
    """Future-like object to get the result of a job execution.

    Attributes:
        job: The job that was scheduled.

    """

    def __init__(
        self,
        coroutine: Coroutine,
        execution: SmartJobExecution,
    ):
        self._execution: SmartJobExecution = execution
        self.__result: SmartJobExecutionResult | None = None
        self.__task = asyncio.create_task(coroutine)
        self.__task.add_done_callback(self._task_done)

    async def result(self) -> SmartJobExecutionResult:
        """Wait and return the result of the job execution.

        This is a coroutine. You have to wait for it to complete with (for example):
        `res = await future.result()`

        Returns:
            The result of the job execution.

        """
        if self.__result is not None:
            return self.__result
        task_result = await self.__task
        res = self._get_result(task_result)
        if self.__result is not None:
            res.stopped = self.__result.stopped
        else:
            res.stopped = datetime.datetime.now(tz=datetime.timezone.utc)
        return res

    def done(self) -> bool:
        """Return True if the job execution is done."""
        return self.__task.done()

    @property
    def execution_id(self) -> str:
        "The execution unique identifier."
        return self._execution.id

    @property
    def log_url(self) -> str:
        "The execution log url."
        if self._execution.log_url is None:
            raise SmartJobException("the log_url property is not set")
        return self._execution.log_url

    @abstractmethod
    def _get_result(self, task_result) -> SmartJobExecutionResult:
        pass

    @abstractmethod
    def _get_result_from_future(self, future) -> SmartJobExecutionResult:
        pass

    def _task_done(self, future: asyncio.Future):
        self.__result = self._get_result_from_future(future)
        self.__result.stopped = datetime.datetime.now(tz=datetime.timezone.utc)
        if self._execution.cancelled:
            return
        if self.__result.success:
            logger.info(
                "Smartjob execution succeeded",
                duration_seconds=self.__result.duration_seconds,
            )
        else:
            logger.warn(
                "Smartjob execution failed",
                log_url=self.__result.log_url,
                duration_seconds=self.__result.duration_seconds,
            )


class SmartJobExecutorPort(ABC):
    @abstractmethod
    async def schedule(self, job: SmartJobExecution) -> SmartJobExecutionResultFuture:
        pass


@dataclass
class SmartJobExecutorService:
    cloudrun_executor_adapter: SmartJobExecutorPort
    vertex_executor_adapter: SmartJobExecutorPort
    file_uploader_adapter: FileUploaderPort
    namespace: str = field(
        default_factory=lambda: os.environ.get("SMARTJOB_NAMESPACE", DEFAULT_NAMESPACE)
    )
    project: str = field(default_factory=lambda: os.environ.get("SMARTJOB_PROJECT", ""))
    region: str = field(default_factory=lambda: os.environ.get("SMARTJOB_REGION", ""))
    docker_image: str = field(
        default_factory=lambda: os.environ.get("SMARTJOB_DOCKER_IMAGE", "")
    )
    staging_bucket: str = field(
        default_factory=lambda: os.environ.get("SMARTJOB_STAGING_BUCKET", "")
    )

    def _update_job_with_default_parameters(self, job: SmartJob, execution_id: str):
        if not job.namespace:
            job.namespace = self.namespace
        if not job.project:
            job.project = self.project
        if not job.region:
            job.region = self.region
        if not job.docker_image:
            job.docker_image = self.docker_image
        if not job.staging_bucket:
            job.staging_bucket = self.staging_bucket

    def _update_execution_env(self, execution: SmartJobExecution):
        execution.add_envs["INPUT_PATH"] = execution.input_path
        execution.add_envs["OUTPUT_PATH"] = execution.output_path
        execution.add_envs["EXECUTION_ID"] = execution.id

    async def _create_input_output_paths_if_needed(self, execution: SmartJobExecution):
        job = execution.job
        coroutines: list[Coroutine] = []
        logger.info(
            "Creating input path gs://%s/%s/...",
            job.staging_bucket_name,
            execution._input_path,
        )
        coroutines.append(
            self.file_uploader_adapter.upload(
                "", job.staging_bucket_name, execution._input_path + "/"
            )
        )
        logger.info(
            "Creating output path gs://%s/%s/...",
            job.staging_bucket_name,
            execution._output_path,
        )
        coroutines.append(
            self.file_uploader_adapter.upload(
                "", job.staging_bucket_name, execution._output_path + "/"
            )
        )
        await asyncio.gather(*coroutines, return_exceptions=True)
        logger.debug("Done creating input/output paths")

    async def _upload_python_script_if_needed_and_update_overridden_args(
        self, execution: SmartJobExecution
    ):
        job = execution.job
        if not job.python_script_path:
            return
        with open(job.python_script_path) as f:
            content = f.read()
        destination_path = f"{execution.base_dir}/script.py"
        logger.info(
            "Uploading python script (%s) to %s/%s...",
            job.python_script_path,
            job.staging_bucket,
            destination_path,
        )
        await self.file_uploader_adapter.upload(
            content, job.staging_bucket_name, destination_path
        )
        logger.debug(
            "Done uploading python script (%s) to %s/%s",
            job.python_script_path,
            job.staging_bucket,
            destination_path,
        )
        execution.overridden_args = [
            "python",
            f"{job.staging_mount_point}/{destination_path}",
        ]

    async def schedule(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
    ) -> SmartJobExecutionResultFuture:
        """Schedule a job and return a kind of future.

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.

        Returns:
            The result of the job execution as a kind of future.

        """
        execution = SmartJobExecution(
            job,
            overridden_args=list(job.overridden_args),
            add_envs={**job.add_envs, **(add_envs or {})},
        )
        self._update_job_with_default_parameters(job, execution_id=execution.id)
        self._update_execution_env(execution)
        job.assert_is_ready()
        LogContext.reset_context()
        LogContext.add(
            job_name=job.name,
            job_namespace=job.namespace,
            project=job.project,
            region=job.region,
            execution_id=execution.id,
        )
        logger.info("Starting a smartjob...")
        await self._create_input_output_paths_if_needed(execution)
        await self._upload_python_script_if_needed_and_update_overridden_args(execution)
        res: SmartJobExecutionResultFuture
        if isinstance(job, CloudRunSmartJob):
            res = await self.cloudrun_executor_adapter.schedule(execution)
        elif isinstance(job, VertexSmartJob):
            res = await self.vertex_executor_adapter.schedule(execution)
        else:
            raise SmartJobException("Unknown job type")
        return res

    async def run(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
    ) -> SmartJobExecutionResult:
        """Schedule a job and wait for its completion.

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.

        Returns:
            The result of the job execution.

        """
        future = await self.schedule(job)
        return await future.result()

    def sync_run(
        self, job: SmartJob, add_envs: dict[str, str] | None = None
    ) -> SmartJobExecutionResult:
        return asyncio.run(self.run(job, add_envs))

    def sync_schedule(
        self, job: SmartJob, add_envs: dict[str, str] | None = None
    ) -> SmartJobExecutionResultFuture:
        return asyncio.run(self.schedule(job, add_envs))
