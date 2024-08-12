import asyncio
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Coroutine

from stlog import LogContext, getLogger
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from smartjob.app.exception import SmartJobException
from smartjob.app.input import Input
from smartjob.app.job import (
    DEFAULT_NAMESPACE,
    CloudRunSmartJob,
    Execution,
    ExecutionResult,
    SmartJob,
    VertexSmartJob,
)
from smartjob.app.retry import RetryConfig
from smartjob.app.storage import StorageService

logger = getLogger("smartjob.executor")


class ExecutionResultFuture(ABC):
    """Future-like object to get the result of a job execution.

    Attributes:
        job: The job that was scheduled.

    """

    def __init__(
        self,
        coroutine: Coroutine,
        execution: Execution,
    ):
        self._execution: Execution = execution
        self.__result: ExecutionResult | None = None
        self.__task = asyncio.create_task(coroutine)
        self.__task.add_done_callback(self._task_done)
        self._storage_service: StorageService | None = None

    def _cancel(self):
        if not self.__result and not self._execution.cancelled:
            self._execution.cancelled = True
            self.__task.cancel()

    async def _download_output(self) -> dict | list | str | int | float | bool | None:
        if self._storage_service is None:
            return None
        logger.info("Downloading smartjob.json from output (if exists)...")
        raw = await self._storage_service.download(
            self._execution.job.staging_bucket_name,
            f"{self._execution._output_path}/smartjob.json",
        )
        if raw == b"":
            logger.debug("No smartjob.json found in output")
            return None
        try:
            res = json.loads(raw)
            logger.debug("smartjob.json downloaded/decoded")
            return res
        except Exception:
            logger.warning("smartjob.json is not a valid json")
            return None

    async def result(self) -> ExecutionResult:
        """Wait and return the result of the job execution.

        This is a coroutine. You have to wait for it to complete with (for example):
        `res = await future.result()`

        Returns:
            The result of the job execution.

        """
        while True:
            # wait for _task_done() callback to be called
            if self.__result is not None:
                self.__result.json_output = await self._download_output()
                return self.__result
            await asyncio.sleep(1.0)

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
    def _get_result_from_future(self, future: asyncio.Future) -> ExecutionResult:
        pass

    def _task_done(self, future: asyncio.Future):
        self.__result = self._get_result_from_future(future)
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


class ExecutorPort(ABC):
    @abstractmethod
    async def schedule(self, job: Execution) -> ExecutionResultFuture:
        pass

    @abstractmethod
    def pre_schedule_checks(self, execution: Execution):
        pass


@dataclass
class ExecutorService:
    cloudrun_executor_adapter: ExecutorPort
    vertex_executor_adapter: ExecutorPort
    storage_service: StorageService
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
    retry_config: RetryConfig = field(default_factory=RetryConfig)

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

    def _update_execution_env(self, execution: Execution):
        execution.add_envs["INPUT_PATH"] = execution.input_path
        execution.add_envs["OUTPUT_PATH"] = execution.output_path
        execution.add_envs["EXECUTION_ID"] = execution.id

    async def _create_input_output_paths_if_needed(self, execution: Execution):
        job = execution.job
        coroutines: list[Coroutine] = []
        logger.info(
            "Creating input path gs://%s/%s/...",
            job.staging_bucket_name,
            execution._input_path,
        )
        coroutines.append(
            self.storage_service.upload(
                b"", job.staging_bucket_name, execution._input_path + "/"
            )
        )
        logger.info(
            "Creating output path gs://%s/%s/...",
            job.staging_bucket_name,
            execution._output_path,
        )
        coroutines.append(
            self.storage_service.upload(
                b"", job.staging_bucket_name, execution._output_path + "/"
            )
        )
        await asyncio.gather(*coroutines, return_exceptions=True)
        logger.debug("Done creating input/output paths")

    async def _upload_python_script_if_needed_and_update_overridden_args(
        self, execution: Execution
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
        await self.storage_service.upload(
            content.encode("utf8"), job.staging_bucket_name, destination_path
        )
        logger.debug(
            "Done uploading python script (%s) to %s/%s",
            job.python_script_path,
            job.staging_bucket,
            destination_path,
        )
        execution.overridden_args = [
            "python",
            f"{job._staging_mount_point}/{destination_path}",
        ]

    async def _upload_inputs(self, execution: Execution, inputs: list[Input]):
        async def _upload_input(input: Input):
            path = f"gs://{execution.job.staging_bucket_name}/{execution._input_path}/{input.filename}"
            logger.info(f"Uploading input to {path}...")
            await input._create(
                execution.job.staging_bucket_name,
                execution._input_path,
                self.storage_service,
            )
            logger.debug(f"Done uploading input: {path}")

        await asyncio.gather(*[_upload_input(input) for input in inputs])

    def get_executor(self, execution: Execution) -> ExecutorPort:
        if isinstance(execution.job, CloudRunSmartJob):
            return self.cloudrun_executor_adapter
        elif isinstance(execution.job, VertexSmartJob):
            return self.vertex_executor_adapter
        raise SmartJobException("Unknown job type")

    async def _schedule(
        self,
        job: SmartJob,
        retry_config: RetryConfig,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
    ) -> ExecutionResultFuture:
        execution = Execution(
            job,
            overridden_args=list(job.overridden_args),
            add_envs={**job.add_envs, **(add_envs or {})},
            max_attempts=retry_config._max_attempts_execute,
        )
        self._update_job_with_default_parameters(job, execution_id=execution.id)
        self._update_execution_env(execution)
        executor = self.get_executor(execution)
        executor.pre_schedule_checks(execution)
        await self._upload_inputs(execution, inputs or [])
        job._assert_is_ready()
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
        res = await executor.schedule(execution)
        res._storage_service = self.storage_service
        return res

    async def schedule(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> ExecutionResultFuture:
        """Schedule a job and return a kind of future (coroutine version).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            retry_config: FIXME.

        Returns:
            The result of the job execution as a kind of future.

        """
        retry_config = retry_config or self.retry_config
        try:
            async for attempt in AsyncRetrying(
                wait=wait_exponential(multiplier=1, min=1, max=300),
                stop=stop_after_attempt(retry_config._max_attempts_schedule),
            ):
                with attempt:
                    return await self._schedule(
                        job, add_envs=add_envs, inputs=inputs, retry_config=retry_config
                    )
        except RetryError:
            raise
        raise Exception("Unreachable code")

    async def run(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> ExecutionResult:
        """Schedule a job and wait for its completion (couroutine version).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            retry_config: FIXME.

        Returns:
            The result of the job execution.

        """
        retry_config = retry_config or self.retry_config
        future = await self.schedule(
            job, add_envs=add_envs, inputs=inputs, retry_config=retry_config
        )
        return await future.result()

    def sync_run(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> ExecutionResult:
        """Schedule a job and wait for its completion (blocking version).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            retry_config: FIXME.

        Returns:
            The result of the job execution.
        """
        return asyncio.run(self.run(job, add_envs, inputs, retry_config=retry_config))
