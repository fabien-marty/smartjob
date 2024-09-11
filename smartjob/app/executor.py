import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Coroutine

from stlog import LogContext, getLogger
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from smartjob.app.exception import SmartJobTimeoutException
from smartjob.app.execution import Execution, ExecutionConfig, ExecutionResult
from smartjob.app.input import Input
from smartjob.app.job import SmartJob
from smartjob.app.storage import StorageService

logger = getLogger("smartjob.executor")


class ExecutionResultFuture(ABC):
    """Future-like object to get the result of a job execution.

    Attributes:
        log_url: The execution log url.

    """

    def __init__(
        self,
        task: asyncio.Task,
        execution: Execution,
        storage_service: StorageService,
        log_url: str,
        **kwargs,
    ):
        self._execution: Execution = execution
        self.__result: ExecutionResult | None = None
        self.__task = task
        self.__task.add_done_callback(self._task_done)
        self.__done_callback_called = asyncio.Event()
        self._storage_service = storage_service
        self.log_url = log_url
        self._log_context: dict[str, Any] = {}

    def _cancel(self):
        if not self.__result and not self._execution.cancelled:
            self._execution.cancelled = True
            self.__task.cancel()

    async def _get_output(self) -> dict | list | str | int | float | bool | None:
        with LogContext.bind(**self._log_context):
            staging_bucket_name = self._execution.config._staging_bucket_name
            staging_bucket = self._execution.config._staging_bucket
            smartjob_path = f"{self._execution.output_relative_path}/smartjob.json"
            logger.info(
                "Downloading smartjob.json from output (if exists)...",
                path=f"{staging_bucket}/{smartjob_path}",
            )
            raw = await self._storage_service.download(
                staging_bucket_name, smartjob_path
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

    async def _get_output_with_timeout_and_retries(self):
        config = self._execution.config
        try:
            async with asyncio.timeout(
                config._timeout_config._timeout_seconds_download
            ):
                try:
                    async for attempt in AsyncRetrying(
                        reraise=True,
                        wait=wait_exponential(min=1, max=300, multiplier=1),
                        stop=stop_after_attempt(
                            config._retry_config._max_attempts_download
                        ),
                    ):
                        with attempt:
                            with LogContext.bind(
                                attempt=attempt.retry_state.attempt_number
                            ):
                                return await self._get_output()
                except RetryError:
                    raise
                raise Exception("Unreachable code")
        except asyncio.TimeoutError:
            raise SmartJobTimeoutException(
                "Timeout reached while downloading ({config._timeout_config._timeout_seconds_download} seconds)"
            )

    async def result(self) -> ExecutionResult:
        """Wait and return the result of the job execution.

        This is a coroutine. You have to wait for it to complete with (for example):
        `res = await future.result()`

        Returns:
            The result of the job execution.

        """
        await self.__done_callback_called.wait()
        assert self.__result is not None
        if self.__result.success:
            self.__result.json_output = (
                await self._get_output_with_timeout_and_retries()
            )
        return self.__result

    def done(self) -> bool:
        """Return True if the job execution is done."""
        return self.__task.done()

    @property
    def execution_id(self) -> str:
        "The execution unique identifier."
        return self._execution.id

    @abstractmethod
    def _get_result_from_future(self, future: asyncio.Future) -> ExecutionResult:
        pass

    def _task_done(self, future: asyncio.Future):
        with LogContext.bind(**self._log_context):
            self.__result = self._get_result_from_future(future)
            if not self._execution.cancelled:
                if self.__result.success:
                    logger.info(
                        "Smartjob execution succeeded",
                        log_url=self.log_url,
                        duration_seconds=self.__result.duration_seconds,
                    )
                else:
                    logger.warn(
                        "Smartjob execution failed",
                        log_url=self.log_url,
                        duration_seconds=self.__result.duration_seconds,
                    )
            self.__done_callback_called.set()


class ExecutorPort(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        pass

    @abstractmethod
    def get_storage_service(self) -> StorageService:
        pass

    @abstractmethod
    def staging_mount_path(self, execution: Execution) -> str:
        pass


@dataclass
class ExecutorService:
    adapter: ExecutorPort
    _storage_service: StorageService | None = field(default=None, init=False)
    executor_name: str = field(default="", init=False)

    @property
    def storage_service(self) -> StorageService:
        assert self._storage_service is not None
        return self._storage_service

    def __post_init__(self):
        self.executor_name = self.adapter.get_name()
        self._storage_service = self.adapter.get_storage_service()

    def get_input_path(self, execution: Execution) -> str:
        return f"{self.adapter.staging_mount_path(execution)}/{execution.input_relative_path}"

    def get_output_path(self, execution: Execution) -> str:
        return f"{self.adapter.staging_mount_path(execution)}/{execution.output_relative_path}"

    def update_execution_env(self, execution: Execution):
        execution.add_envs["INPUT_PATH"] = self.get_input_path(execution)
        execution.add_envs["OUTPUT_PATH"] = self.get_output_path(execution)
        execution.add_envs["EXECUTION_ID"] = execution.id

    async def upload_python_script_if_needed_and_update_overridden_args(
        self, execution: Execution
    ):
        job = execution.job
        if not job.python_script_path:
            return
        with open(job.python_script_path) as f:
            content = f.read()
        destination_path = f"{execution.base_dir}/input/script.py"
        logger.info(
            "Uploading python script (%s) to %s/%s...",
            job.python_script_path,
            execution.config._staging_bucket,
            destination_path,
        )
        await self.storage_service.upload(
            content.encode("utf8"),
            execution.config._staging_bucket_name,
            destination_path,
        )
        logger.debug(
            "Done uploading python script (%s) to %s/%s",
            job.python_script_path,
            execution.config.staging_bucket,
            destination_path,
        )
        execution.overridden_args = [
            "python",
            f"{self.adapter.staging_mount_path(execution)}/{destination_path}",
        ]

    def replace_overridden_args_placeholders(self, execution: Execution):
        input_path = (
            f"{self.adapter.staging_mount_path(execution)}/{execution.base_dir}/input"
        )
        execution.overridden_args = [
            x.replace("{{INPUT}}", input_path) for x in execution.overridden_args
        ]

    async def upload_inputs(self, execution: Execution):
        async def _upload_input(input: Input):
            path = f"{execution.config._staging_bucket}/{execution.input_relative_path}/{input.filename}"
            logger.info(f"Uploading input to {path}...")
            await input._create(
                execution.config._staging_bucket_name,
                execution.input_relative_path,
                self.storage_service,
            )
            logger.debug(f"Done uploading input: {path}")

        inputs = execution.inputs
        await asyncio.gather(*[_upload_input(input) for input in inputs])

    async def create_input_output_paths_if_needed(self, execution: Execution):
        coroutines: list[Coroutine] = []
        logger.info(
            "Creating input path %s/%s/...",
            execution.config._staging_bucket,
            execution.input_relative_path,
        )
        coroutines.append(
            self.storage_service.upload(
                b"",
                execution.config._staging_bucket_name,
                execution.input_relative_path + "/",
            )
        )
        logger.info(
            "Creating output path %s/%s/...",
            execution.config._staging_bucket,
            execution.output_relative_path,
        )
        coroutines.append(
            self.storage_service.upload(
                b"",
                execution.config._staging_bucket_name,
                execution.output_relative_path + "/",
            )
        )
        await asyncio.gather(*coroutines, return_exceptions=True)
        logger.debug("Done creating input/output paths")

    async def prepare(self, execution: Execution):
        self.update_execution_env(execution)
        await self.create_input_output_paths_if_needed(execution)
        await self.upload_python_script_if_needed_and_update_overridden_args(execution)
        self.replace_overridden_args_placeholders(execution)
        await self.upload_inputs(execution)

    async def _schedule(
        self,
        job: SmartJob,
        execution_config: ExecutionConfig,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
    ) -> ExecutionResultFuture:
        execution = Execution(
            job,
            overridden_args=list(job.overridden_args),
            add_envs={**job.add_envs, **(add_envs or {})},
            config=execution_config,
            inputs=inputs or [],
        )
        with LogContext.bind(execution_id=execution.id):
            logger.info("Preparing the smartjob execution...")
            await self.prepare(execution)
            logger.debug("Smartjob execution prepared")
            logger.info("Scheduling the smartjob execution...")
            res = await self.adapter.schedule(execution)
            logger.debug("Smartjob execution scheduled")
            return res

    async def schedule(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        execution_config: ExecutionConfig | None = None,
    ) -> ExecutionResultFuture:
        """Schedule a job and return a kind of future (coroutine version).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            execution_config: Execution configuration.

        Returns:
            The result of the job execution as a kind of future.

        """
        execution_config = execution_config or ExecutionConfig()
        execution_config.fix_for_executor_name(self.executor_name)
        try:
            async with asyncio.timeout(
                execution_config._timeout_config._timeout_seconds_schedule
            ):
                with LogContext.bind(
                    job_name=job.name,
                    job_namespace=job.namespace,
                ):
                    try:
                        async for attempt in AsyncRetrying(
                            reraise=True,
                            wait=wait_exponential(multiplier=1, min=1, max=300),
                            stop=stop_after_attempt(
                                execution_config._retry_config._max_attempts_schedule
                            ),
                        ):
                            with attempt:
                                with LogContext.bind(
                                    attempt=attempt.retry_state.attempt_number
                                ):
                                    fut = await self._schedule(
                                        job,
                                        execution_config=execution_config,
                                        add_envs=add_envs,
                                        inputs=inputs,
                                    )
                                    logger.info(
                                        "Smartjob execution scheduled",
                                        log_url=fut.log_url,
                                    )
                                    LogContext.remove("attempt")
                                    LogContext.add(execution_id=fut.execution_id)
                                    fut._log_context = LogContext.getall()
                                    return fut
                    except RetryError:
                        raise
                    raise Exception("Unreachable code")
        except asyncio.TimeoutError:
            raise SmartJobTimeoutException(
                f"Timeout reached (after {execution_config._timeout_config._timeout_seconds_schedule} seconds)"
            )

    async def run(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        execution_config: ExecutionConfig | None = None,
    ) -> ExecutionResult:
        """Schedule a job and wait for its completion (couroutine version).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            execution_config: Execution configuration.

        Returns:
            The result of the job execution.

        """
        execution_config = execution_config or ExecutionConfig()
        execution_config.fix_timeout_config()
        try:
            async with asyncio.timeout(
                execution_config._timeout_config.timeout_seconds
            ):
                future = await self.schedule(
                    job,
                    add_envs=add_envs,
                    inputs=inputs,
                    execution_config=execution_config,
                )
                return await future.result()
        except asyncio.TimeoutError:
            raise SmartJobTimeoutException(
                f"Timeout reached ({execution_config._timeout_config.timeout_seconds} seconds)"
            )

    def sync_run(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        execution_config: ExecutionConfig | None = None,
    ) -> ExecutionResult:
        """Schedule a job and wait for its completion (blocking version).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            execution_config: Execution configuration.

        Returns:
            The result of the job execution.
        """
        return asyncio.run(
            self.run(job, add_envs, inputs, execution_config=execution_config)
        )
