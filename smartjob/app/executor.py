import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Coroutine

from stlog import LogContext, getLogger
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

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
        coroutine: Coroutine,
        execution: Execution,
        storage_service: StorageService,
        log_url: str,
        **kwargs,
    ):
        self._execution: Execution = execution
        self.__result: ExecutionResult | None = None
        self.__task = asyncio.create_task(coroutine)
        self.__task.add_done_callback(self._task_done)
        self.__done_callback_called = asyncio.Event()
        self._storage_service = storage_service
        self.log_url = log_url

    def _cancel(self):
        if not self.__result and not self._execution.cancelled:
            self._execution.cancelled = True
            self.__task.cancel()

    @abstractmethod
    async def _get_output(self) -> dict | list | str | int | float | bool | None:
        pass

    async def result(self) -> ExecutionResult:
        """Wait and return the result of the job execution.

        This is a coroutine. You have to wait for it to complete with (for example):
        `res = await future.result()`

        Returns:
            The result of the job execution.

        """
        await self.__done_callback_called.wait()
        assert self.__result is not None
        self.__result.json_output = await self._get_output()
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
    async def prepare(self, execution: Execution):
        pass

    @abstractmethod
    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        pass


@dataclass
class ExecutorService:
    adapter: ExecutorPort
    executor_name: str = field(default="", init=False)

    def __post_init__(self):
        self.executor_name = self.adapter.get_name()

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
            await self.adapter.prepare(execution)
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
                                add_envs=add_envs,
                                inputs=inputs,
                                execution_config=execution_config,
                            )
                            logger.info(
                                "Smartjob execution scheduled",
                                execution_id=fut.execution_id,
                                log_url=fut.log_url,
                            )
                            return fut
            except RetryError:
                raise
            raise Exception("Unreachable code")

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
        future = await self.schedule(
            job, add_envs=add_envs, inputs=inputs, execution_config=execution_config
        )
        return await future.result()

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
