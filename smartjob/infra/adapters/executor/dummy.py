import concurrent.futures
import time
from dataclasses import dataclass, field
from typing import cast

from smartjob.app.execution import Execution
from smartjob.app.executor import (
    ExecutorPort,
    SchedulingResult,
    _ExecutionResult,
    _ExecutionResultFuture,
)


@dataclass
class DummyExecutorAdapter(ExecutorPort):
    sleep: float = 1.0
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

    def wait(self, execution: Execution) -> _ExecutionResult:
        time.sleep(self.sleep)
        return _ExecutionResult._from_execution(
            execution, True, "https://no-log-url.com/sorry"
        )

    def schedule(
        self, execution: Execution, forget: bool
    ) -> tuple[SchedulingResult, _ExecutionResultFuture | None]:
        sr = SchedulingResult._from_execution(
            execution, True, "https://no-log-url.com/sorry"
        )
        if forget:
            return sr, None
        future = self.executor.submit(self.wait, execution)
        casted_future = cast(_ExecutionResultFuture, future)
        return sr, casted_future

    def get_name(self):
        return "dummy"

    def staging_mount_path(self, execution: Execution) -> str:
        return "/empty"
