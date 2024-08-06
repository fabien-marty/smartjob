import asyncio

from smartjob.app.executor import ExecutionResultFuture, ExecutorPort
from smartjob.app.job import (
    Execution,
    ExecutionResult,
)


class DummyExecutionResultFuture(ExecutionResultFuture):
    def _get_result_from_future(self, future: asyncio.Future) -> ExecutionResult:
        return future.result()


class DummyExecutor(ExecutorPort):
    def __init__(self, sleep: float = 1.0):
        self._sleep = sleep

    async def work(self, execution: Execution) -> ExecutionResult:
        await asyncio.sleep(self._sleep)
        return ExecutionResult.from_execution(execution, True)

    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        execution.log_url = "https://example.com/log"
        return DummyExecutionResultFuture(self.work(execution), execution)
