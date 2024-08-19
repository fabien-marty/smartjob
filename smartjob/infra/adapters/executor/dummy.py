import asyncio
from dataclasses import dataclass, field

from smartjob.app.execution import Execution, ExecutionResult
from smartjob.app.executor import ExecutionResultFuture, ExecutorPort
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.storage.dummy import DummyStorageAdapter


class DummyExecutionResultFuture(ExecutionResultFuture):
    def _get_result_from_future(self, future: asyncio.Future) -> ExecutionResult:
        return future.result()

    async def _get_output(self) -> dict | list | str | int | float | bool | None:
        return None


def make_storage_service() -> StorageService:
    return StorageService(adapter=DummyStorageAdapter())


@dataclass
class DummyExecutorAdapter(ExecutorPort):
    sleep: float = 1.0
    storage_service: StorageService = field(default_factory=make_storage_service)

    async def work(self, execution: Execution) -> ExecutionResult:
        await asyncio.sleep(self.sleep)
        return ExecutionResult._from_execution(
            execution, True, "https://no-log-url.com/sorry"
        )

    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        return DummyExecutionResultFuture(
            asyncio.create_task(self.work(execution)),
            execution,
            storage_service=self.storage_service,
            log_url="https://no-log-url.com/sorry",
        )

    def get_name(self):
        return "dummy"

    def get_input_path(self, execution: Execution) -> str:
        return "/dummy"

    def get_output_path(self, execution: Execution) -> str:
        return "/dummy"

    def get_storage_service(self) -> StorageService:
        return self.storage_service

    def staging_mount_path(self, execution: Execution) -> str:
        return "/empty"
