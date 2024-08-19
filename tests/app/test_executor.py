import asyncio
from dataclasses import dataclass

import pytest

from smartjob.app.exception import SmartJobTimeoutException
from smartjob.app.execution import Execution, ExecutionConfig, ExecutionResult
from smartjob.app.executor import ExecutionResultFuture, ExecutorService
from smartjob.app.job import SmartJob
from smartjob.app.storage import StorageService
from smartjob.app.timeout import TimeoutConfig
from smartjob.infra.adapters.executor.dummy import (
    DummyExecutionResultFuture,
    DummyExecutorAdapter,
)
from smartjob.infra.adapters.storage.dummy import DummyStorageAdapter


@pytest.fixture
def storage_service():
    return StorageService(adapter=DummyStorageAdapter(sleep=1, keep_in_memory=True))


@pytest.fixture
def executor_service(storage_service):
    adapter = DummyExecutorAdapter(sleep=0, storage_service=storage_service)
    return ExecutorService(adapter=adapter)


def test_basic(executor_service):
    job = SmartJob("foo", docker_image="python:3.12")
    result = executor_service.sync_run(job)
    assert result.success is True


@pytest.mark.asyncio
async def test_schedule(executor_service):
    job = SmartJob("foo", docker_image="python:3.12")
    fut = await executor_service.schedule(job)
    assert fut.done() is False
    result = await fut.result()
    assert result.success is True


@dataclass
class TimeoutDummyExecutorAdapter(DummyExecutorAdapter):
    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        await asyncio.sleep(3)
        return DummyExecutionResultFuture(
            asyncio.create_task(self.work(execution)),
            execution,
            storage_service=self.storage_service,
            log_url="https://no-log-url.com/sorry",
        )


@pytest.fixture
def timeout_executor_service(storage_service):
    return ExecutorService(
        adapter=TimeoutDummyExecutorAdapter(sleep=0, storage_service=storage_service)
    )


@pytest.mark.asyncio
async def test_timeout_during_schedule(timeout_executor_service):
    job = SmartJob("foo", docker_image="python:3.12")
    execution_config = ExecutionConfig(timeout_config=TimeoutConfig(timeout_seconds=1))
    with pytest.raises(SmartJobTimeoutException):
        await timeout_executor_service.schedule(job, execution_config=execution_config)


@dataclass
class RetryDummyExecutorAdapter(DummyExecutorAdapter):
    attempt: int = 1

    async def work(self, execution: Execution) -> ExecutionResult:
        self.attempt += 1
        if self.attempt < 3:
            raise Exception("failed => must be retried")
        return await super().work(execution)


@pytest.mark.asyncio
async def test_retry(storage_service):
    executor_service = ExecutorService(
        adapter=TimeoutDummyExecutorAdapter(sleep=0, storage_service=storage_service)
    )
    job = SmartJob("foo", docker_image="python:3.12")
    res = await executor_service.run(job)
    assert res.success is True
