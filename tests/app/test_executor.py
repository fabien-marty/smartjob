import pytest

from smartjob.app.executor import ExecutorService
from smartjob.app.job import SmartJob
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.executor.dummy import DummyExecutorAdapter
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
