import pytest
import stlog

from smartjob.app.executor import ExecutorService
from smartjob.app.job import SmartJob
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.executor.dummy import (
    DummyExecutorAdapter,
)
from smartjob.infra.adapters.storage.dummy import DummyStorageAdapter


@pytest.fixture
def storage_service():
    return StorageService(adapter=DummyStorageAdapter(sleep=1, keep_in_memory=True))


@pytest.fixture
def executor_service(storage_service) -> ExecutorService:
    adapter = DummyExecutorAdapter(sleep=0)
    return ExecutorService(adapter=adapter, storage_service=storage_service)


def xxxtest_basic(executor_service: ExecutorService):
    job = SmartJob("foo", docker_image="python:3.12")
    result = executor_service.run(job)
    assert result.success is True


def test_schedule(executor_service: ExecutorService):
    job = SmartJob("foo", docker_image="python:3.12")
    sr, fer = executor_service.schedule(job)
    assert sr.success is True
    assert fer is not None
    result = fer.result()
    assert result.success is True
