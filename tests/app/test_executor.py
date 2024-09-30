import os

import pytest
import stlog

from smartjob.app.executor import ExecutorService
from smartjob.app.input import BytesInput
from smartjob.app.job import SmartJob
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.executor.docker import DockerExecutorAdapter
from smartjob.infra.adapters.storage.docker import DockerStorageAdapter

DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_SCRIPT_PATH = os.path.join(DIR, "data", "python_script.py")


@pytest.fixture
def storage_service() -> StorageService:
    return StorageService(adapter=DockerStorageAdapter())


@pytest.fixture
def executor_service(storage_service) -> ExecutorService:
    return ExecutorService(
        adapter=DockerExecutorAdapter(), storage_service=storage_service
    )


stlog.setup(level="INFO")


def test_basic(executor_service: ExecutorService):
    job = SmartJob(
        "foo",
        docker_image="docker.io/python:3.12",
        python_script_path=PYTHON_SCRIPT_PATH,
    )
    result = executor_service.run(job)
    assert result.success is True
    assert isinstance(result.json_output, dict)
    assert len(result.json_output["execution_id"]) > 0


def test_with_inputs(executor_service: ExecutorService):
    job = SmartJob(
        "foo",
        docker_image="docker.io/python:3.12",
        python_script_path=PYTHON_SCRIPT_PATH,
    )
    result = executor_service.run(
        job, inputs=[BytesInput(filename="input1", content="foo")]
    )
    assert result.success is True
    assert isinstance(result.json_output, dict)
    assert len(result.json_output["execution_id"]) > 0
    assert result.json_output["input1"] == "foo"


def test_schedule(executor_service: ExecutorService):
    job = SmartJob("foo", docker_image="python:3.12")
    _, fer = executor_service.schedule(job)
    assert fer is not None
    result = fer.result()
    assert result.success is True


def test_schedule_and_forget(executor_service: ExecutorService):
    job = SmartJob("foo", docker_image="python:3.12")
    details, fer = executor_service.schedule(job, forget=True)
    assert fer is None
    assert len(details.execution_id) > 0
