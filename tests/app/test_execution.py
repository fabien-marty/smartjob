import os
from unittest import mock

import pytest

from smartjob.app.execution import (
    Execution,
    ExecutionConfig,
    read_from_env,
)
from smartjob.app.executor import ExecutionResult
from smartjob.app.job import SmartJob


@pytest.fixture()
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "FOO": "BAR",
            "SMARTJOB_PROJECT": "project",
            "SMARTJOB_REGION": "region",
            "SMARTJOB_STAGING_BUCKET": "gs://staging-bucket",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield  # This is the magical bit which restore the environment after


def test_read_from_env(setenvvar):
    assert read_from_env("FOO") == "BAR"
    assert read_from_env("FOO2") is None
    assert read_from_env("FOO2", "BAR") == "BAR"


def test_execution_config(setenvvar):
    empty = ExecutionConfig()
    empty.fix_for_executor_name("cloudrun")
    assert empty._staging_bucket == "gs://staging-bucket"
    assert empty._staging_bucket_name == "staging-bucket"
    assert empty._project == "project"
    assert empty._region == "region"
    assert empty._retry_config.max_attempts == 1


def test_execution():
    exec = Execution(
        job=SmartJob("foo", docker_image="python:3.12"),
        config=ExecutionConfig(),
    )
    assert exec.base_dir.startswith("default-foo")
    assert exec.input_relative_path.startswith("default-foo")
    assert exec.input_relative_path.endswith("/input")
    assert exec.output_relative_path.startswith("default-foo")
    assert exec.output_relative_path.endswith("/output")


def test_execution_result():
    exec = Execution(
        job=SmartJob("foo", docker_image="python:3.12"),
        config=ExecutionConfig(),
    )
    er = ExecutionResult._from_execution(exec, True, "https://nolog.com")
    assert er.success
    assert bool(er)
    assert len(str(er)) > 10
    assert er.duration_seconds < 1
