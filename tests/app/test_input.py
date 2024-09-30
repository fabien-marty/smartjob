import json
import os

import pytest

from smartjob.app.input import BytesInput, GcsInput, JsonInput, LocalPathInput
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.storage.dummy import DummyStorageAdapter

CURRENT_FILE = os.path.abspath(__file__)


@pytest.fixture
def storage_service():
    return StorageService(adapter=DummyStorageAdapter(sleep=0, keep_in_memory=True))


def test_bytes_input(storage_service):
    input = BytesInput(filename="bar", content=b"foo")
    input._create("bucket", "path1/path2", storage_service)
    assert storage_service.adapter.data["path1/path2/bar"] == b"foo"


def test_json_input(storage_service):
    input = JsonInput(filename="jsonbar", content={"foo": "bar"})
    input._create("bucket", "path1/path2", storage_service)
    res = json.loads(storage_service.adapter.data["path1/path2/jsonbar"])
    assert res["foo"] == "bar"


def test_local_path_input(storage_service):
    input = LocalPathInput(filename="localbar", local_path=CURRENT_FILE)
    input._create("bucket", "path1/path2", storage_service)
    content1 = storage_service.adapter.data["path1/path2/localbar"]
    with open(CURRENT_FILE, "rb") as f:
        content2 = f.read()
    assert content1 == content2


def test_gcs_input(storage_service):
    input = GcsInput(filename="gcs", gcs_path="gs://foo/bar")
    input._create("bucket", "path1/path2", storage_service)
    content1 = storage_service.adapter.data["path1/path2/gcs"]
    assert content1 == b"copied from foo/bar"
