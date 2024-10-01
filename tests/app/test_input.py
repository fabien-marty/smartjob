import json
import os
from dataclasses import dataclass, field

import pytest

from smartjob.app.input import BytesInput, GcsInput, JsonInput, LocalPathInput
from smartjob.app.storage import StoragePort, StorageService

CURRENT_FILE = os.path.abspath(__file__)


@dataclass
class DummyStorageAdapter(StoragePort):
    data: dict[str, bytes | str] = field(default_factory=dict, init=False)

    def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        self.data[destination_path] = content

    def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        self.data[destination_path] = (
            f"copied from {source_bucket}/{source_path}".encode()
        )

    def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        return json.dumps({"dummy": "result"}).encode()


@pytest.fixture
def storage_service():
    return StorageService(adapter=DummyStorageAdapter())


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
