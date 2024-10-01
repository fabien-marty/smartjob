from typing import Any

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage  # type: ignore

from smartjob.app.exception import SmartJobException
from smartjob.app.storage import StoragePort


class GcsStorageAdapter(StoragePort):
    def __init__(self):
        self.storage_client = storage.Client()

    def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        if not destination_path:
            raise SmartJobException("destination_path is required")
        if not destination_bucket:
            raise SmartJobException("bucket is required")
        b = self.storage_client.bucket(destination_bucket)
        blob = b.blob(destination_path)
        kwargs: dict[str, Any] = {}
        if only_if_not_exists:
            kwargs["if_generation_match"] = 0
        try:
            blob.upload_from_string(content, **kwargs, retry=None)
        except PreconditionFailed:
            pass

    def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        kwargs: dict[str, Any] = {"retry": None}
        if only_if_not_exists:
            kwargs["if_generation_match"] = 0
        sb = self.storage_client.bucket(source_bucket)
        source_blob = sb.blob(source_path)
        db = self.storage_client.bucket(destination_bucket)
        sb.copy_blob(source_blob, db, destination_path, **kwargs)

    def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        b = self.storage_client.bucket(source_bucket)
        blob = b.blob(source_path)
        try:
            return blob.download_as_bytes(retry=None)
        except NotFound:
            return b""
