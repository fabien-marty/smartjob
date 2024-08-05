import asyncio
import concurrent.futures
from typing import Any

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage  # type: ignore

from smartjob.app.exception import SmartJobException
from smartjob.app.storage import StoragePort


class GcsStorageAdapter(StoragePort):
    def __init__(self, max_workers: int = 10):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.storage_client = storage.Client()

    def sync_upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
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
            blob.upload_from_string(content, **kwargs)
        except PreconditionFailed:
            pass
        return f"gs://{destination_bucket}/{destination_path}"

    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        cf_future = self.executor.submit(
            self.sync_upload,
            content,
            destination_bucket,
            destination_path,
            only_if_not_exists=only_if_not_exists,
        )
        return await asyncio.wrap_future(cf_future)

    def sync_copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        kwargs: dict[str, Any] = {}
        if only_if_not_exists:
            kwargs["if_generation_match"] = 0
        sb = self.storage_client.bucket(source_bucket)
        source_blob = sb.blob(source_path)
        db = self.storage_client.bucket(destination_bucket)
        sb.copy_blob(source_blob, db, destination_path, **kwargs)
        return f"gs://{destination_bucket}/{destination_path}"

    async def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        cf_future = self.executor.submit(
            self.sync_copy,
            source_bucket,
            source_path,
            destination_bucket,
            destination_path,
            only_if_not_exists=only_if_not_exists,
        )
        return await asyncio.wrap_future(cf_future)

    def sync_download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        b = self.storage_client.bucket(source_bucket)
        blob = b.blob(source_path)
        try:
            return blob.download_as_bytes()
        except NotFound:
            print("not found")
            return b""

    async def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        cf_future = self.executor.submit(
            self.sync_download,
            source_bucket,
            source_path,
        )
        return await asyncio.wrap_future(cf_future)
