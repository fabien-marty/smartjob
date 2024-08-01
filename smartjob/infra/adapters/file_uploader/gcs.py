import asyncio
import concurrent.futures
import os
from typing import Any

from google.cloud import storage  # type: ignore

from smartjob.app.exception import SmartJobException
from smartjob.app.file_uploader import FileUploaderPort

from google.api_core.exceptions import PreconditionFailed


class GcsFileUploaderAdapter(FileUploaderPort):
    def __init__(self, max_workers: int = 10):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.storage_client = storage.Client()

    def sync_upload(
        self,
        content: str,
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
        self, content: str, destination_bucket: str, destination_path: str
    ) -> str:
        cf_future = self.executor.submit(
            self.sync_upload,
            content,
            destination_bucket,
            destination_path,
        )
        return await asyncio.wrap_future(cf_future)
