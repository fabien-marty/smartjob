import datetime
import json
import typing
from dataclasses import dataclass

from smartjob.app.exception import SmartJobException
from smartjob.app.storage import StoragePort


@dataclass
class Input:
    filename: str

    async def _create(self, bucket: str, path: str, storage_adapter: StoragePort):
        raise NotImplementedError("must be implemented in subclasses")


@dataclass
class BytesInput(Input):
    content: bytes

    async def _create(self, bucket: str, path: str, storage_adapter: StoragePort):
        await storage_adapter.upload(self.content, bucket, f"{path}/{self.filename}")


@dataclass
class JsonInput(Input):
    content: typing.Any

    def _json_default(self, value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return "*** NOT JSON SERIALIZABLE ***"

    def _as_bytes(self) -> bytes:
        return json.dumps(self.content, indent=4, default=self._json_default).encode(
            "utf-8"
        )

    async def _create(self, bucket: str, path: str, storage_adapter: StoragePort):
        bytesInput = BytesInput(filename=self.filename, content=self._as_bytes())
        await bytesInput._create(bucket, path, storage_adapter)


@dataclass
class GcsInput(Input):
    gcs_path: str

    def __post_init__(self):
        if not self.gcs_path.startswith("gs://"):
            raise SmartJobException("gcs_path must start with gs://")
        if "/" not in self.gcs_path[5:]:
            raise SmartJobException(
                "gcs_path must contain a / (excluding the gs:// prefix)"
            )

    async def _create(self, bucket: str, path: str, storage_adapter: StoragePort):
        source_bucket = self.gcs_path[5:].split("/")[0]
        source_path = self.gcs_path[5:].split("/", 1)[1]
        await storage_adapter.copy(
            source_bucket, source_path, bucket, f"{path}/{self.filename}"
        )
