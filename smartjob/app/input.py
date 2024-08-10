import datetime
import json
import typing
from dataclasses import dataclass

from smartjob.app.exception import SmartJobException
from smartjob.app.storage import StorageService


@dataclass
class Input:
    """Abstract base class for inputs. Do not use directly."""

    filename: str
    """File name to be used in the input path (without /)."""

    async def _create(self, bucket: str, path: str, storage_service: StorageService):
        raise NotImplementedError("must be implemented in subclasses")

    def __post_init__(self):
        if "/" in self.filename:
            raise SmartJobException("filename cannot contain /")


@dataclass
class BytesInput(Input):
    """Represents an input given as bytes or str."""

    content: bytes | str
    """Content to be written to the input path (can be bytes or string)."""

    async def _create(self, bucket: str, path: str, storage_service: StorageService):
        await storage_service.upload(self.content, bucket, f"{path}/{self.filename}")


@dataclass
class JsonInput(Input):
    """Represents an input given as a JSON-serializable object."""

    content: typing.Any
    """Content to be seriliazed to JSON and written to the input path."""

    def _json_default(self, value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return "*** NOT JSON SERIALIZABLE ***"

    def _as_bytes(self) -> bytes:
        return json.dumps(self.content, indent=4, default=self._json_default).encode(
            "utf-8"
        )

    async def _create(self, bucket: str, path: str, storage_service: StorageService):
        bytesInput = BytesInput(filename=self.filename, content=self._as_bytes())
        await bytesInput._create(bucket, path, storage_service)


@dataclass
class LocalPathInput(Input):
    """Represents an input given as a local file path."""

    local_path: str
    """Local path to the file to be copied to the input path."""

    async def _create(self, bucket: str, path: str, storage_service: StorageService):
        with open(self.local_path, "rb") as f:
            content = f.read()
        bytesInput = BytesInput(filename=self.filename, content=content)
        await bytesInput._create(bucket, path, storage_service)


@dataclass
class GcsInput(Input):
    """Can be used to copy a file from a GCS bucket to the input path."""

    gcs_path: str
    """GCS path to the file to be copied (e.g. gs://my-bucket/my-file.txt)."""

    def __post_init__(self):
        super().__post_init__()
        if not self.gcs_path.startswith("gs://"):
            raise SmartJobException("gcs_path must start with gs://")
        if "/" not in self.gcs_path[5:]:
            raise SmartJobException(
                "gcs_path must contain a / (excluding the gs:// prefix)"
            )

    async def _create(self, bucket: str, path: str, storage_service: StorageService):
        source_bucket = self.gcs_path[5:].split("/")[0]
        source_path = self.gcs_path[5:].split("/", 1)[1]
        await storage_service.copy(
            source_bucket, source_path, bucket, f"{path}/{self.filename}"
        )
