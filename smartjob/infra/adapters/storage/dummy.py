import json
from dataclasses import dataclass, field
import time

from smartjob.app.storage import StoragePort


@dataclass
class DummyStorageAdapter(StoragePort):
    sleep: float = 1.0
    keep_in_memory: bool = False
    data: dict[str, bytes | str] = field(default_factory=dict, init=False)

    def _sleep(self):
        time.sleep(self.sleep)

    def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        self._sleep()
        if self.keep_in_memory:
            self.data[destination_path] = content

    def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        self._sleep()
        if self.keep_in_memory:
            self.data[destination_path] = (
                f"copied from {source_bucket}/{source_path}".encode()
            )

    def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        self._sleep()
        return json.dumps({"dummy": "result"}).encode()
