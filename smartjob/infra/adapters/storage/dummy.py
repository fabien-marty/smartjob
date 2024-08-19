import asyncio
import json
from dataclasses import dataclass, field

from smartjob.app.storage import StoragePort


@dataclass
class DummyStorageAdapter(StoragePort):
    sleep: float = 1.0
    keep_in_memory: bool = False
    data: dict[str, bytes | str] = field(default_factory=dict, init=False)

    async def _sleep(self):
        await asyncio.sleep(self.sleep)

    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        await self._sleep()
        if self.keep_in_memory:
            self.data[destination_path] = content

    async def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        await self._sleep()
        if self.keep_in_memory:
            self.data[destination_path] = (
                f"copied from {source_bucket}/{source_path}".encode()
            )

    async def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        await self._sleep()
        return json.dumps({"dummy": "result"}).encode()
