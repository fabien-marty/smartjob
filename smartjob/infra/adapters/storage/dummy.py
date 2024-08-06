import asyncio
import json

from smartjob.app.storage import StoragePort


class DummyStorageAdapter(StoragePort):
    def __init__(self, sleep: float = 1.0):
        self._sleep = sleep

    async def sleep(self):
        await asyncio.sleep(self._sleep)

    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        await self.sleep()
        return f"{destination_bucket}/{destination_path}"

    async def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        await self.sleep()
        return f"{destination_bucket}/{destination_path}"

    async def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
        await self.sleep()
        return json.dumps({"dummy": "result"}).encode()
