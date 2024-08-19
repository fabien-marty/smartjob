from abc import ABC, abstractmethod
from dataclasses import dataclass


class StoragePort(ABC):
    @abstractmethod
    async def download(self, source_bucket: str, source_path: str) -> bytes:
        pass

    @abstractmethod
    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        pass

    @abstractmethod
    async def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        pass


@dataclass
class StorageService:
    adapter: StoragePort

    async def download(self, source_bucket: str, source_path: str) -> bytes:
        return await self.adapter.download(source_bucket, source_path)

    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        return await self.adapter.upload(
            content, destination_bucket, destination_path, only_if_not_exists
        )

    async def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        return await self.adapter.copy(
            source_bucket,
            source_path,
            destination_bucket,
            destination_path,
            only_if_not_exists=only_if_not_exists,
        )
