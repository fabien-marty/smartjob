from abc import ABC, abstractmethod


class StoragePort(ABC):
    @abstractmethod
    async def download(
        self,
        source_bucket: str,
        source_path: str,
    ) -> bytes:
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
