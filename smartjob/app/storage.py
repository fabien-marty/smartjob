from abc import ABC, abstractmethod
from dataclasses import dataclass

# FIXME: timeouts


class StoragePort(ABC):
    @abstractmethod
    def download(self, source_bucket: str, source_path: str) -> bytes:
        pass

    @abstractmethod
    def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        pass

    @abstractmethod
    def copy(
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

    def download(self, source_bucket: str, source_path: str) -> bytes:
        return self.adapter.download(source_bucket, source_path)

    def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        return self.adapter.upload(
            content, destination_bucket, destination_path, only_if_not_exists
        )

    def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        return self.adapter.copy(
            source_bucket,
            source_path,
            destination_bucket,
            destination_path,
            only_if_not_exists=only_if_not_exists,
        )
