from abc import ABC, abstractmethod


class FileUploaderPort(ABC):
    @abstractmethod
    async def upload(
        self,
        content: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
        pass
