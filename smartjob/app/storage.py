from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from stlog import LogContext
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from smartjob.app.retry import RetryConfig


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
    retry_config: RetryConfig = field(default_factory=RetryConfig)

    async def download(self, source_bucket: str, source_path: str) -> bytes:
        try:
            async for attempt in AsyncRetrying(
                reraise=True,
                wait=wait_exponential(min=1, max=300, multiplier=1),
                stop=stop_after_attempt(self.retry_config._max_attempts_download),
            ):
                with attempt:
                    with LogContext.bind(attempt=attempt.retry_state.attempt_number):
                        return await self.adapter.download(source_bucket, source_path)
        except RetryError:
            raise
        return b""

    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ) -> str:
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
    ) -> str:
        return await self.adapter.copy(
            source_bucket,
            source_path,
            destination_bucket,
            destination_path,
            only_if_not_exists=only_if_not_exists,
        )
