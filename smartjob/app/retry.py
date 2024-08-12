from dataclasses import dataclass


@dataclass
class RetryConfig:
    max_attempts: int = 3
    max_attempts_schedule: int | None = None
    max_attempts_execute: int | None = None
    max_attempts_download: int | None = None

    @property
    def _max_attempts_schedule(self) -> int:
        return self.max_attempts_schedule or self.max_attempts

    @property
    def _max_attempts_execute(self) -> int:
        return self.max_attempts_schedule or self.max_attempts

    @property
    def _max_attempts_download(self) -> int:
        return self.max_attempts_download or self.max_attempts
