from dataclasses import dataclass


@dataclass
class RetryConfig:
    """Retry configuration."""

    max_attempts: int = 3
    """FIXME"""

    max_attempts_schedule: int | None = None
    """FIXME"""

    max_attempts_execute: int | None = None
    """FIXME"""

    max_attempts_download: int | None = None
    """FIXME"""

    @property
    def _max_attempts_schedule(self) -> int:
        return self.max_attempts_schedule or self.max_attempts

    @property
    def _max_attempts_execute(self) -> int:
        return self.max_attempts_schedule or self.max_attempts

    @property
    def _max_attempts_download(self) -> int:
        return self.max_attempts_download or self.max_attempts

    def __post_init__(self):
        for field in (
            "max_attempts",
            "max_attempts_schedule",
            "max_attempts_execute",
            "max_attempts_download",
        ):
            if getattr(self, field) is not None and getattr(self, field) < 1:
                raise ValueError(f"{field} must be >= 1")
