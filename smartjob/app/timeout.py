from dataclasses import dataclass


@dataclass
class TimeoutConfig:
    """Timeout configuration."""

    timeout_seconds: int = 3600
    """FIXME"""

    timeout_seconds_schedule: int | None = None
    """FIXME"""

    timeout_seconds_download: int | None = None
    """FIXME"""

    @property
    def _timeout_seconds_schedule(self) -> int:
        return self.timeout_seconds_schedule or self.timeout_seconds

    @property
    def _timeout_seconds_download(self) -> int:
        return self.timeout_seconds_download or self.timeout_seconds

    def __post_init__(self):
        for field in (
            "timeout_seconds",
            "timeout_seconds_schedule",
            "timeout_seconds_download",
        ):
            if getattr(self, field) is not None and getattr(self, field) <= 0:
                raise ValueError(f"{field} must be > 0")
