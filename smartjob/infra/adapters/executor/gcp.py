from abc import abstractmethod
from dataclasses import dataclass, field

from stlog import getLogger

from smartjob.app.execution import Execution
from smartjob.app.executor import ExecutionResultFuture, ExecutorPort
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.storage.gcs import GcsStorageAdapter

logger = getLogger("smartjob.executor.gcp")


class GCPExecutionResultFuture(ExecutionResultFuture):
    pass


@dataclass
class GCPExecutor(ExecutorPort):
    """Abstract base class for GCP executors."""

    max_workers: int = 10
    _storage_service: StorageService | None = field(default=None, init=False)

    def __post_init__(self):
        self._storage_service = StorageService(
            adapter=GcsStorageAdapter(max_workers=self.max_workers)
        )

    @property
    def storage_service(self) -> StorageService:
        assert self._storage_service is not None
        return self._storage_service

    @abstractmethod
    def staging_mount_path(self, execution: Execution) -> str:
        pass

    def get_storage_service(self) -> StorageService:
        return self.storage_service
