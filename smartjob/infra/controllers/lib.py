from smartjob.app.executor import ExecutorPort, ExecutorService
from smartjob.app.storage import StoragePort, StorageService
from smartjob.infra.adapters.storage.docker import DockerStorageAdapter
from smartjob.infra.adapters.storage.gcs import GcsStorageAdapter

# Default max workers
DEFAULT_MAX_WORKERS = 10
__cache: dict[str, ExecutorService] = {}


def get_executor_service(
    type: str = "cloudrun",
    max_workers: int = DEFAULT_MAX_WORKERS,
    use_cache: bool = True,
) -> ExecutorService:
    """Return a singleton instance of ExecutorService (initialized on first call with given arguments).

    Args:
        type: The type of executor to use ('cloudrun', 'cloudbatch', 'vertex' or 'docker').
        max_workers: Maximum number of workers
        use_cache: if set to True, the same instance will be returned for the same type.

    Returns:
        Instance of ExecutorService.

    """
    adapter: ExecutorPort
    storage_adapter: StoragePort
    if type == "cloudrun":
        from smartjob.infra.adapters.executor.cloudrun import CloudRunExecutorAdapter

        adapter = CloudRunExecutorAdapter(max_workers=max_workers)
        storage_adapter = GcsStorageAdapter()
    elif type == "cloudbatch":
        from smartjob.infra.adapters.executor.cloudbatch import (
            CloudBatchExecutorAdapter,
        )

        adapter = CloudBatchExecutorAdapter(max_workers=max_workers)
        storage_adapter = GcsStorageAdapter()
    elif type == "vertex":
        from smartjob.infra.adapters.executor.vertex import VertexExecutorAdapter

        adapter = VertexExecutorAdapter(max_workers=max_workers)
        storage_adapter = GcsStorageAdapter()
    elif type == "docker":
        from smartjob.infra.adapters.executor.docker import DockerExecutorAdapter

        adapter = DockerExecutorAdapter(max_workers=max_workers)
        storage_adapter = DockerStorageAdapter()
    else:
        raise ValueError(
            f"Invalid executor type: {type} => must be 'cloudrun', 'cloudbatch', 'vertex' or 'docker'"
        )
    if not use_cache or type not in __cache:
        __cache[type] = ExecutorService(
            adapter=adapter,
            storage_service=StorageService(adapter=storage_adapter),
        )
    return __cache[type]
