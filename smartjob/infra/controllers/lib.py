from smartjob.app.executor import ExecutorPort, ExecutorService
from smartjob.infra.adapters.executor.cloudrun import CloudRunExecutorAdapter
from smartjob.infra.adapters.executor.vertex import VertexExecutorAdapter

# Default max workers for vertex executor and for file uploader (cloud run executor is not multi-threaded)
DEFAULT_MAX_WORKERS = 10
__cache: dict[str, ExecutorService] = {}


def get_executor_service(
    type: str = "cloudrun",
    max_workers: int = DEFAULT_MAX_WORKERS,
    use_cache: bool = True,
) -> ExecutorService:
    """Return a singleton instance of ExecutorService (initialized on first call with given arguments).

    Most of the arguments are optional and can also be set via environment variables or at SmartJob level.

    See ExecutorService or SmartJob for more details on the arguments.

    Note: if SMARTJOB_USE_DUMMY_EXECUTOR is set to 'true', a dummy executor will be returned.
          (only useful for testing or debugging)

    Args:
        type: The type of executor to use ('cloudrun' or 'vertex').
        max_workers: Maximum number of workers for the vertex executor and for the file uploader.

    Returns:
        Instance of ExecutorService.

    """
    adapter: ExecutorPort
    if type == "cloudrun":
        adapter = CloudRunExecutorAdapter(max_workers=max_workers)
    elif type == "vertex":
        adapter = VertexExecutorAdapter(max_workers=max_workers)
    else:
        raise ValueError(
            f"Invalid executor type: {type} => must be 'cloudrun' or 'vertex'"
        )
    if not use_cache or type not in __cache:
        __cache[type] = ExecutorService(
            adapter=adapter,
        )
    return __cache[type]
