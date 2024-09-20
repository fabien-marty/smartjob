from smartjob.app.executor import ExecutorPort, ExecutorService

# Default max workers for vertex executor and for file uploader (cloud run executor is not multi-threaded)
DEFAULT_MAX_WORKERS = 10
__cache: dict[str, ExecutorService] = {}


def get_executor_service(
    type: str = "cloudrun",
    max_workers: int = DEFAULT_MAX_WORKERS,
    use_cache: bool = True,
) -> ExecutorService:
    """Return a singleton instance of ExecutorService (initialized on first call with given arguments).

    Args:
        type: The type of executor to use ('cloudrun', 'vertex' or 'docker').
        max_workers: Maximum number of workers for the vertex executor and for the file uploader.
        use_cache: if set to True, the same instance will be returned for the same type.

    Returns:
        Instance of ExecutorService.

    """
    adapter: ExecutorPort
    if type == "cloudrun":
        from smartjob.infra.adapters.executor.cloudrun import CloudRunExecutorAdapter

        adapter = CloudRunExecutorAdapter(max_workers=max_workers)
    elif type == "vertex":
        from smartjob.infra.adapters.executor.vertex import VertexExecutorAdapter

        adapter = VertexExecutorAdapter(max_workers=max_workers)
    elif type == "docker":
        from smartjob.infra.adapters.executor.docker import DockerExecutorAdapter

        adapter = DockerExecutorAdapter(max_workers=max_workers)
    else:
        raise ValueError(
            f"Invalid executor type: {type} => must be 'cloudrun', 'vertex' or 'docker'"
        )
    if not use_cache or type not in __cache:
        __cache[type] = ExecutorService(
            adapter=adapter,
        )
    return __cache[type]
