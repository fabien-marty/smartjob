import os
from typing import Any

import stlog

from smartjob.app.executor import ExecutorService
from smartjob.infra.adapters.executor.cloudrun import CloudRunExecutor
from smartjob.infra.adapters.executor.dummy import DummyExecutor
from smartjob.infra.adapters.executor.vertex import VertexExecutor
from smartjob.infra.adapters.storage.dummy import DummyStorageAdapter
from smartjob.infra.adapters.storage.gcs import GcsStorageAdapter

# Default max workers for vertex executor and for file uploader (cloud run executor is not multi-threaded)
DEFAULT_MAX_WORKERS = 10
__singleton: ExecutorService | None = None


def get_executor_service_singleton(
    max_workers: int = DEFAULT_MAX_WORKERS,
    namespace: str | None = None,
    project: str | None = None,
    region: str | None = None,
    staging_bucket: str | None = None,
    docker_image: str | None = None,
) -> ExecutorService:
    """Return a singleton instance of ExecutorService (initialized on first call with given arguments).

    Most of the arguments are optional and can also be set via environment variables or at SmartJob level.

    See ExecutorService or SmartJob for more details on the arguments.

    Note: if SMARTJOB_USE_DUMMY_EXECUTOR is set to 'true', a dummy executor will be returned.
          (only useful for testing or debugging)

    Args:
        max_workers: Maximum number of workers for the vertex executor and for the file uploader.
        namespace: Default namespace to use.
        project: Default project to use.
        region: Default region to use.
        staging_bucket: Default staging bucket to use (for input/output/local script uploading)
        docker_image: Default docker image to use.

    Returns:
        Instance of ExecutorService.

    """
    global __singleton
    kwargs: dict[str, Any] = {}
    if docker_image:
        kwargs["docker_image"] = docker_image
    if staging_bucket:
        kwargs["staging_bucket"] = staging_bucket
    if region:
        kwargs["region"] = region
    if project:
        kwargs["project"] = project
    if namespace:
        kwargs["namespace"] = namespace
    if os.environ.get("SMARTJOB_USE_DUMMY_EXECUTOR", "false").lower() == "true":
        stlog.getLogger("smartjob").warning(
            "Using dummy executor because SMARTJOB_USE_DUMMY_EXECUTOR is set to true"
        )
        return ExecutorService(
            cloudrun_executor_adapter=DummyExecutor(),
            vertex_executor_adapter=DummyExecutor(),
            storage_adapter=DummyStorageAdapter(),
            **kwargs,
        )
    if __singleton is None:
        __singleton = ExecutorService(
            cloudrun_executor_adapter=CloudRunExecutor(),
            vertex_executor_adapter=VertexExecutor(max_workers=max_workers),
            storage_adapter=GcsStorageAdapter(max_workers=max_workers),
            **kwargs,
        )
    return __singleton
