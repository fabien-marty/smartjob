from typing import Any

from smartjob.app.executor import SmartJobExecutorService
from smartjob.infra.adapters.executor.cloudrun import CloudRunSmartJobExecutor
from smartjob.infra.adapters.executor.vertex import VertexSmartJobExecutor
from smartjob.infra.adapters.file_uploader.gcs import GcsFileUploaderAdapter

__singleton: SmartJobExecutorService | None = None


def get_smart_job_executor_service_singleton(
    max_workers: int = 10,
    namespace: str | None = None,
    project: str | None = None,
    region: str | None = None,
    staging_bucket: str | None = None,
    docker_image: str | None = None,
    input_bucket_base_path: str | None = None,
    output_bucket_base_path: str | None = None,
) -> SmartJobExecutorService:
    global __singleton
    if __singleton is None:
        kwargs: dict[str, Any] = {}
        if input_bucket_base_path:
            kwargs["input_bucket_base_path"] = input_bucket_base_path
        if output_bucket_base_path:
            kwargs["output_bucket_base_path"] = output_bucket_base_path
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
        __singleton = SmartJobExecutorService(
            cloudrun_executor_adapter=CloudRunSmartJobExecutor(),
            vertex_executor_adapter=VertexSmartJobExecutor(max_workers=max_workers),
            file_uploader_adapter=GcsFileUploaderAdapter(max_workers=max_workers),
            **kwargs,
        )
    return __singleton
