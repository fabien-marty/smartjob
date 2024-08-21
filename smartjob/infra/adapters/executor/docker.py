import asyncio
import concurrent.futures
from dataclasses import dataclass, field

import docker
from stlog import LogContext, getLogger

from smartjob.app.execution import Execution, ExecutionResult
from smartjob.app.executor import ExecutionResultFuture, ExecutorPort
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.storage.docker import DockerStorageAdapter

logger = getLogger("smartjob.executor.docker")


class DockerExecutionResultFuture(ExecutionResultFuture):
    def _get_result_from_future(self, future: asyncio.Future) -> ExecutionResult:
        return future.result()


def make_storage_service() -> StorageService:
    return StorageService(adapter=DockerStorageAdapter())


@dataclass
class DockerExecutorAdapter(ExecutorPort):
    max_workers: int = 10
    sleep: float = 1.0
    storage_service: StorageService = field(default_factory=make_storage_service)
    _executor: concurrent.futures.ThreadPoolExecutor | None = field(
        default=None, init=False
    )

    def __post_init__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        )

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        assert self._executor is not None
        return self._executor

    def sync_run(
        self, execution: Execution, container_id: str, log_context: dict
    ) -> ExecutionResult:
        docker_client = docker.DockerClient()
        with LogContext.bind(
            **log_context
        ):  # we need to rebind here as we are now in another thread
            container = docker_client.containers.get(container_id)
            res = container.wait()
            logger.debug("Container stopped")
            return ExecutionResult._from_execution(
                execution, res["StatusCode"] == 0, f"docker logs -f {container.id}"
            )

    def load_docker_image_if_needed(self, docker_image: str):
        docker_client = docker.DockerClient()
        try:
            docker_client.images.get(docker_image)
            logger.debug(f"{docker_image} image already exists => let's use it")
        except Exception:
            logger.debug(f"let's pull {docker_image} image...")
            docker_client.images.pull(docker_image)
            logger.debug(f"{docker_image} image pulled")

    async def schedule(self, execution: Execution) -> ExecutionResultFuture:
        docker_client = docker.DockerClient()
        loop = asyncio.get_event_loop()
        job = execution.job
        name = f"{job.name}-{execution.id}"
        await loop.run_in_executor(
            self.executor, self.load_docker_image_if_needed, job.docker_image
        )
        container = docker_client.containers.create(
            name=name,
            image=job.docker_image,
            command=execution.overridden_args,
            auto_remove=False,
            volumes={"smartjob-staging": {"bind": "/staging", "mode": "rw"}},
            environment=execution.add_envs,
        )
        with LogContext.bind(container_id=container.id):
            logger.info(
                "Container created",
                container_name=name,
                image=job.docker_image,
                command=execution.overridden_args_as_string,
                env=execution.add_envs_as_string,
            )
            container.start()
            logger.info("Container started")
            future = loop.run_in_executor(
                self.executor,
                self.sync_run,
                execution,
                container.id,
                LogContext.getall(),
            )
            return DockerExecutionResultFuture(
                asyncio.ensure_future(future),
                execution,
                storage_service=self.storage_service,
                log_url=f"docker logs -f {container.id}",
            )

    def get_name(self):
        return "docker"

    def get_storage_service(self) -> StorageService:
        return self.storage_service

    def staging_mount_path(self, execution: Execution) -> str:
        return "/staging"
