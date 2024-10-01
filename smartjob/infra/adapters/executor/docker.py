import concurrent.futures
from dataclasses import dataclass, field

import docker
from stlog import LogContext, getLogger

from smartjob.app.execution import (
    Execution,
)
from smartjob.app.executor import (
    ExecutorPort,
    SchedulingDetails,
    _ExecutionResult,
)

logger = getLogger("smartjob.executor.docker")


@dataclass
class DockerExecutorAdapter(ExecutorPort):
    max_workers: int = 10
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

    def load_docker_image_if_needed(self, docker_image: str):
        docker_client = docker.DockerClient()
        try:
            docker_client.images.get(docker_image)
            logger.debug(f"{docker_image} image already exists => let's use it")
        except Exception:
            logger.debug(f"let's pull {docker_image} image...")
            docker_client.images.pull(docker_image)
            logger.debug(f"{docker_image} image pulled")

    def wait(
        self, execution: Execution, container_id: str, log_context: dict
    ) -> _ExecutionResult:
        """Note: executed in another thread."""
        with LogContext.bind(**log_context):
            try:
                docker_client = docker.DockerClient()
                container = docker_client.containers.get(container_id)
                res = container.wait()
                logger.debug("Container stopped")
            except Exception:
                logger.debug("exception catched during wait()", exc_info=True)
            return _ExecutionResult._from_execution(
                execution,
                success=res["StatusCode"] == 0,
                log_url=f"docker logs -f {container.id}",
            )

    def schedule(
        self, execution: Execution, forget: bool
    ) -> tuple[SchedulingDetails, concurrent.futures.Future[_ExecutionResult] | None]:
        docker_client = docker.DockerClient()
        job = execution.job
        name = f"{job.name}-{execution.id}"
        self.load_docker_image_if_needed(job.docker_image)
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
            scheduling_details = SchedulingDetails(
                execution_id=execution.id, log_url=f"docker logs -f {container.id}"
            )
            if forget:
                return scheduling_details, None
            future = self.executor.submit(
                self.wait, execution, container.id, LogContext.getall()
            )
            return scheduling_details, future

    def get_name(self):
        return "docker"

    def staging_mount_path(self, execution: Execution) -> str:
        return "/staging"
