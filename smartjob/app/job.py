import datetime
import hashlib
import uuid
from dataclasses import dataclass, field

from smartjob.app.exception import SmartJobException

# DEFAULT_NAMESPACE is the default namespace for jobs.
DEFAULT_NAMESPACE = "default"

# DEFAULT_TIMEOUT_SECONDS is the default timeout in seconds for jobs.
DEFAULT_TIMEOUT_SECONDS = 3600


def _hex_hash(*args: str) -> str:
    return hashlib.sha1("\n".join(args).encode()).hexdigest()


def _unique_id() -> str:
    return str(uuid.uuid4()).replace("-", "")


@dataclass
class SmartJob:
    """Base class for smart jobs."""

    name: str
    """Name of the job."""

    namespace: str = DEFAULT_NAMESPACE
    """Namespace of the job."""

    project: str = ""
    """GCP project to use for executing the job.

    If not set here, the value will be forced by the SmartJobExecutorService object.
    """

    region: str = ""
    """GCP region where to execute the job.

    If not set here, the value will be forced by the SmartJobExecutorService object.
    """

    docker_image: str = ""
    """Docker image to use for the job.

    Example: `docker.io/python:3.12`.
    """

    add_envs: dict[str, str] = field(default_factory=dict)
    """Environnement variables to add in the container.

    Note: `INPUT_PATH` and `OUTPUT_PATH` will be automatically injected (if relevant).
    """

    overridden_args: list[str] = field(default_factory=list)
    """Container arguments (including command) to use.

    If not set, the default container image arguments will be used.

    """

    python_script_path: str = ""
    staging_bucket: str = ""
    input_bucket_path: str = ""
    output_bucket_path: str = ""
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    service_account: str | None = None

    def assert_is_ready(self):
        """Test if the job is ready to be run.

        It raises an exception if the job is not ready to be run.
        If it's ok, it does nothing.

        Raises:
            SmartJobException: If the job is not ready to be run.

        """
        if not self.name:
            raise SmartJobException("name is required for job")
        if not self.project:
            raise SmartJobException("project is required")
        if not self.region:
            raise SmartJobException("region is required")
        if not self.docker_image:
            raise SmartJobException("docker_image is required")
        if self.staging_bucket:
            if not self.staging_bucket.startswith("gs://"):
                raise SmartJobException("staging_bucket must start with gs://")
            if "/" in self.staging_bucket[5:]:
                raise SmartJobException(
                    "staging_bucket name must not contain / (except for the the gs:// prefix)"
                )
        elif self.python_script_path:
            raise SmartJobException("staging_bucket is required for python_script_path")

    def python_script_hash(self) -> str:
        """Return the hash of the python script.

        If the python_script_path is not set, it returns an empty string.

        """
        if not self.python_script_path:
            return ""
        with open(self.python_script_path) as f:
            return hashlib.sha1(f.read().encode()).hexdigest()

    @property
    def staging_bucket_name(self) -> str:
        """Return the name of the staging bucket without the gs:// prefix.

        If it does not start with gs://, it returns an empty string.

        """
        if not self.staging_bucket.startswith("gs://"):
            return ""
        return self.staging_bucket[5:]  # let's remove gs:// prefix

    @property
    def input_bucket_name(self) -> str:
        """Return the name of the input bucket without the gs:// prefix.

        If it does not start with gs://, it returns an empty string.

        """
        if not self.input_bucket_path.startswith("gs://"):
            return ""
        return self.input_bucket_path[5:].split("/")[0]

    @property
    def _input_path(self) -> str:
        try:
            return "/".join(self.input_bucket_path[5:].split("/")[1:])
        except Exception:
            return ""

    @property
    def _output_path(self) -> str:
        try:
            return "/".join(self.output_bucket_path[5:].split("/")[1:])
        except Exception:
            return ""

    @property
    def output_bucket_name(self) -> str:
        """Return the name of the output bucket without the gs:// prefix.

        If it does not start with gs://, it returns an empty string.

        """
        if not self.output_bucket_path.startswith("gs://"):
            return ""
        return self.output_bucket_path[5:].split("/")[0]

    @property
    def staging_mount_point(self) -> str:
        """Return the mount point for the staging bucket."""
        raise NotImplementedError("mount_point must be implemented in subclasses")

    @property
    def input_mount_point(self) -> str:
        """Return the mount point for the input bucket."""
        raise NotImplementedError("mount_point must be implemented in subclasses")

    @property
    def output_mount_point(self) -> str:
        """Return the mount point for the output bucket."""
        raise NotImplementedError("mount_point must be implemented in subclasses")

    @property
    def input_path(self) -> str:
        """Return the input full path."""
        return f"{self.input_mount_point}/{self._input_path}"

    @property
    def output_path(self) -> str:
        """Return the output full path."""
        return f"{self.output_mount_point}/{self._output_path}"

    @property
    def full_name(self) -> str:
        """Return the full name of the job (namespace + name)."""
        return f"{self.namespace}-{self.name}"

    def set_auto_input_bucket_path(
        self, input_bucket_base_path: str, execution_id: str
    ):
        if not input_bucket_base_path.startswith("gs://"):
            raise SmartJobException("input_bucket_base_path must start with gs://")
        self.input_bucket_path = f"{input_bucket_base_path}/{self.full_name}/{execution_id[0:2]}/{execution_id}/input"

    def set_auto_output_bucket_path(
        self, output_bucket_base_path: str, execution_id: str
    ):
        if not output_bucket_base_path.startswith("gs://"):
            raise SmartJobException("output_bucket_base_path must start with gs://")
        self.output_bucket_path = f"{output_bucket_base_path}/{self.full_name}/{execution_id[0:2]}/{execution_id}/output"


@dataclass
class CloudRunSmartJob(SmartJob):
    # cpu is the number of requested CPUs.
    cpu: float = 1

    # memory_gb is the requested memory in Gb.
    memory_gb: float = 0.5

    @property
    def job_hash(self) -> str:
        """Return a short stable job_hash.

        If the job_hash is the same, we don't have to create a new CloudRunJob instance
        (but only a new job execution).

        """
        args: list[str] = [self.name, self.project, self.region, self.docker_image]
        args += [self.namespace, self.staging_bucket, self.service_account or ""]
        args += [self.input_bucket_name, self.output_bucket_name]
        return _hex_hash(*args)[:10]

    @property
    def _parent_name(self) -> str:
        return f"projects/{self.project}/locations/{self.region}"

    @property
    def cloud_run_job_name(self) -> str:
        """Return the cloud run job name."""
        return f"{self.namespace}-{self.name}-{self.job_hash}"

    @property
    def full_cloud_run_job_name(self) -> str:
        """Return the full cloud run job name."""
        return f"{self._parent_name}/jobs/{self.cloud_run_job_name}"

    @property
    def _cpuLimit(self) -> str:
        return "%f" % self.cpu

    @property
    def _memoryLimit(self) -> str:
        return "%f" % self.memory_gb + "Gi"

    @property
    def staging_mount_point(self) -> str:
        """Return the mount point for the staging bucket."""
        return "/staging"

    @property
    def input_mount_point(self) -> str:
        """Return the mount point for the input bucket."""
        return "/input"

    @property
    def output_mount_point(self) -> str:
        """Return the mount point for the output bucket."""
        return "/output"


@dataclass
class VertexSmartJob(SmartJob):
    machine_type: str = "n1-standard-4"
    accelerator_type: str = "ACCELERATOR_TYPE_UNSPECIFIED"
    accelerator_count: int = 0
    boot_disk_type: str = "pd-ssd"
    boot_disk_size_gb: int = 100

    @property
    def staging_mount_point(self) -> str:
        """Return the mount point for the staging bucket."""
        return f"/gcs/{self.staging_bucket_name}"

    @property
    def input_mount_point(self) -> str:
        """Return the mount point for the input bucket."""
        return f"/gcs/{self.input_bucket_name}"

    @property
    def output_mount_point(self) -> str:
        """Return the mount point for the output bucket."""
        return f"/gcs/{self.output_bucket_name}"

    def assert_is_ready(self):
        """Test if the job is ready to be run.

        It raises an exception if the job is not ready to be run.
        If it's ok, it does nothing.

        Raises:
            SmartJobException: If the job is not ready to be run.

        """
        super().assert_is_ready()
        if not self.staging_bucket:
            raise SmartJobException("staging_bucket is required for vertex jobs")


@dataclass
class SmartJobExecution:
    job: SmartJob
    id: str = field(default_factory=_unique_id)
    created: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(tz=datetime.timezone.utc),
        init=False,
    )
    log_url: str | None = field(default=None, init=False)
    cancelled: bool = field(default=False, init=False)
    add_envs: dict[str, str] = field(default_factory=dict)
    overridden_args: list[str] = field(default_factory=list)

    @property
    def short_id(self) -> str:
        """Return a short id."""
        if self.id is None:
            raise SmartJobException("execution_id is not set")
        return self.id[:8]


@dataclass
class SmartJobExecutionResult:
    """SmartJobExecutionResult is the (final) result of a job execution.

    Attributes:
        success: Whether the job has succeeded or not.
        created: The datetime when the job has started.
        stopped: The datetime when the job has stopped.
        execution_id: The execution id of the job.
        short_execution_id: The short execution id of the job.
        job_name: The name of the job.
        job_namespace: The namespace of the job.

    """

    success: bool
    created: datetime.datetime
    execution_id: str
    short_execution_id: str
    job_name: str
    job_namespace: str
    log_url: str
    stopped: datetime.datetime

    def __bool__(self) -> bool:
        return self.success or False

    def __str__(self) -> str:
        if self.success:
            state = "SUCCESS"
        else:
            state = "FAILURE"
        return f"SmartJobExecutionResult(short_execution_id={self.short_execution_id}, job_name={self.job_name}, job_namespace={self.job_namespace}): {state} in {self.duration_seconds or -1} seconds"

    def asdict(self) -> dict:
        """Return this object as a dictionary."""
        return {
            "success": self.success,
            "created": self.created,
            "stopped": self.stopped,
            "execution_id": self.execution_id,
            "short_execution_id": self.short_execution_id,
            "job_name": self.job_name,
            "job_namespace": self.job_namespace,
            "duration_seconds": self.duration_seconds,
        }

    @property
    def duration_seconds(self) -> int | None:
        """The duration of the job in seconds."""
        return (self.stopped - self.created).seconds

    @classmethod
    def from_execution(
        cls, execution: SmartJobExecution, success: bool
    ) -> "SmartJobExecutionResult":
        """Create a SmartJobExecutionResult from a SmartJobExecution."""
        if not execution.log_url:
            raise SmartJobException("log_url is required")
        return cls(
            created=execution.created,
            execution_id=execution.id,
            short_execution_id=execution.short_id,
            job_name=execution.job.name,
            job_namespace=execution.job.namespace,
            log_url=execution.log_url,
            success=success,
            stopped=datetime.datetime.now(tz=datetime.timezone.utc),
        )
