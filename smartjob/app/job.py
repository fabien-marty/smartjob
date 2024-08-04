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
        if not self.staging_bucket:
            raise SmartJobException("staging_bucket is required")
        if not self.staging_bucket.startswith("gs://"):
            raise SmartJobException("staging_bucket must start with gs://")
        if "/" in self.staging_bucket[5:]:
            raise SmartJobException(
                "staging_bucket name must not contain / (except for the the gs:// prefix)"
            )

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
    def staging_mount_point(self) -> str:
        """Return the mount point for the staging bucket."""
        raise NotImplementedError("mount_point must be implemented in subclasses")

    @property
    def full_name(self) -> str:
        """Return the full name of the job (namespace + name)."""
        return f"{self.namespace}-{self.name}"


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
    def base_dir(self) -> str:
        dat = self.created.strftime("%Y-%m-%d")
        tim = self.created.strftime("%H%M")
        return f"{self.job.full_name}/{dat}/{tim}/{self.id}"

    @property
    def _input_path(self) -> str:
        return f"{self.base_dir}/input"

    @property
    def _output_path(self) -> str:
        return f"{self.base_dir}/output"

    @property
    def input_path(self) -> str:
        return f"{self.job.staging_mount_point}/{self._input_path}"

    @property
    def output_path(self) -> str:
        return f"{self.job.staging_mount_point}/{self._output_path}"

    @property
    def full_input_path(self) -> str:
        return f"{self.job.staging_bucket}/{self._input_path}"

    @property
    def full_output_path(self) -> str:
        return f"{self.job.staging_bucket}/{self._output_path}"


@dataclass
class SmartJobExecutionResult:
    """SmartJobExecutionResult is the (final) result of a job execution.

    Attributes:
        success: Whether the job has succeeded or not.
        created: The datetime when the job has started.
        stopped: The datetime when the job has stopped.
        execution_id: The execution id of the job.
        job_name: The name of the job.
        job_namespace: The namespace of the job.
        log_url: The execution log url.
        full_input_path: The full input path (starting with gs://).
        full_output_path: The full output path (starting with gs//).

    """

    success: bool
    created: datetime.datetime
    stopped: datetime.datetime
    execution_id: str
    job_name: str
    job_namespace: str
    log_url: str
    full_input_path: str
    full_output_path: str

    def __bool__(self) -> bool:
        return self.success or False

    def __str__(self) -> str:
        if self.success:
            state = "SUCCESS"
        else:
            state = "FAILURE"
        return f"""SmartJobExecutionResult(
    job_name={self.job_name}, job_namespace={self.job_namespace},
    execution_id={self.execution_id},
    state={state}, duration_seconds={self.duration_seconds},
    log_url={self.log_url},
)"""

    def asdict(self) -> dict:
        """Return this object as a dictionary."""
        return {
            "success": self.success,
            "created": self.created,
            "stopped": self.stopped,
            "execution_id": self.execution_id,
            "job_name": self.job_name,
            "job_namespace": self.job_namespace,
            "duration_seconds": self.duration_seconds,
            "full_input_path": self.full_input_path,
            "full_output_path": self.full_output_path,
        }

    @property
    def duration_seconds(self) -> int:
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
            job_name=execution.job.name,
            job_namespace=execution.job.namespace,
            log_url=execution.log_url,
            success=success,
            stopped=datetime.datetime.now(tz=datetime.timezone.utc),
            full_input_path=execution.full_input_path,
            full_output_path=execution.full_output_path,
        )
