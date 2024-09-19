import datetime
import json
import os
import shlex
from dataclasses import dataclass, field

from stlog import getLogger

from smartjob.app.exception import SmartJobException
from smartjob.app.input import Input
from smartjob.app.job import SmartJob
from smartjob.app.retry import RetryConfig
from smartjob.app.timeout import TimeoutConfig
from smartjob.app.utils import unique_id

DEFAULT_MACHINE_TYPE = "n1-standard-4"
DEFAULT_ACCELERATOR_COUNT = 0
DEFAULT_ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
DEFAULT_BOOT_DISK_TYPE = "pd-ssd"
DEFAULT_BOOT_DISK_SIZE_GB = 100
DEFAULT_CPU = 1.0
DEFAULT_MEMORY_GB = 0.5

logger = getLogger("smartjob.execution")


def read_from_env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    logger.debug("Read %s=%s from env", name, value)
    return value


@dataclass
class ExecutionConfig:
    """ExecutionConfig is the configuration for a job execution."""

    retry_config: RetryConfig | None = None
    """Retry configuration."""

    timeout_config: TimeoutConfig | None = None
    """Timeout configuration."""

    project: str | None = None
    """GCP project (only for cloudrun/vertex executor)."""

    region: str | None = None
    """GCP region (only for cloudrun/vertex executor)."""

    labels: dict[str, str] | None = None
    """GCP labels (only for cloudrun/vertex executor)."""

    staging_bucket: str | None = None
    """Staging bucket (for input, output and/or loading python_script_path, only for cloudrun/vertex executor)."""

    service_account: str | None = None
    """Service account (email) to use for the job execution (only for cloudrun/vertex executor)."""

    cpu: float | None = None
    """Number of requested CPUs (only for cloudrun executor)."""

    memory_gb: float | None = None
    """Memory in Gb (only for cloudrun executor)."""

    machine_type: str | None = None
    """Machine type (only for vertex executor)."""

    accelerator_type: str | None = None
    """Accelerator type (only for vertex executor)."""

    accelerator_count: int | None = None
    """Number of accelerators (only for vertex executor)."""

    boot_disk_type: str | None = None
    """Boot disk type (only for vertex executor)."""

    boot_disk_size_gb: int | None = None
    """Boot disk size in Gb (only for vertex executor)."""

    vpc_connector_network: str | None = None
    """VPC connector network (only for cloudrun executor)."""

    vpc_connector_subnetwork: str | None = None
    """VPC connector subnetwork (only for cloudrun executor)."""

    @property
    def _retry_config(self) -> RetryConfig:
        assert self.retry_config is not None
        return self.retry_config

    @property
    def _labels(self) -> dict[str, str]:
        assert self.labels is not None
        return self.labels

    @property
    def _timeout_config(self) -> TimeoutConfig:
        assert self.timeout_config is not None
        return self.timeout_config

    @property
    def _staging_bucket(self) -> str:
        if self.staging_bucket is None:
            return "dummy"
        return self.staging_bucket

    @property
    def _project(self) -> str:
        assert self.project is not None
        return self.project

    @property
    def _region(self) -> str:
        assert self.region is not None
        return self.region

    @property
    def _staging_bucket_name(self) -> str:
        if self.staging_bucket is None:
            return "dummy"
        if self.staging_bucket.startswith("gs://"):
            return self.staging_bucket[5:]
        return self.staging_bucket

    @property
    def _cpu(self) -> float:
        assert self.cpu is not None
        return self.cpu

    @property
    def _memory_gb(self) -> float:
        assert self.memory_gb is not None
        return self.memory_gb

    @property
    def _machine_type(self) -> str:
        assert self.machine_type is not None
        return self.machine_type

    @property
    def _accelerator_type(self) -> str:
        assert self.accelerator_type is not None
        return self.accelerator_type

    @property
    def _accelerator_count(self) -> int:
        assert self.accelerator_count is not None
        return self.accelerator_count

    @property
    def _boot_disk_type(self) -> str:
        assert self.boot_disk_type is not None
        return self.boot_disk_type

    @property
    def _boot_disk_size_gb(self) -> int:
        assert self.boot_disk_size_gb is not None
        return self.boot_disk_size_gb

    def fix_timeout_config(self) -> None:
        if self.timeout_config is None:
            self.timeout_config = TimeoutConfig()

    def fix_for_executor_name(self, executor_name: str) -> None:
        # Prechecks
        if executor_name == "cloudrun":
            for field_name in [
                "machine_type",
                "accelerator_type",
                "accelerator_count",
                "boot_disk_type",
                "boot_disk_size_gb",
            ]:
                if getattr(self, field_name) is not None:
                    logger.warning(f"{field_name} is ignored for cloudrun executor")
        elif executor_name == "vertex":
            for field_name in [
                "cpu",
                "memory_gb",
                "vpc_connector_network",
                "vpc_connector_subnetwork",
            ]:
                if getattr(self, field_name) is not None:
                    logger.warning(f"{field_name} is ignored for vertex executor")
        elif executor_name == "docker":
            for field_name in [
                "machine_type",
                "accelerator_type",
                "accelerator_count",
                "boot_disk_type",
                "boot_disk_size_gb",
                "cpu",
                "memory_gb",
                "labels",
                "vpc_connector_network",
                "vpc_connector_subnetwork",
            ]:
                if getattr(self, field_name) is not None:
                    logger.warning(f"{field_name} is ignored for docker executor")
            self.staging_bucket = "smartjob-staging"  # we force this special value
            if self.staging_bucket is not None:
                logger.warning(
                    "staging_bucket is not supported for docker executor => let's ignore it"
                )
        if executor_name in ("vertex", "docker"):
            if self.retry_config is not None:
                if self.retry_config._max_attempts_execute != 1:
                    logger.warning(
                        f"retry_config.max_attempts_execute if not supported for {executor_name} executor => let's change the setting to 1"
                    )
                    self.retry_config.max_attempts_execute = 1
            else:
                self.retry_config = RetryConfig(max_attempts_execute=1)
        # Default values
        self.fix_timeout_config()
        if self.retry_config is None:
            self.retry_config = RetryConfig()
        if self.staging_bucket is None:
            self.staging_bucket = read_from_env("SMARTJOB_STAGING_BUCKET")
        if self.service_account is None:
            self.service_account = read_from_env("SMARTJOB_SERVICE_ACCOUNT")
        if self.project is None:
            self.project = read_from_env("SMARTJOB_PROJECT")
        if self.region is None:
            self.region = read_from_env("SMARTJOB_REGION")
        if self.machine_type is None:
            self.machine_type = DEFAULT_MACHINE_TYPE
        if self.accelerator_count is None:
            self.accelerator_count = DEFAULT_ACCELERATOR_COUNT
        if self.accelerator_type is None:
            self.accelerator_type = DEFAULT_ACCELERATOR_TYPE
        if self.boot_disk_type is None:
            self.boot_disk_type = DEFAULT_BOOT_DISK_TYPE
        if self.boot_disk_size_gb is None:
            self.boot_disk_size_gb = DEFAULT_BOOT_DISK_SIZE_GB
        if self.cpu is None:
            self.cpu = DEFAULT_CPU
        if self.memory_gb is None:
            self.memory_gb = DEFAULT_MEMORY_GB
        if self.labels is None:
            self.labels = {}
        # Post checks
        if executor_name in ("cloudrun", "vertex"):
            for field_name in ["staging_bucket", "project", "region"]:
                if getattr(self, field_name) is None:
                    raise SmartJobException(
                        f"{field_name} is required for cloudrun executor"
                    )
            if not self.staging_bucket.startswith("gs://"):  # type: ignore
                raise SmartJobException("staging_bucket must start with gs://")


@dataclass
class Execution:
    job: SmartJob
    config: ExecutionConfig
    id: str = field(default_factory=unique_id)
    inputs: list[Input] = field(default_factory=list)
    created: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(tz=datetime.timezone.utc),
        init=False,
    )
    cancelled: bool = field(default=False, init=False)
    add_envs: dict[str, str] = field(default_factory=dict)
    overridden_args: list[str] = field(default_factory=list)

    @property
    def base_dir(self) -> str:
        dat = self.created.strftime("%Y-%m-%d")
        tim = self.created.strftime("%H%M")
        return f"{self.job.full_name}/{dat}/{tim}/{self.id}"

    @property
    def input_relative_path(self) -> str:
        return f"{self.base_dir}/input"

    @property
    def output_relative_path(self) -> str:
        return f"{self.base_dir}/output"

    @property
    def add_envs_as_string(self) -> str:
        return ", ".join([f"{x}={y}" for x, y in self.add_envs.items()])

    @property
    def overridden_args_as_string(self) -> str:
        return shlex.join(self.overridden_args)

    @property
    def labels(self) -> dict[str, str]:
        return self.config._labels | {
            "smartjob": "true",
            "smartjob.namespace": self.job.namespace,
        }


@dataclass
class ExecutionResult:
    """ExecutionResult is the (final) result of a job execution.

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
        json_output: if the job has created a json file named smartjob.json in the output directory, it will be stored/decoded here.

    """

    success: bool
    created: datetime.datetime
    stopped: datetime.datetime
    execution_id: str
    job_name: str
    job_namespace: str
    log_url: str
    json_output: dict | list | str | float | int | bool | None = None

    def __bool__(self) -> bool:
        return self.success or False

    def __str__(self) -> str:
        if self.success:
            state = "SUCCESS"
        else:
            state = "FAILURE"
        if self.json_output is not None:
            json_output = json.dumps(self.json_output, indent=4)
        else:
            json_output = "None"
        res = f"""ExecutionResult(
    job_name={self.job_name}, job_namespace={self.job_namespace},
    execution_id={self.execution_id},
    state={state}, duration_seconds={self.duration_seconds},
    json_output={json_output}
)"""
        return res

    @property
    def duration_seconds(self) -> int:
        """The duration of the job in seconds."""
        return (self.stopped - self.created).seconds

    @classmethod
    def _from_execution(
        cls,
        execution: Execution,
        success: bool,
        log_url: str,
    ) -> "ExecutionResult":
        """Create a ExecutionResult from a SmartJobExecution."""
        return cls(
            created=execution.created,
            execution_id=execution.id,
            job_name=execution.job.name,
            job_namespace=execution.job.namespace,
            success=success,
            stopped=datetime.datetime.now(tz=datetime.timezone.utc),
            log_url=log_url,
        )
