import concurrent
import time
from dataclasses import dataclass, field

from google.cloud import batch_v1
from google.protobuf.duration_pb2 import Duration
from stlog import LogContext, getLogger

from smartjob.app.execution import Execution
from smartjob.app.executor import ExecutorPort, SchedulingDetails, _ExecutionResult

logger = getLogger("smartjob.executor.cloudbatch")


@dataclass
class CloudBatchExecutorAdapter(ExecutorPort):
    _client: batch_v1.BatchServiceClient | None = None
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

    @property
    def client(self) -> batch_v1.BatchServiceClient:
        if self._client is None:
            self._client = batch_v1.BatchServiceClient()
        return self._client

    def parent_name(self, execution: Execution) -> str:
        conf = execution.config
        return f"projects/{conf._project}/locations/{conf._region}"

    def get_log_url(self, execution: Execution, job_id: str) -> str:
        return f"https://console.cloud.google.com/batch/jobsDetail/regions/{execution.config._region}/jobs/{job_id}/logs?project={execution.config._project}"

    def _clean(self, string: str) -> str:
        tmp = string.lower()
        tmp = "".join([c if (c.isalnum() or c in ("-", "_")) else "" for c in tmp])
        if tmp and tmp[0] in ("-", "_"):
            raise Exception("namespace cannot start with '-' or '_'")
        if tmp and tmp[-1] in ("-", "_"):
            raise Exception("namespace cannot end with '-' or '_'")
        return tmp

    def schedule(
        self, execution: Execution, forget: bool
    ) -> tuple[SchedulingDetails, concurrent.futures.Future[_ExecutionResult] | None]:
        network_policy = None
        if (
            execution.config.vpc_connector_network
            and execution.config.vpc_connector_subnetwork
        ):
            network_policy = batch_v1.AllocationPolicy.NetworkPolicy(
                network_interfaces=[
                    batch_v1.AllocationPolicy.NetworkInterface(
                        network=execution.config.vpc_connector_network,
                        subnetwork=execution.config.vpc_connector_subnetwork,
                    )
                ]
            )
        volumes: list[batch_v1.Volume] = []
        volumes.append(
            batch_v1.Volume(
                gcs=batch_v1.GCS(
                    remote_path=execution.config._staging_bucket_name,
                ),
                mount_path="/mnt/disks/staging",
            )
        )
        logger.info(
            "Let's trigger a new execution of Cloud Batch Job...",
            docker_image=execution.job.docker_image,
            overridden_args=execution.overridden_args_as_string,
            add_envs=execution.add_envs_as_string,
            timeout_s=execution.config._timeout_config.timeout_seconds,
        )
        job_id = f"{self._clean(execution.job.namespace)}-{self._clean(execution.job.name)}-{execution.id}"
        if len(job_id) > 63:
            job_id = job_id[:63]
        job = self.client.create_job(
            job_id=job_id,
            parent=self.parent_name(execution),
            job=batch_v1.Job(
                allocation_policy=batch_v1.AllocationPolicy(
                    location=batch_v1.AllocationPolicy.LocationPolicy(
                        allowed_locations=[f"regions/{execution.config._region}"],
                    ),
                    service_account=batch_v1.ServiceAccount(
                        email=execution.config.service_account,
                    ),
                    labels={
                        key.replace(".", "_"): value.replace(".", "_")
                        for key, value in execution.labels.items()
                    },
                    network=network_policy,
                    instances=[
                        batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
                            policy=batch_v1.AllocationPolicy.InstancePolicy(
                                machine_type=execution.config._machine_type,
                            ),
                        )
                    ],
                ),
                task_groups=[
                    batch_v1.TaskGroup(
                        task_spec=batch_v1.TaskSpec(
                            runnables=[
                                batch_v1.Runnable(
                                    container=batch_v1.Runnable.Container(
                                        image_uri=execution.job.docker_image,
                                        commands=execution.overridden_args,
                                        volumes=["/mnt/disks/staging:/staging"],
                                    ),
                                    environment=batch_v1.Environment(
                                        variables=execution.add_envs
                                    ),
                                )
                            ],
                            volumes=volumes,
                            # Add timeout configuration
                            max_run_duration=Duration(
                                seconds=execution.config._timeout_config.timeout_seconds
                            ),
                            max_retry_count=execution.config._retry_config._max_attempts_execute
                            - 1,
                        )
                    )
                ],
                logs_policy=batch_v1.LogsPolicy(
                    destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING,
                    cloud_logging_option=batch_v1.LogsPolicy.CloudLoggingOption(
                        use_generic_task_monitored_resource=False
                    ),
                ),
                labels={
                    key.replace(".", "_"): value.replace(".", "_")
                    for key, value in execution.labels.items()
                },
            ),
        )

        scheduling_details = SchedulingDetails(
            execution_id=execution.id,
            log_url=self.get_log_url(execution, job_id),
        )

        future = self.executor.submit(
            self.wait, execution, job.name, job_id, LogContext.getall()
        )

        return scheduling_details, future

    def wait(
        self, execution: Execution, job_name: str, job_id: str, log_context: dict
    ) -> _ExecutionResult:
        """Note: executed in another thread."""
        with LogContext.bind(**log_context):
            before = time.perf_counter()
            job: batch_v1.Job | None = None
            while (
                time.perf_counter() - before
                < execution.config._timeout_config.timeout_seconds + 9
            ):
                time.sleep(5)
                if job is None and time.perf_counter() - before > 30:
                    logger.warning("Can't find the job after 30 seconds => giving up")
                    return _ExecutionResult._from_execution(
                        execution, False, self.get_log_url(execution, job_id)
                    )
                try:
                    job = self.client.get_job(name=job_name, timeout=10)
                except Exception:
                    logger.warning("Job not found, retrying...", exc_info=True)
                    continue
                if job.status.state == batch_v1.JobStatus.State.SUCCEEDED:
                    return _ExecutionResult._from_execution(
                        execution, True, self.get_log_url(execution, job_id)
                    )
                elif job.status.state in (
                    batch_v1.JobStatus.State.FAILED,
                    batch_v1.JobStatus.State.CANCELLED,
                ):
                    return _ExecutionResult._from_execution(
                        execution, False, self.get_log_url(execution, job_id)
                    )
        return _ExecutionResult._from_execution(
            execution, True, self.get_log_url(execution, job_id)
        )

    def get_name(self) -> str:
        return "cloudbatch"

    def staging_mount_path(self, execution: Execution) -> str:
        return "/staging"
