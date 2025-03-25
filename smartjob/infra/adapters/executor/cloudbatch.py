import concurrent
from dataclasses import dataclass, field

from google.cloud import batch_v1
from google.protobuf.duration_pb2 import Duration
from stlog import getLogger

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
                name="staging",
                gcs=batch_v1.GCS(
                    bucket=execution.config._staging_bucket_name,
                ),
                mount_path="/staging",
            )
        )
        logger.info(
            "Let's trigger a new execution of Cloud Batch Job...",
            docker_image=execution.job.docker_image,
            overridden_args=execution.overridden_args_as_string,
            add_envs=execution.add_envs_as_string,
            timeout_s=execution.config._timeout_config.timeout_seconds,
        )
        job = self.client.create_job(
            parent=self.parent_name(execution),
            job=batch_v1.Job(
                allocation_policy=batch_v1.AllocationPolicy(
                    location=execution.config._region,
                    service_account=execution.config.service_account,
                    labels=execution.labels,
                    network=network_policy,
                    instances=[
                        batch_v1.AllocationPolicy.InstancePolicy(
                            machine_type=execution.config._machine_type,
                        )
                    ],
                ),
                task_groups=[
                    batch_v1.TaskGroup(
                        task_spec=batch_v1.TaskSpec(
                            runnables=[
                                batch_v1.Runnable(
                                    container=batch_v1.Runnable.Container(
                                        image=execution.job.docker_image,
                                        commands=execution.overridden_args,
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
                labels=execution.labels,
            ),
        )

        scheduling_details = SchedulingDetails(
            execution_id=execution.id,
            log_url=f"https://console.cloud.google.com/batch/jobs/{job.name}?project={execution.config._project}",
        )

        return scheduling_details, None

    def get_name(self) -> str:
        return "cloudbatch"

    def staging_mount_path(self, execution: Execution) -> str:
        return "/staging"
