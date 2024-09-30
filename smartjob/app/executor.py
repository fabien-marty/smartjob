import concurrent.futures
import datetime
import json
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from stlog import LogContext, getLogger
from tenacity import (
    RetryError,
    Retrying,
    stop_after_attempt,
    wait_exponential,
)

from smartjob.app.execution import (
    Execution,
    ExecutionConfig,
)
from smartjob.app.input import Input
from smartjob.app.job import SmartJob
from smartjob.app.storage import StorageService

logger = getLogger("smartjob.executor")


@dataclass
class _ExecutionResult:
    """_ExecutionResult is an internal object representing the result of a job execution.

    It does not contain the json_output attribute.
    """

    success: bool
    created: datetime.datetime
    stopped: datetime.datetime
    execution_id: str
    job_name: str
    job_namespace: str
    log_url: str

    def __bool__(self) -> bool:
        return self.success or False

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
    ) -> "_ExecutionResult":
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


@dataclass
class ExecutionResult(_ExecutionResult):
    """ExecutionResult is the (final) result of a job execution.

    Attributes:
        success: Whether the job has succeeded or not.
        created: The datetime when the job has started.
        stopped: The datetime when the job has stopped.
        execution_id: The execution id of the job.
        job_name: The name of the job.
        job_namespace: The namespace of the job.
        log_url: The execution log url.
        json_output: if the job has created a json file named smartjob.json in the output directory, it will be stored/decoded here.

    """

    json_output: dict | list | str | float | int | bool | None = None

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

    @classmethod
    def _from_execution_result(
        cls,
        execution_result: _ExecutionResult,
        json_output: dict | list | str | float | int | bool | None,
    ) -> "ExecutionResult":
        return cls(
            success=execution_result.success,
            created=execution_result.created,
            stopped=execution_result.stopped,
            execution_id=execution_result.execution_id,
            job_name=execution_result.job_name,
            job_namespace=execution_result.job_namespace,
            log_url=execution_result.log_url,
            json_output=json_output,
        )


@dataclass
class SchedulingDetails:
    """SchedulingDetails holds some data/details about the scheduling of a job.

    Attributes:
        created: The datetime when the job has started.
        execution_id: The execution id of the job.
        job_name: The name of the job.
        job_namespace: The namespace of the job.
        log_url: The execution log url.

    """

    scheduled_date: datetime.datetime = field(
        init=False,
        default_factory=lambda: datetime.datetime.now(tz=datetime.timezone.utc),
    )
    execution_id: str
    log_url: str


class ExecutionResultFuture(concurrent.futures.Future[ExecutionResult]):
    def __init__(
        self,
        log_context: dict,
        storage_service: StorageService,
        execution: Execution,
        future: concurrent.futures.Future[_ExecutionResult],
    ):
        super().__init__()
        self.set_running_or_notify_cancel()
        self._log_context = log_context
        self._storage_service = storage_service
        self._execution = execution
        future.add_done_callback(self._first_future_done)
        self._json_output: dict | list | str | int | float | bool | None = None

    def _get_output(self) -> dict | list | str | int | float | bool | None:
        """Warning: executed in another thread."""
        staging_bucket_name = self._execution.config._staging_bucket_name
        staging_bucket = self._execution.config._staging_bucket
        smartjob_path = f"{self._execution.output_relative_path}/smartjob.json"
        logger.info(
            "Downloading smartjob.json from output (if exists)...",
            path=f"{staging_bucket}/{smartjob_path}",
        )
        raw = self._storage_service.download(staging_bucket_name, smartjob_path)
        if raw == b"":
            logger.debug("No smartjob.json found in output")
            return None
        try:
            res = json.loads(raw)
            logger.debug("smartjob.json downloaded/decoded")
            return res
        except Exception:
            logger.warning("smartjob.json is not a valid json")
            return None

    def _get_output_and_set_result(
        self, future: concurrent.futures.Future[ExecutionResult]
    ):
        """Warning: executed in another thread."""
        with LogContext.bind(**self._log_context):
            try:
                result = future.result()
                if result.success:
                    self._json_output = self._get_output()
                self.set_result(
                    ExecutionResult._from_execution_result(result, self._json_output)
                )
            except Exception as e:
                self.set_exception(e)

    def _first_future_done(self, future: concurrent.futures.Future):
        try:
            t = threading.Thread(
                target=self._get_output_and_set_result, args=(future,), daemon=True
            )
            t.start()
        except Exception as e:
            self.set_exception(e)


class ExecutorPort(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def schedule(
        self, execution: Execution, forget: bool
    ) -> tuple[SchedulingDetails, concurrent.futures.Future[_ExecutionResult] | None]:
        pass

    @abstractmethod
    def staging_mount_path(self, execution: Execution) -> str:
        pass


@dataclass
class ExecutorService:
    adapter: ExecutorPort
    storage_service: StorageService
    executor_name: str = field(default="", init=False)

    def __post_init__(self):
        self.executor_name = self.adapter.get_name()

    def _get_input_path(self, execution: Execution) -> str:
        return f"{self.adapter.staging_mount_path(execution)}/{execution.input_relative_path}"

    def _get_output_path(self, execution: Execution) -> str:
        return f"{self.adapter.staging_mount_path(execution)}/{execution.output_relative_path}"

    def _update_execution_env(self, execution: Execution):
        execution.add_envs["INPUT_PATH"] = self._get_input_path(execution)
        execution.add_envs["OUTPUT_PATH"] = self._get_output_path(execution)
        execution.add_envs["EXECUTION_ID"] = execution.id

    def _upload_python_script_if_needed_and_update_overridden_args(
        self, execution: Execution
    ):
        job = execution.job
        if not job.python_script_path:
            return
        with open(job.python_script_path) as f:
            content = f.read()
        destination_path = f"{execution.base_dir}/input/script.py"
        logger.info(
            "Uploading python script (%s) to %s/%s...",
            job.python_script_path,
            execution.config._staging_bucket,
            destination_path,
        )
        self.storage_service.upload(
            content.encode("utf8"),
            execution.config._staging_bucket_name,
            destination_path,
        )
        logger.debug(
            "Done uploading python script (%s) to %s/%s",
            job.python_script_path,
            execution.config.staging_bucket,
            destination_path,
        )
        execution.overridden_args = [
            "python",
            f"{self.adapter.staging_mount_path(execution)}/{destination_path}",
        ]

    def _replace_overridden_args_placeholders(self, execution: Execution):
        input_path = (
            f"{self.adapter.staging_mount_path(execution)}/{execution.base_dir}/input"
        )
        execution.overridden_args = [
            x.replace("{{INPUT}}", input_path) for x in execution.overridden_args
        ]

    def _upload_input(self, execution: Execution, input: Input):
        path = f"{execution.config._staging_bucket}/{execution.input_relative_path}/{input.filename}"
        logger.info(f"Uploading input to {path}...")
        input._create(
            execution.config._staging_bucket_name,
            execution.input_relative_path,
            self.storage_service,
        )
        logger.debug(f"Done uploading input: {path}")

    def _upload_inputs(self, execution: Execution):
        inputs = execution.inputs
        for input in inputs:
            self._upload_input(execution, input)

    def _create_input_output_paths_if_needed(self, execution: Execution):
        logger.info(
            "Creating input path %s/%s/...",
            execution.config._staging_bucket,
            execution.input_relative_path,
        )
        self.storage_service.upload(
            b"",
            execution.config._staging_bucket_name,
            execution.input_relative_path + "/",
        )
        logger.info(
            "Creating output path %s/%s/...",
            execution.config._staging_bucket,
            execution.output_relative_path,
        )
        self.storage_service.upload(
            b"",
            execution.config._staging_bucket_name,
            execution.output_relative_path + "/",
        )
        logger.debug("Done creating input/output paths")

    def _prepare(self, execution: Execution):
        self._update_execution_env(execution)
        self._create_input_output_paths_if_needed(execution)
        self._upload_python_script_if_needed_and_update_overridden_args(execution)
        self._replace_overridden_args_placeholders(execution)
        self._upload_inputs(execution)

    def _schedule(
        self,
        job: SmartJob,
        execution_config: ExecutionConfig,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        forget: bool = False,
    ) -> tuple[SchedulingDetails, concurrent.futures.Future[ExecutionResult] | None]:
        execution = Execution(
            job,
            overridden_args=list(job.overridden_args),
            add_envs={**job.add_envs, **(add_envs or {})},
            config=execution_config,
            inputs=inputs or [],
        )
        with LogContext.bind(execution_id=execution.id):
            logger.info("Preparing the smartjob execution...")
            self._prepare(execution)
            logger.debug("Smartjob execution prepared")
            logger.info("Scheduling the smartjob execution...")
            scheduling_result, future_or_none = self.adapter.schedule(execution, forget)
            logger.debug("Smartjob execution scheduled")
            if future_or_none is None:
                return scheduling_result, None
            return scheduling_result, ExecutionResultFuture(
                log_context=LogContext.getall(),
                storage_service=self.storage_service,
                execution=execution,
                future=future_or_none,
            )

    def schedule(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        execution_config: ExecutionConfig | None = None,
        forget: bool = False,
    ) -> tuple[SchedulingDetails, concurrent.futures.Future[ExecutionResult] | None]:
        """Schedule a job.

        This method returns when the job is scheduled (not when it is finished!).

        If we can't schedule the job an exception is raised.

        If forget=False (default), we return a first object about scheduling details
        and a second object about the future result of the job execution.

        If forget=True, we return only the first object about scheduling details
        (and None for the second one).

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            execution_config: Execution configuration.
            forget: if True, don't return a future on ExecutionResult (but None).

        Returns:
            The result of the job scheduling and (optionally) a future object about the result of the job execution.

        """
        execution_config = execution_config or ExecutionConfig()
        execution_config.fix_for_executor_name(self.executor_name)
        with LogContext.bind(
            job_name=job.name,
            job_namespace=job.namespace,
        ):
            try:
                for attempt in Retrying(
                    reraise=True,
                    wait=wait_exponential(multiplier=1, min=1, max=300),
                    stop=stop_after_attempt(
                        execution_config._retry_config._max_attempts_schedule
                    ),
                ):
                    with attempt:
                        with LogContext.bind(
                            attempt=attempt.retry_state.attempt_number
                        ):
                            schedule_result, future_or_none = self._schedule(
                                job,
                                execution_config=execution_config,
                                add_envs=add_envs,
                                inputs=inputs,
                                forget=forget,
                            )
                            logger.info(
                                "Smartjob execution scheduled",
                                log_url=schedule_result.log_url,
                            )
                            LogContext.remove("attempt")
                            LogContext.add(execution_id=schedule_result.execution_id)
                            return schedule_result, future_or_none
            except RetryError:
                raise
            raise Exception("Unreachable code")

    def run(
        self,
        job: SmartJob,
        add_envs: dict[str, str] | None = None,
        inputs: list[Input] | None = None,
        execution_config: ExecutionConfig | None = None,
    ) -> ExecutionResult:
        """Schedule a job and wait for its completion.

        Arguments:
            job: The job to run.
            add_envs: Environment variables to add for this particular execution.
            inputs: Inputs to add for this particular execution.
            execution_config: Execution configuration.

        Returns:
            The result of the job execution.

        """
        execution_config = execution_config or ExecutionConfig()
        execution_config.fix_timeout_config()
        _, execution_result = self.schedule(
            job,
            add_envs=add_envs,
            inputs=inputs,
            execution_config=execution_config,
            forget=False,
        )
        assert execution_result is not None  # Can't be None because forget=False
        result = execution_result.result()  # FIXME: timeout
        return result
