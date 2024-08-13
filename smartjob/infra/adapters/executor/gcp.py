import asyncio
import json
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Coroutine

from stlog import getLogger

from smartjob.app.execution import Execution
from smartjob.app.executor import ExecutionResultFuture, ExecutorPort
from smartjob.app.input import Input
from smartjob.app.storage import StorageService
from smartjob.infra.adapters.storage.gcs import GcsStorageAdapter

logger = getLogger("smartjob.executor.gcp")


class GCPExecutionResultFuture(ExecutionResultFuture):
    def __init__(self, *args, **kwargs):
        self._gcs_output_path = kwargs.pop("gcs_output_path")
        super().__init__(*args, **kwargs)

    async def _get_output(self) -> dict | list | str | int | float | bool | None:
        logger.info("Downloading smartjob.json from output (if exists)...")
        raw = await self._storage_service.download(
            self._execution.config._staging_bucket_name,
            f"{self._gcs_output_path}/smartjob.json",
        )
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


@dataclass
class GCPExecutor(ExecutorPort):
    """Abstract base class for GCP executors."""

    max_workers: int = 10
    _storage_service: StorageService | None = field(default=None, init=False)

    def __post_init__(self):
        self._storage_service = StorageService(
            adapter=GcsStorageAdapter(max_workers=self.max_workers)
        )

    @property
    def storage_service(self) -> StorageService:
        assert self._storage_service is not None
        return self._storage_service

    @abstractmethod
    def staging_mount_path(self, execution: Execution) -> str:
        pass

    def get_input_path(self, execution: Execution) -> str:
        return f"{self.staging_mount_path(execution)}/{execution.input_relative_path}"

    def get_output_path(self, execution: Execution) -> str:
        return f"{self.staging_mount_path(execution)}/{execution.output_relative_path}"

    def update_execution_env(self, execution: Execution):
        execution.add_envs["INPUT_PATH"] = self.get_input_path(execution)
        execution.add_envs["OUTPUT_PATH"] = self.get_output_path(execution)
        execution.add_envs["EXECUTION_ID"] = execution.id

    async def create_input_output_paths_if_needed(self, execution: Execution):
        coroutines: list[Coroutine] = []
        logger.info(
            "Creating input path gs://%s/%s/...",
            execution.config._staging_bucket_name,
            execution.input_relative_path,
        )
        coroutines.append(
            self.storage_service.upload(
                b"",
                execution.config._staging_bucket_name,
                execution.input_relative_path + "/",
            )
        )
        logger.info(
            "Creating output path gs://%s/%s/...",
            execution.config._staging_bucket_name,
            execution.output_relative_path,
        )
        coroutines.append(
            self.storage_service.upload(
                b"",
                execution.config._staging_bucket_name,
                execution.output_relative_path + "/",
            )
        )
        await asyncio.gather(*coroutines, return_exceptions=True)
        logger.debug("Done creating input/output paths")

    async def upload_python_script_if_needed_and_update_overridden_args(
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
        await self.storage_service.upload(
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
            f"{self.staging_mount_path(execution)}/{destination_path}",
        ]

    async def upload_inputs(self, execution: Execution):
        async def _upload_input(input: Input):
            path = f"gs://{execution.config._staging_bucket_name}/{execution.input_relative_path}/{input.filename}"
            logger.info(f"Uploading input to {path}...")
            await input._create(
                execution.config._staging_bucket_name,
                execution.input_relative_path,
                self.storage_service,
            )
            logger.debug(f"Done uploading input: {path}")

        inputs = execution.inputs
        await asyncio.gather(*[_upload_input(input) for input in inputs])

    async def prepare(self, execution: Execution):
        self.update_execution_env(execution)
        await self.create_input_output_paths_if_needed(execution)
        await self.upload_python_script_if_needed_and_update_overridden_args(execution)
        await self.upload_inputs(execution)
