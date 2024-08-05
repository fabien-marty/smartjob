import asyncio
import json
import sys

import typer
from stlog import setup

from smartjob.app.executor import ExecutorService
from smartjob.app.input import GcsInput, Input, LocalPathInput
from smartjob.app.job import SmartJob
from smartjob.infra.controllers.lib import get_executor_service_singleton


def get_job_service(ctx: typer.Context, **kwargs) -> ExecutorService:
    return get_executor_service_singleton(
        namespace=ctx.obj.namespace,
        project=ctx.obj.project,
        region=ctx.obj.region,
        **kwargs,
    )


def init_stlog(ctx: typer.Context):
    setup(level=ctx.obj.log_level)


NameArgument = typer.Argument(help="Name of the job")
DockerImageArgument = typer.Option("", help="Docker image to use")
OverrideCommandArgument = typer.Option(
    "",
    help="Override docker image command and arguments",
)
AddEnvArgument = typer.Option(
    [],
    help="Add a new env var in the container environment (format: key=value, can be used multiple times)",
)
StagingBucketArgument = typer.Option(
    None,
    envvar="SMARTJOB_STAGING_BUCKET",
    help="staging bucket (starting with gs://) for loading python_script_path",
)
InputBucketBasePathArgument = typer.Option(
    None, envvar="SMARTJOB_INPUT_BUCKET_BASE_PATH"
)
OutputBucketBasePathArgument: str = typer.Option(
    None, envvar="SMARTJOB_OUTPUT_BUCKET_BASE_PATH"
)
InputBucketPathArgument = typer.Option("")
OutputBucketPathArgument = typer.Option("")
PythonScriptPathArgument = typer.Option(
    "", help="local path to python script to execute in the container"
)
WaitArgument = typer.Option(True, help="Wait for the job to finish")
LocalPathInputArgument = typer.Option(
    [],
    help="Local path input (format: filename=local_path, can be used multiple times)",
)
GcsPathInputArguments = typer.Option(
    [],
    help="GCS path input (format: filename=full_source_gcs_path, can be used multiple times)",
)


def add_env_argument_to_dict(add_env: list[str]) -> dict[str, str]:
    add_envs: dict[str, str] = {}
    for ae in add_env:
        if "=" not in ae:
            raise ValueError(f"Invalid env var format: {ae} => must be key=value")
        key, value = ae.split("=", 1)
        add_envs[key] = value
    return add_envs


async def fire_and_forget_job(
    service: ExecutorService, job: SmartJob, inputs: list[Input]
):
    future = await service.schedule(job, inputs=inputs)
    print("SCHEDULED")
    print("Id:      %s" % future.execution_id)
    print("Logs:    %s" % future.log_url)


def cli_process(
    service: ExecutorService, job: SmartJob, wait: bool, inputs: list[Input]
):
    if wait:
        result = service.sync_run(job, inputs=inputs)
        return_code = 0
        if result:
            print("SUCCESS in %i seconds" % (result.duration_seconds or -1))
        else:
            print("FAILED in %i seconds" % (result.duration_seconds or -1))
            return_code = 2
        print("Id:      %s" % result.execution_id)
        print("Logs:    %s" % result.log_url)
        if result.json_output is not None:
            print("JsonOutput:")
            print(json.dumps(result.json_output, indent=4))
        sys.exit(return_code)
    else:
        asyncio.run(fire_and_forget_job(service, job, inputs))


def local_path_input_to_list(local_path_input: list[str]) -> list[Input]:
    res: list[Input] = []
    for lpi in local_path_input:
        if "=" not in lpi:
            raise ValueError(
                f"Invalid input format: {lpi} => must be filename=local_path"
            )
        filename, local_path = lpi.split("=", 1)
        res.append(
            LocalPathInput(
                filename=filename,
                local_path=local_path,
            )
        )
    return res


def gcs_input_to_list(gcs_input: list[str]) -> list[Input]:
    res: list[Input] = []
    for lpi in gcs_input:
        if "=" not in lpi:
            raise ValueError(
                f"Invalid input format: {lpi} => must be filename=full_gcs_path"
            )
        filename, gcs_path = lpi.split("=", 1)
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {gcs_path} => must start with gs://")
        if "/" not in gcs_path[5:]:
            raise ValueError(
                f"Invalid GCS path: {gcs_path} => must have a / after gs://"
            )
        res.append(
            GcsInput(
                filename=filename,
                gcs_path=gcs_path,
            )
        )
    return res
