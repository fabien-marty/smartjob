import sys

import typer
from stlog import setup

from smartjob.app.executor import SmartJobExecutorService
from smartjob.app.job import SmartJob
from smartjob.infra.controllers.lib import get_smart_job_executor_service_singleton


def get_job_service(ctx: typer.Context, **kwargs) -> SmartJobExecutorService:
    return get_smart_job_executor_service_singleton(
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


def add_env_argument_to_dict(add_env: list[str]) -> dict[str, str]:
    add_envs: dict[str, str] = {}
    for ae in add_env:
        if "=" not in ae:
            raise ValueError(f"Invalid env var format: {ae} => must be key=value")
        key, value = ae.split("=", 1)
        add_envs[key] = value
    return add_envs


def cli_process(service: SmartJobExecutorService, job: SmartJob, wait: bool):
    if wait:
        result = service.sync_run(job)
        return_code = 0
        if result:
            print("SUCCESS in %i seconds" % (result.duration_seconds or -1))
        else:
            print("FAILED in %i seconds" % (result.duration_seconds or -1))
            return_code = 2
        print("Id:      %s" % result.execution_id)
        print("Logs:    %s" % result.log_url)
        sys.exit(return_code)
    else:
        future = service.sync_schedule(job)
        print("SCHEDULED")
        print("Id:      %s" % future.execution_id)
        print("Logs:    %s" % future.log_url)
