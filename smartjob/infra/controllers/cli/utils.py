import asyncio
import sys

import typer
from stlog import setup

from smartjob.app.executor import SmartJobExecutionResultFuture, SmartJobExecutorService
from smartjob.app.job import SmartJob, SmartJobExecutionResult
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
OverrideEnvArgument = typer.Option(
    [],
    help="Override docker image env vars (key=value, can be used multiple times)",
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


async def _run(
    service: SmartJobExecutorService, job: SmartJob
) -> SmartJobExecutionResult:
    return await service.run(job)


async def _schedule(
    service: SmartJobExecutorService, job: SmartJob
) -> SmartJobExecutionResultFuture:
    return await service.schedule(job)


def cli_process(service: SmartJobExecutorService, job: SmartJob, wait: bool):
    if wait:
        result = asyncio.run(_run(service, job))
        return_code = 0
        if result:
            print("SUCCESS in %i seconds" % (result.duration_seconds or -1))
        else:
            print("FAILED in %i seconds" % (result.duration_seconds or -1))
            return_code = 2
        print("Id:      %s" % result.job.execution_id)
        print("ShortId: %s" % result.job.short_execution_id)
        print("Logs:    %s" % result.log_url)
        sys.exit(return_code)
    else:
        future = asyncio.run(_schedule(service, job))
        print("SCHEDULED")
        print("Id:      %s" % future.job.execution_id)
        print("ShortId: %s" % future.job.short_execution_id)
        print("Logs:    %s" % future.log_url)
