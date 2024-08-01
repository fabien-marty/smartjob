import asyncio
import shlex
import sys

import typer

from smartjob.app.executor import SmartJobExecutorService
from smartjob.app.job import CloudRunSmartJob, SmartJobExecutionResult
from smartjob.infra.controllers.cli.utils import get_job_service, init_stlog

cli = typer.Typer()


async def _run(
    service: SmartJobExecutorService, job: CloudRunSmartJob
) -> SmartJobExecutionResult:
    return await service.run(job)


@cli.command()
def run(
    ctx: typer.Context,
    name: str = typer.Argument(help="Name of the job"),
    docker_image: str = typer.Option("", help="Docker image to use"),
    override_command_and_args: str = typer.Option(
        "",
        help="Override docker image command and arguments",
    ),
    override_env: list[str] = typer.Option(
        [],
        help="Override docker image env vars (key=value, can be used multiple times)",
    ),
    staging_bucket: str = typer.Option(
        None,
        envvar="SMARTJOB_STAGING_BUCKET",
        help="staging bucket (starting with gs://) for loading python_script_path",
    ),
    input_bucket_base_path: str = typer.Option(
        None, envvar="SMARTJOB_INPUT_BUCKET_BASE_PATH"
    ),
    output_bucket_base_path: str = typer.Option(
        None, envvar="SMARTJOB_OUTPUT_BUCKET_BASE_PATH"
    ),
    input_bucket_path: str = typer.Option(""),
    output_bucket_path: str = typer.Option(""),
    python_script_path: str = typer.Option(
        "", help="local path to python script to execute in the container"
    ),
    cpu: float = typer.Option(1.0, help="Number of CPUs"),
    memory_gb: float = typer.Option(0.5, help="Memory in Gb"),
):
    init_stlog(ctx)
    overriden_args = shlex.split(override_command_and_args)
    overriden_envs = {
        x.split("=")[0].strip().upper(): x.split("=")[1].strip() for x in override_env
    }
    service = get_job_service(
        ctx,
        input_bucket_base_path=input_bucket_base_path,
        output_bucket_base_path=output_bucket_base_path,
    )
    job = CloudRunSmartJob(
        name=name,
        docker_image=docker_image,
        overridden_args=overriden_args,
        overridden_envs=overriden_envs,
        staging_bucket=staging_bucket,
        input_bucket_path=input_bucket_path,
        output_bucket_path=output_bucket_path,
        python_script_path=python_script_path,
        cpu=cpu,
        memory_gb=memory_gb,
    )
    result = asyncio.run(_run(service, job))
    return_code = 0
    if result:
        print("SUCCESS in %i seconds" % result.duration_seconds)
    else:
        print("FAILED in %i seconds" % result.duration_seconds)
        return_code = 2
    print("Id:   %s" % result.job.id)
    print("Logs: %s" % result.log_url)
    sys.exit(return_code)
