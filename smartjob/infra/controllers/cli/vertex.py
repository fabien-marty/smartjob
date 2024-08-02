import shlex

import typer

from smartjob.app.job import VertexSmartJob
from smartjob.infra.controllers.cli.utils import (
    DockerImageArgument,
    InputBucketBasePathArgument,
    InputBucketPathArgument,
    NameArgument,
    OutputBucketBasePathArgument,
    OutputBucketPathArgument,
    OverrideCommandArgument,
    OverrideEnvArgument,
    PythonScriptPathArgument,
    StagingBucketArgument,
    WaitArgument,
    cli_process,
    get_job_service,
    init_stlog,
)

cli = typer.Typer()


@cli.command()
def run(
    ctx: typer.Context,
    name: str = NameArgument,
    docker_image: str = DockerImageArgument,
    override_command_and_args: str = OverrideCommandArgument,
    override_env: list[str] = OverrideEnvArgument,
    staging_bucket: str = StagingBucketArgument,
    input_bucket_base_path: str = InputBucketBasePathArgument,
    output_bucket_base_path: str = OutputBucketBasePathArgument,
    input_bucket_path: str = InputBucketPathArgument,
    output_bucket_path: str = OutputBucketPathArgument,
    python_script_path: str = PythonScriptPathArgument,
    wait: bool = WaitArgument,
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
    job = VertexSmartJob(
        name=name,
        docker_image=docker_image,
        overridden_args=overriden_args,
        overridden_envs=overriden_envs,
        staging_bucket=staging_bucket,
        input_bucket_path=input_bucket_path,
        output_bucket_path=output_bucket_path,
        python_script_path=python_script_path,
    )
    cli_process(service, job, wait)
