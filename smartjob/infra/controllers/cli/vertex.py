import shlex

import typer

from smartjob.app.job import VertexSmartJob
from smartjob.infra.controllers.cli.utils import (
    AddEnvArgument,
    DockerImageArgument,
    GcsPathInputArguments,
    LocalPathInputArgument,
    NameArgument,
    OverrideCommandArgument,
    PythonScriptPathArgument,
    StagingBucketArgument,
    WaitArgument,
    add_env_argument_to_dict,
    cli_process,
    gcs_input_to_list,
    get_job_service,
    init_stlog,
    local_path_input_to_list,
)

cli = typer.Typer()


@cli.command()
def run(
    ctx: typer.Context,
    name: str = NameArgument,
    docker_image: str = DockerImageArgument,
    override_command_and_args: str = OverrideCommandArgument,
    add_env: list[str] = AddEnvArgument,
    staging_bucket: str = StagingBucketArgument,
    python_script_path: str = PythonScriptPathArgument,
    wait: bool = WaitArgument,
    local_path_input: list[str] = LocalPathInputArgument,
    gcs_input: list[str] = GcsPathInputArguments,
):
    init_stlog(ctx)
    overriden_args = shlex.split(override_command_and_args)
    add_envs = add_env_argument_to_dict(add_env)
    inputs = local_path_input_to_list(local_path_input) + gcs_input_to_list(gcs_input)
    service = get_job_service(ctx)
    job = VertexSmartJob(
        name=name,
        docker_image=docker_image,
        overridden_args=overriden_args,
        add_envs=add_envs,
        staging_bucket=staging_bucket,
        python_script_path=python_script_path,
    )
    cli_process(service, job, wait, inputs)
