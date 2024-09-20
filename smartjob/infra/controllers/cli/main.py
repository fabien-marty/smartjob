import asyncio
import json
import shlex
import sys

import typer
import typer.core

from smartjob.app.execution import ExecutionConfig
from smartjob.app.executor import ExecutionResultFuture
from smartjob.app.job import SmartJob
from smartjob.app.retry import RetryConfig
from smartjob.infra.controllers.cli.common import (
    AddEnvArgument,
    CpuOption,
    DockerImageArgument,
    ExecutorArgument,
    GcsPathInputArguments,
    LocalPathInputArgument,
    LogLevelArgument,
    MachineOption,
    MaxAttemptsArgument,
    MemoryOption,
    NameArgument,
    NamespaceArgument,
    OverrideCommandArgument,
    ProjectArgument,
    PythonScriptPathArgument,
    RegionArgument,
    ServiceAccountArgument,
    StagingBucketArgument,
    VpcConnectorNetwork,
    VpcConnectorSubNetwork,
    add_env_argument_to_dict,
    gcs_input_to_list,
    init_stlog,
    local_path_input_to_list,
)
from smartjob.infra.controllers.lib import get_executor_service

# Disable rich usage for CLI doc generation
typer.core.rich = None  # type: ignore

cli = typer.Typer(add_completion=False)


if __name__ == "__main__":
    cli()


@cli.command()
def run(
    name: str = NameArgument,
    docker_image: str = DockerImageArgument,
    override_command_and_args: str = OverrideCommandArgument,
    add_env: list[str] = AddEnvArgument,
    staging_bucket: str = StagingBucketArgument,
    python_script_path: str = PythonScriptPathArgument,
    cpu: float = CpuOption,
    memory_gb: float = MemoryOption,
    local_path_input: list[str] = LocalPathInputArgument,
    gcs_input: list[str] = GcsPathInputArguments,
    log_level: str = LogLevelArgument,
    namespace: str = NamespaceArgument,
    project: str = ProjectArgument,
    region: str = RegionArgument,
    executor: str = ExecutorArgument,
    service_account: str = ServiceAccountArgument,
    max_attempts: int = MaxAttemptsArgument,
    vpc_connector_network: str = VpcConnectorNetwork,
    vpc_connector_subnetwork: str = VpcConnectorSubNetwork,
    machine_type: str = MachineOption,
):
    init_stlog(log_level)
    executor_service = get_executor_service(executor)
    add_envs = add_env_argument_to_dict(add_env)
    job = SmartJob(
        name=name,
        namespace=namespace,
        docker_image=docker_image,
        add_envs=add_envs,
        python_script_path=python_script_path,
        overridden_args=shlex.split(override_command_and_args),
    )
    execution_config = ExecutionConfig(
        project=project,
        region=region,
        staging_bucket=staging_bucket,
        cpu=cpu,
        memory_gb=memory_gb,
        service_account=service_account,
        retry_config=RetryConfig(max_attempts=max_attempts),
        vpc_connector_network=vpc_connector_network,
        vpc_connector_subnetwork=vpc_connector_subnetwork,
        machine_type=machine_type,
    )
    inputs = local_path_input_to_list(local_path_input) + gcs_input_to_list(gcs_input)
    result = executor_service.sync_run(
        job, inputs=inputs, execution_config=execution_config
    )
    return_code = 0
    if result:
        print("SUCCESS in %i seconds" % result.duration_seconds)
    else:
        print("FAILED in %i seconds" % result.duration_seconds)
        return_code = 2
    print("Id:      %s" % result.execution_id)
    print("Logs:    %s" % result.log_url)
    if result.json_output is not None:
        print("JsonOutput:")
        print(json.dumps(result.json_output, indent=4))
    sys.exit(return_code)


async def _schedule(
    executor_service, job, inputs, execution_config
) -> ExecutionResultFuture:
    return await executor_service.schedule(
        job, inputs=inputs, execution_config=execution_config
    )


@cli.command()
def schedule(
    name: str = NameArgument,
    docker_image: str = DockerImageArgument,
    override_command_and_args: str = OverrideCommandArgument,
    add_env: list[str] = AddEnvArgument,
    staging_bucket: str = StagingBucketArgument,
    python_script_path: str = PythonScriptPathArgument,
    cpu: float = typer.Option(1.0, help="Number of CPUs"),
    memory_gb: float = typer.Option(0.5, help="Memory in Gb"),
    local_path_input: list[str] = LocalPathInputArgument,
    gcs_input: list[str] = GcsPathInputArguments,
    log_level: str = LogLevelArgument,
    namespace: str = NamespaceArgument,
    project: str = ProjectArgument,
    region: str = RegionArgument,
    executor: str = ExecutorArgument,
    service_account: str = ServiceAccountArgument,
    max_attempts: int = MaxAttemptsArgument,
    vpc_connector_network: str = VpcConnectorNetwork,
    vpc_connector_subnetwork: str = VpcConnectorSubNetwork,
    machine_type: str = MachineOption,
):
    init_stlog(log_level)
    executor_service = get_executor_service(executor)
    add_envs = add_env_argument_to_dict(add_env)
    job = SmartJob(
        name=name,
        namespace=namespace,
        docker_image=docker_image,
        add_envs=add_envs,
        python_script_path=python_script_path,
        overridden_args=shlex.split(override_command_and_args),
    )
    execution_config = ExecutionConfig(
        project=project,
        region=region,
        staging_bucket=staging_bucket,
        cpu=cpu,
        memory_gb=memory_gb,
        service_account=service_account,
        retry_config=RetryConfig(max_attempts=max_attempts),
        vpc_connector_network=vpc_connector_network,
        vpc_connector_subnetwork=vpc_connector_subnetwork,
        machine_type=machine_type,
    )
    inputs = local_path_input_to_list(local_path_input) + gcs_input_to_list(gcs_input)
    result_future = asyncio.run(
        _schedule(executor_service, job, inputs, execution_config)
    )
    print("SCHEDULED!")
    print("Id:      %s" % result_future.execution_id)
    print("Logs:    %s" % result_future.log_url)
    result_future._cancel()
