import typer

cli = typer.Typer()


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
    pass
