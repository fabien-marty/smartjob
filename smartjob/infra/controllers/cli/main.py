from types import SimpleNamespace

import typer
import typer.core

from smartjob.infra.controllers.cli.cloudrun import cli as cloudrun_cli
from smartjob.infra.controllers.cli.vertex import cli as vertex_cli

# Disable rich usage for CLI doc generation
typer.core.rich = None  # type: ignore

cli = typer.Typer(add_completion=False)
cli.add_typer(cloudrun_cli, name="cloudrun")
cli.add_typer(vertex_cli, name="vertex")


@cli.callback()
def main(
    ctx: typer.Context,
    namespace: str = typer.Option(
        None, envvar="SMARTJOB_NAMESPACE", help="namespace (simple string)"
    ),
    project: str = typer.Option(None, envvar="SMARTJOB_PROJECT", help="project name"),
    region: str = typer.Option(None, envvar="SMARTJOB_REGION", help="region name"),
    log_level: str = typer.Option("INFO", envvar="LOG_LEVEL", help="log level"),
):
    ctx.obj = SimpleNamespace(
        namespace=namespace,
        region=region,
        project=project,
        log_level=log_level,
    )


if __name__ == "__main__":
    cli()
