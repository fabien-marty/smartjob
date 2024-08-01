import typer
from stlog import setup

from smartjob.app.executor import SmartJobExecutorService
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
