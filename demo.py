import asyncio

from devtools import debug
from stlog import setup

from smartjob import CloudRunSmartJob, get_smart_job_executor_service_singleton

job_service = get_smart_job_executor_service_singleton(
    namespace="demo",
    project="botify-pw-experimental",
    region="us-east1",
)


async def main():
    job = CloudRunSmartJob(
        name="foo", docker_image="python:3.12", overridden_args=["python", "--version"]
    )
    future1 = job_service.run(job)
    future2 = job_service.run(job)
    result1 = await future1
    result2 = await future2
    debug(result1)
    debug(result2)


if __name__ == "__main__":
    setup()
    asyncio.run(main())
