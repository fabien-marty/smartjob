import asyncio

from rich import print as rprint
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
    future1 = await job_service.schedule(job)
    future2 = await job_service.schedule(job)
    # result1 = await future1
    # result2 = await future2
    # print("waiting...")
    # await asyncio.sleep(30)
    # print("done waiting")
    result1 = await future1.result()
    result2 = await future2.result()
    rprint(result1.asdict())
    rprint(result2.asdict())


if __name__ == "__main__":
    setup()
    asyncio.run(main())
