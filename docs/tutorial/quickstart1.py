import asyncio
import logging

from smartjob import CloudRunSmartJob, get_smart_job_executor_service_singleton

# Get a JobExecutorService object
job_executor_service = get_smart_job_executor_service_singleton(
    project="your-gcp-project",
    region="us-east1",
)


async def main():
    # Let's define a Cloud Run job that runs a Python 3.12 container with the command "python --version"
    job = CloudRunSmartJob(
        name="foo", docker_image="python:3.12", overridden_args=["python", "--version"]
    )

    # Let's execute the job (and wait for the result)
    result = await job_executor_service.run(job)

    # Let's print the execution result
    print(result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
