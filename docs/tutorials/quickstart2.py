import asyncio

import stlog

from smartjob import CloudRunSmartJob, get_smart_job_executor_service_singleton
from smartjob.app.executor import SmartJobExecutionResultFuture

# Get a JobExecutorService object
job_executor_service = get_smart_job_executor_service_singleton(
    project="your-gcp-project",
    region="us-east1",
    staging_bucket="gs://a-bucket-name",
)


async def main():
    # Let's define a Cloud Run job that runs a Python 3.12 container with the command "python --version"
    job = CloudRunSmartJob(
        name="foo", docker_image="python:3.12", overridden_args=["python", "--version"]
    )

    # Let's launch 10 jobs (in parallel!) and get 10 futures on the results
    futures: list[SmartJobExecutionResultFuture] = []
    for i in range(10):
        futures.append(
            await job_executor_service.schedule(job, add_envs={"JOB_NUMBER": str(i)})
        )

    # Let's print the log urls of the jobs
    for future in futures:
        print(f"You can follow job: {future.execution_id} at {future.log_url}")

    # Let's wait for all the results
    # (this is blocking until all the jobs are done)
    results = await asyncio.gather(*[future.result() for future in futures])

    # Let's print the execution results for each job
    for result in results:
        print(f"Job: {result.execution_id} => {result.success}")


if __name__ == "__main__":
    stlog.setup(level="INFO")  # setup a better logging
    asyncio.run(main())
