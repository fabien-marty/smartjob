import asyncio

from stlog import setup

from smartjob import CloudRunSmartJob, get_smart_job_executor_service_singleton

job_service = get_smart_job_executor_service_singleton(
    max_workers=10,
    namespace="demo",
    project="botify-pw-experimental",
    region="us-east1",
    staging_bucket="gs://botify-pw-vertex-ai-pipelines",
    input_bucket_base_path="gs://botify-pw-vertex-ai-pipelines",
    output_bucket_base_path="gs://botify-pw-vertex-ai-pipelines",
)


async def main():
    job = CloudRunSmartJob(
        name="foo", docker_image="python:3.12", python_script_path="./coucou.py"
    )
    result = await job_service.run(job)
    print(result)


if __name__ == "__main__":
    setup(
        level="DEBUG",
        extra_levels={
            "urllib3": "INFO",
            "asyncio": "INFO",
            "google": "INFO",
            "grpc": "INFO",
        },
    )
    asyncio.run(main())
