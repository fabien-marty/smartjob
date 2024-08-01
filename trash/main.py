import asyncio

from google.cloud import run_v2

request = run_v2.RunJobRequest(
    name="projects/botify-pw-experimental/locations/europe-west9/jobs/fabien",
    overrides={
        "container_overrides": [
            {
                "env": [{"name": "FABIEN", "value": "Beta-1"}],
            }
        ],
        "task_count": 1,
    },
)


async def main():
    client = run_v2.JobsAsyncClient()
    print("before")
    operations = []
    for i in range(0, 10):
        operation = await client.run_job(request=request)
        operations.append(operation)
    print("after")
    print("awaiting...")
    await asyncio.gather(*[operation.result() for operation in operations])


asyncio.run(main())
