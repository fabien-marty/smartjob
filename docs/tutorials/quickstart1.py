from smartjob import SmartJob, get_executor_service
from smartjob.app.execution import ExecutionConfig

# Get a JobExecutorService object (for executing jobs with Cloud Run Jobs)
job_executor_service = get_executor_service("cloudrun")

# Let's define a Cloud Run job that runs a Python 3.12 container with the command "python --version"
job = SmartJob(
    name="foo", docker_image="python:3.12", overridden_args=["python", "--version"]
)

# Let's define an ExecutionConfig object
execution_config = ExecutionConfig(
    project="your-gcp-project",
    region="us-east1",
    staging_bucket="gs://a-bucket-name",
)

# Let's execute the job (and wait for the result) in a blocking synchronous way
result = job_executor_service.sync_run(job, execution_config=execution_config)

# Let's print the execution result
# => it will print something like:
# ExecutionResult(
#     job_name=foo, job_namespace=default,
#     execution_id=ce5dc480b392434a833a7dbbfae8dcd9,
#     state=SUCCESS, duration_seconds=14,
#     log_url=https://...
# )
print(result)
