from smartjob import CloudRunSmartJob, get_smart_job_executor_service_singleton

# Get a JobExecutorService object
job_executor_service = get_smart_job_executor_service_singleton(
    project="your-gcp-project",
    region="us-east1",
    staging_bucket="gs://a-bucket-name",
)

# Let's define a Cloud Run job that runs a Python 3.12 container with the command "python --version"
job = CloudRunSmartJob(
    name="foo", docker_image="python:3.12", overridden_args=["python", "--version"]
)

# Let's execute the job (and wait for the result) in a blocking synchronous way
result = job_executor_service.sync_run(job)

# Let's print the execution result
# => it will print something like:
# SmartJobExecutionResult(
#     job_name=foo, job_namespace=default,
#     execution_id=ce5dc480b392434a833a7dbbfae8dcd9,
#     state=SUCCESS, duration_seconds=14,
#     log_url=https://...
# )
print(result)
