import stlog

from smartjob import SmartJob, get_executor_service
from smartjob.app.execution import ExecutionConfig

# Setup logging
stlog.setup(level="INFO")

# Get a JobExecutorService object
job_executor_service = get_executor_service("cloudrun")

# Let's define an ExecutionConfig object
execution_config = ExecutionConfig(
    project="your-gcp-project",
    region="us-east1",
    staging_bucket="gs://a-bucket-name",
)

# Let's define a Cloud Run job that runs a local Python script
# (that will be automatically uploaded) into a Python 3.12 container
job = SmartJob(
    name="foo", docker_image="python:3.12", python_script_path="./local_script.py"
)

# Let's execute the job (and wait for the result) in a blocking synchronous way
result = job_executor_service.run(job, execution_config=execution_config)

# Let's print the execution result
print(result)
