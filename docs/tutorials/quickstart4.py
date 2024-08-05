import stlog

from smartjob import CloudRunSmartJob, get_executor_service_singleton
from smartjob.app.input import LocalPathInput

# Setup logging
stlog.setup(level="INFO")

# Get a JobExecutorService object
job_executor_service = get_executor_service_singleton(
    project="your-gcp-project",
    region="us-east1",
    staging_bucket="gs://a-bucket-name",
)

# Let's define a Cloud Run job that runs a local Python script
# (that will be automatically uploaded) into a Python 3.12 container
job = CloudRunSmartJob(
    name="foo", docker_image="python:3.12", python_script_path="./local_script2.py"
)

# Let's execute the job (and wait for the result) in a blocking synchronous way
# and pass an input as a local file
#
# 'my-input-file' will be the filename in the input directory of the container
# './local_script2.py' is the local path to the input file (you want to send)
# (here we are sending the same script as input)
result = job_executor_service.sync_run(
    job, inputs=[LocalPathInput("my-input-file", "./local_script2.py")]
)

# Let's print the execution result
# => It will print something like:
# ExecutionResult(
#     job_name=foo, job_namespace=default,
#     execution_id=48d23af998f54f0a812cdea85429850a,
#     state=SUCCESS, duration_seconds=21,
#     log_url=https://...,
#     json_output={
#         "hash": "087c01ffad8fb2cd580c63896e0c75d9ac6b028d"
#     }
# )
print(result)
