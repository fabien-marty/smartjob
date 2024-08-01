import datetime

from google.cloud import aiplatform

aiplatform.init(
    project="botify-pw-experimental",
    location="us-east1",
    staging_bucket="gs://botify-pw-vertex-ai-pipelines",
)

job = aiplatform.CustomJob.from_local_script(
    display_name="fabien2-custom-job",
    script_path="test.py",
    container_uri="docker.io/python:3.12",
)

print("before")
print(datetime.datetime.now())
job.run(sync=False)
print("after")
print(datetime.datetime.now())
