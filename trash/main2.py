import datetime

from google.cloud import aiplatform

aiplatform.init(
    project="botify-pw-experimental",
    location="us-east1",
    staging_bucket="gs://botify-pw-vertex-ai-pipelines",
)

job = aiplatform.CustomContainerTrainingJob(
    display_name="fabien-custom-job",
    container_uri="docker.io/python:3.12",
)

print("before")
print(datetime.datetime.now())
job.run()
print("after")
print(datetime.datetime.now())
