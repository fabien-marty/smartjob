# How it works?

## The (mostly) unified way

We try to keep a unified way of dealing with CloudRun jobs, Vertex AI custom jobs and (local) Docker jobs.

### Concepts

- a [smartjob](/reference/api/jobs/) is only an execution *(with CloudRun Jobs, jobs and jobs executions are two separate things, we completely hide this)* 
- each [smartjob](/reference/api/jobs/) can have an "input" and an "output" (in the form of a GCS bucket path):
    - these input/output are "mounted" as directories in the container tree
    - the input directory full path is set in the `INPUT_PATH` environment variable *(automatically injected by the library)*
    - the output directory full path is set in the `OUTPUT_PATH` environment variable *(automatically injected by the library)*
        - you can create any file you want in the output directory but the `smartjob.json` file name because (if present) it will be automatically downloaded/parsed by the SmartJob lib and the content will be injected in the Python result object.
- when you're in the experimentation phase, you can pass a Python script local path to execute in the job container *(so it overrides the default docker image command/arguments)*; when doing that:
    - the local Python script is automatically uploaded in the configured `staging_bucket` *(that will be mounted in the container tree)* 
    - job args are overriden to execute the Python script *(instead of the default docker image command/arguments)*
without rebuilding/pushing a whole docker image at each attempt

## CloudRun specifics 

- we use a threadpool (default size: `10`) for concurrency but only for uploading/downloading files as the CloudRun API is fully async

## Vertex specifics

- we use a threadpool (default size: `10`) for concurrency

## (local) Docker specifics

- we use a named volume `smartjob-staging` for "input"/"ouput" 
- we use a container `smartjob-dummy-container` (based on `docker.io/alpine:latest` image) for copying files from/to the staging volume (the container is created but never started)
- we use a threadpool (default size: `10`) for concurrency