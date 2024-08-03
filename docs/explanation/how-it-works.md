# How it works?

## The (mostly) unified way

We try to keep a unified way of dealing with CloudRun jobs and Vertex AI custom jobs.

### Concepts

- a [smartjob](/reference/api/jobs/) is only an execution *(with CloudRun Jobs, jobs and jobs executions are two separate things, we completely hide this)* 
- each [smartjob](/reference/api/jobs/) can have an "input" and an "output" (in the form of a GCS bucket path):
    - these input/output are "mounted" as directories in the container tree
    - the input directory full path is set in the `IN_PATH` environment variable *(automatically injected by the library)*
    - the output directory full path is set in the `OUT_PATH` environment variable *(automatically injected by the library)*
- when you're in the experimentation phase, you can pass a Python script local path to execute in the job container *(so it overrides the default docker image command/arguments)*; when doing that:
    - the local Python script is automatically uploaded in the configured `staging_bucket` *(that will be mounted in the container tree)* 
    - job args are overriden to execute the Python script *(instead of the default docker image command/arguments)*
without rebuilding/pushing a whole docker image at each attempt)*

- FIXME: threadpool (for uploading)

## CloudRun specifics 

## Vertex specifics

- FIXME: threadpool