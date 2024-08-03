# SmartJob Executor Service

To get a [smartjob.SmartJobExecutorService][] use the function [smartjob.get_smart_job_executor_service_singleton][].

Then you can use the two methods on [smartjob.SmartJobExecutorService][] to execute [smartjob.SmartJob][] jobs.

These methods will return a [smartjob.SmartJobExecutionResult][] object (when the job is fully executed) or a [smartjob.SmartJobExecutionResultFuture][]  (a kind of future on the job result).

## Getting the executor service

::: smartjob.get_smart_job_executor_service_singleton
    options:
      show_root_heading: true
      heading_level: 3
      show_signature: false

## SmartJobExecutorService object

::: smartjob.SmartJobExecutorService
    options:
      show_root_heading: true
      heading_level: 3

## ExecutionResult classes

::: smartjob.SmartJobExecutionResult
    options:
      show_root_heading: true
      heading_level: 3

::: smartjob.SmartJobExecutionResultFuture
    options:
      show_root_heading: true
      heading_level: 3
