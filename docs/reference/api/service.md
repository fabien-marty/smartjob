# SmartJob Executor Service

To get a [smartjob.ExecutorService][] use the function [smartjob.get_executor_service][].

Then you can use the two methods on [smartjob.ExecutorService][] to execute [smartjob.SmartJob][] jobs
(depending on your use-case).

These methods will return a [smartjob.ExecutionResult][] object (when the job is fully executed) or a future on it. 

## Getting the executor service

::: smartjob.get_executor_service
    options:
      show_root_heading: true
      heading_level: 3
      show_signature: false

## ExecutorService object

::: smartjob.ExecutorService
    options:
      show_root_heading: true
      heading_level: 3

## ExecutionResult classes

::: smartjob.SchedulingDetails
    options:
      show_root_heading: true
      heading_level: 3

::: smartjob.ExecutionResult
    options:
      show_root_heading: true
      heading_level: 3
