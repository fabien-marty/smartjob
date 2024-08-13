# SmartJob Executor Service

To get a [smartjob.ExecutorService][] use the function [smartjob.get_executor_service][].

Then you can use the three methods on [smartjob.ExecutorService][] to execute [smartjob.SmartJob][] jobs
(depending on your use-case).

These methods will return a [smartjob.ExecutionResult][] object (when the job is fully executed) or a [smartjob.ExecutionResultFuture][]  (a kind of future on the job result).

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

::: smartjob.ExecutionResult
    options:
      show_root_heading: true
      heading_level: 3

::: smartjob.ExecutionResultFuture
    options:
      show_root_heading: true
      heading_level: 3
