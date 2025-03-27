# SmartJob and ExecutionConfig objects

At your user level, you have to manipulate two classes:

- [smartjob.SmartJob][] to define jobs
- [smartjob.ExecutionConfig][] to define "execution config" for running your jobs

Depending on the options you want to set, you can also use the following classes:

- [smartjob.RetryConfig][] to define "retries" in `ExecutionConfig`
- [smartjob.TimeoutConfig][] to define "timeouts" in `ExecutionConfig`

## SmartJob class

::: smartjob.SmartJob
    options:
      show_root_heading: true
      heading_level: 3


## ExecutionConfig class

::: smartjob.ExecutionConfig
    options:
      show_root_heading: true
      heading_level: 3

### Dedicated class for defining "retries" in `ExecutionConfig`

::: smartjob.RetryConfig
    options:
      show_root_heading: true
      heading_level: 4

### Dedicated class for defining "timeouts" in `ExecutionConfig`

::: smartjob.TimeoutConfig
    options:
      show_root_heading: true
      heading_level: 4
