# SmartJob and ExecutionConfig objects

At your user level, you have to manipulate two classes:

- [smartjob.SmartJob][] to define jobs
- [smartjob.ExecutionConfig][] to define "execution config" for running your jobs

## SmartJob class

::: smartjob.SmartJob
    options:
      show_root_heading: true
      heading_level: 3


## ExecutionConfig class

Create an instance of this class if you are targeting Cloud Run Jobs.

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