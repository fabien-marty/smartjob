# SmartJobs objects

At your user level, you have to manipulate two classes:

- [smartjob.CloudRunSmartJob][] for creating jobs for executing with Cloud Run Jobs
- [smartjob.VertexSmartJob][] for creating jobs for executing with Vertex AI CustomJobs

Both classes inherits from the [smartjob.SmartJob] abstract base class. 

## SmartJob base class

This is an abstract base class. **You don't have to manipulate it directly.**

::: smartjob.SmartJob
    options:
      show_root_heading: true
      heading_level: 3


## CloudRun SmartJob class

Create an instance of this class if you are targeting Cloud Run Jobs.

::: smartjob.CloudRunSmartJob
    options:
      show_root_heading: true
      heading_level: 3

## Vertex SmartJob class

Create an instance of this class if you are targeting Vertex AI CustomJobs.

::: smartjob.VertexSmartJob
    options:
      show_root_heading: true
      heading_level: 3
