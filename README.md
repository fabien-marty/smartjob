# Welcome on SmartJob Python Library!

## What is it?

**SmartJob** is a thin Python 3.10+ library for dealing with [GCP/Cloud Run Jobs](https://cloud.google.com/run) and [GCP/VertexAI CustomJobs](https://cloud.google.com/vertex-ai/docs/training/create-custom-job):

- in a **very simple** way
- in a **unified** way *(with as little difference as possible between the 2 providers)*
- in a **fully [async](https://docs.python.org/3/library/asyncio.html)** way *(in most cases, plain `asyncio` Python is enough for dealing with complex parallel workflows (including parralelism, chaining, conditionals...) and you don't need to learn another pipeline workflow)*
- in a **reactive** way *(when you're in the experimentation phase, you can pass the main Python script without rebuilding/pushing a whole docker image at each attempt)*


## Non-features

**SmartJob** is a thin library and not a whole pipeline framework. He tries to be as un-opinionated as possible.

## Quickstart

[Let's go 🚀](https://fabien-marty.github.io/smartjob/tutorials/quickstart/)

## Documentation

[https://fabien-marty.github.io/smartjob/](https://fabien-marty.github.io/smartjob/)