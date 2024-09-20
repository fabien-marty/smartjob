import typer
from stlog import setup

from smartjob.app.input import GcsInput, Input, LocalPathInput
from smartjob.app.job import DEFAULT_NAMESPACE


def add_env_argument_to_dict(add_env: list[str]) -> dict[str, str]:
    add_envs: dict[str, str] = {}
    for ae in add_env:
        if "=" not in ae:
            raise ValueError(f"Invalid env var format: {ae} => must be key=value")
        key, value = ae.split("=", 1)
        add_envs[key] = value
    return add_envs


def local_path_input_to_list(local_path_input: list[str]) -> list[Input]:
    res: list[Input] = []
    for lpi in local_path_input:
        if "=" not in lpi:
            raise ValueError(
                f"Invalid input format: {lpi} => must be filename=local_path"
            )
        filename, local_path = lpi.split("=", 1)
        res.append(
            LocalPathInput(
                filename=filename,
                local_path=local_path,
            )
        )
    return res


def gcs_input_to_list(gcs_input: list[str]) -> list[Input]:
    res: list[Input] = []
    for lpi in gcs_input:
        if "=" not in lpi:
            raise ValueError(
                f"Invalid input format: {lpi} => must be filename=full_gcs_path"
            )
        filename, gcs_path = lpi.split("=", 1)
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {gcs_path} => must start with gs://")
        if "/" not in gcs_path[5:]:
            raise ValueError(
                f"Invalid GCS path: {gcs_path} => must have a / after gs://"
            )
        res.append(
            GcsInput(
                filename=filename,
                gcs_path=gcs_path,
            )
        )
    return res


def init_stlog(log_level: str):
    setup(level=log_level)


CpuOption = typer.Option(None, help="Number of CPUs (cloudrun only)")
MemoryOption = typer.Option(None, help="Memory in Gb (cloudrun only)")
MachineOption = typer.Option(None, help="Machine type (vertex only)")
NamespaceArgument = typer.Option(
    DEFAULT_NAMESPACE, envvar="SMARTJOB_NAMESPACE", help="namespace (simple string)"
)
NameArgument = typer.Argument(help="Name of the job")
DockerImageArgument = typer.Option("", help="Docker image to use")
OverrideCommandArgument = typer.Option(
    "",
    help="Override docker image command and arguments",
)
AddEnvArgument = typer.Option(
    [],
    help="Add a new env var in the container environment (format: key=value, can be used multiple times)",
)
StagingBucketArgument = typer.Option(
    None,
    envvar="SMARTJOB_STAGING_BUCKET",
    help="staging bucket (starting with gs://) for loading python_script_path (cloudrun or vertex only)",
)
InputBucketBasePathArgument = typer.Option(
    None, envvar="SMARTJOB_INPUT_BUCKET_BASE_PATH"
)
OutputBucketBasePathArgument: str = typer.Option(
    None, envvar="SMARTJOB_OUTPUT_BUCKET_BASE_PATH"
)
InputBucketPathArgument = typer.Option("")
OutputBucketPathArgument = typer.Option("")
PythonScriptPathArgument = typer.Option(
    "", help="local path to python script to execute in the container"
)
LocalPathInputArgument = typer.Option(
    [],
    help="Local path input (format: filename=local_path, can be used multiple times)",
)
GcsPathInputArguments = typer.Option(
    [],
    help="GCS path input (format: filename=full_source_gcs_path, can be used multiple times)",
)
LogLevelArgument = typer.Option("INFO", envvar="LOG_LEVEL", help="log level")
ProjectArgument = typer.Option(None, envvar="SMARTJOB_PROJECT", help="GCP project")
RegionArgument = typer.Option(None, envvar="SMARTJOB_REGION", help="GCP region")
ExecutorArgument = typer.Option(
    "cloudrun", help="executor type ('vertex', 'cloudrun' or 'docker')"
)
ServiceAccountArgument = typer.Option(
    None, envvar="SMARTJOB_SERVICE_ACCOUNT", help="GCP service account to use"
)
MaxAttemptsArgument = typer.Option(
    3, help="Number of attempts (if failure), must be >= 1"
)
VpcConnectorNetwork = typer.Option(None, help="VPC connector network (cloudrun only)")
VpcConnectorSubNetwork = typer.Option(
    None, help="VPC connector subnetwork (cloudrun only)"
)
