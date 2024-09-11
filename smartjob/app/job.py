from dataclasses import dataclass, field

# DEFAULT_NAMESPACE is the default namespace for jobs.
DEFAULT_NAMESPACE = "default"


@dataclass
class SmartJob:
    """Base class for smart jobs."""

    name: str
    """Name of the job."""

    docker_image: str
    """Docker image to use for the job.

    Example: `docker.io/python:3.12`.
    """

    namespace: str = DEFAULT_NAMESPACE
    """Namespace of the job."""

    add_envs: dict[str, str] = field(default_factory=dict)
    """Environnement variables to add in the container.

    Note: `INPUT_PATH` and `OUTPUT_PATH` will be automatically injected (if relevant).
    """

    overridden_args: list[str] = field(default_factory=list)
    """Container arguments (including command) to use.

    Notes:
        - if not set, the default container image arguments will be used.
        - the placeholder {{INPUT}} will be automatically replaced by the local full path of the input directory.

    """

    python_script_path: str = ""
    """Local path to a python script to execute in the container.

    Note: if set, it will override overridden_args."""

    @property
    def full_name(self) -> str:
        """Return the full name of the job (namespace + name)."""
        return f"{self.namespace}-{self.name}"
