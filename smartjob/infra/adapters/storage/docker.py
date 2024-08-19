import asyncio
import io
import tarfile
from dataclasses import dataclass, field

import docker
from stlog import getLogger

from smartjob.app.storage import StoragePort

logger = getLogger("smartjob.storage.docker")


# inspired from https://stackoverflow.com/questions/46390309/how-to-copy-a-file-from-host-to-container-using-docker-py-docker-sdk
def copy_to_container(
    container: docker.models.containers.Container, content: str | bytes, dst: str
):
    tar_stream = io.BytesIO()
    src_stream = io.BytesIO()
    if isinstance(content, str):
        length = src_stream.write(content.encode())
    else:
        length = src_stream.write(content)
    src_stream.seek(0)
    with tarfile.open(fileobj=tar_stream, mode="w|") as tar:
        info = tarfile.TarInfo(name=dst)
        info.size = length
        tar.addfile(info, fileobj=src_stream)
    container.put_archive("/", tar_stream.getvalue())


def read_from_container(
    container: docker.models.containers.Container, src: str
) -> bytes:
    try:
        bits, _ = container.get_archive(src)
    except Exception:
        return b""
    res = io.BytesIO()
    for chunk in bits:
        res.write(chunk)
    res.seek(0)
    with tarfile.open(fileobj=res, mode="r|") as tar:
        for member in tar:
            memberfile = tar.extractfile(member)
            if memberfile:
                return memberfile.read()
    return b""


@dataclass
class DockerStorageAdapter(StoragePort):
    docker_client: docker.DockerClient = field(default_factory=docker.DockerClient)
    mount_points_cache: dict[str, str] = field(default_factory=dict, init=False)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def get_or_create_dummy_container_for_uploading_to_volumes(
        self,
    ) -> docker.models.containers.Container:
        # see https://github.com/moby/moby/issues/25245
        async with self.lock:
            staging_volume = self.get_or_create_staging_docker_volume()
            try:
                res = self.docker_client.containers.get("smartjob-dummy-container")
                logger.debug("smartjob-dummy-container found => let's reuse it")
                return res
            except Exception:
                pass
            logger.debug("let's create smartjob-dummy-container")
            try:
                self.docker_client.images.get("docker.io/alpine:latest")
            except Exception:
                logger.debug("let's pull docker.io/alpine:latest image")
                self.docker_client.images.pull("docker.io/alpine:latest")
            return self.docker_client.containers.create(
                image="docker.io/alpine:latest",
                name="smartjob-dummy-container",
                volumes={staging_volume.name: {"bind": "/staging", "mode": "rw"}},
            )

    def get_or_create_staging_docker_volume(self) -> docker.models.volumes.Volume:
        for volume in self.docker_client.volumes.list():
            if volume.name == "smartjob-staging":
                logger.debug("found smartjob-staging volume => let's reuse it")
                return volume
        # we have to create it
        logger.debug("let's create the smartjob-staging volume")
        return self.docker_client.volumes.create(name="smartjob-staging")

    async def download(self, source_bucket: str, source_path: str) -> bytes:
        dummy = await self.get_or_create_dummy_container_for_uploading_to_volumes()
        return read_from_container(dummy, "/staging/" + source_path)

    async def upload(
        self,
        content: bytes | str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        # Note: only_if_not_exists is not supported!
        if len(content) == 0 and destination_path.endswith("/"):
            # this is a special case for bucket-like storage
            # let's just create an empty file
            destination_path = destination_path + ".empty"
        dummy = await self.get_or_create_dummy_container_for_uploading_to_volumes()
        copy_to_container(dummy, content, "/staging/" + destination_path)

    async def copy(
        self,
        source_bucket: str,
        source_path: str,
        destination_bucket: str,
        destination_path: str,
        only_if_not_exists: bool = True,
    ):
        raise Exception("not supported for the moment")
