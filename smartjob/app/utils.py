import hashlib
import uuid


def hex_hash(*args: str) -> str:
    return hashlib.sha1("\n".join(args).encode()).hexdigest()


def unique_id() -> str:
    return str(uuid.uuid4()).replace("-", "")
