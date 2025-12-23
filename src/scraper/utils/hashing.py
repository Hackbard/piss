import hashlib
from typing import Any


def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_hash_json(obj: Any) -> str:
    import json

    json_bytes = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return sha256_hash(json_bytes)

