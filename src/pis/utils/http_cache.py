from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from pis.utils.hashing import sha256_json
from pis.utils.time import utc_now_iso


class HttpCacheError(RuntimeError):
    pass


@dataclass(frozen=True)
class CachedHttpResponse:
    url: str
    status_code: int
    retrieved_at: str
    sha256: str
    cache_key: str
    raw_path: Path
    metadata_path: Path
    data: Any


def _request_key(method: str, url: str, params: Mapping[str, Any] | None, json_body: Any) -> str:
    return sha256_json(
        {
            "method": method.upper(),
            "url": url,
            "params": dict(params or {}),
            "json": json_body,
        }
    )


def _sleep_rate_limited(last_request_at: float | None, rps: float) -> float:
    if rps <= 0:
        return time.time()
    min_interval = 1.0 / rps
    now = time.time()
    if last_request_at is not None:
        elapsed = now - last_request_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
    return time.time()


@retry(
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError)),
    wait=wait_exponential_jitter(initial=0.5, max=10.0),
    stop=stop_after_attempt(5),
    reraise=True,
)
def cached_get_json(
    *,
    client: httpx.Client,
    cache_dir: Path,
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 30.0,
    force: bool = False,
    rate_limit_rps: float = 1.0,
    _last_request_at: dict[str, float] | None = None,
) -> CachedHttpResponse:
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = _request_key("GET", url, params, None)
    entry_dir = cache_dir / key[:2] / key[2:4] / key
    raw_path = entry_dir / "raw.json"
    metadata_path = entry_dir / "metadata.json"

    if not force and raw_path.exists() and metadata_path.exists():
        with open(raw_path, encoding="utf-8") as f:
            data = json.load(f)
        with open(metadata_path, encoding="utf-8") as f:
            meta = json.load(f)
        return CachedHttpResponse(
            url=url,
            status_code=int(meta.get("status_code", 200)),
            retrieved_at=str(meta.get("retrieved_at")),
            sha256=str(meta.get("sha256")),
            cache_key=key,
            raw_path=raw_path,
            metadata_path=metadata_path,
            data=data,
        )

    entry_dir.mkdir(parents=True, exist_ok=True)

    if _last_request_at is not None:
        _last_request_at["t"] = _sleep_rate_limited(_last_request_at.get("t"), rate_limit_rps)

    resp = client.get(url, params=params, headers=headers, timeout=timeout_seconds)
    resp.raise_for_status()
    data = resp.json()
    sha = sha256_json(data)
    meta = {
        "url": url,
        "params": dict(params or {}),
        "status_code": resp.status_code,
        "retrieved_at": utc_now_iso(),
        "sha256": sha,
        "headers": {"accept": resp.headers.get("content-type")},
    }
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return CachedHttpResponse(
        url=url,
        status_code=resp.status_code,
        retrieved_at=str(meta["retrieved_at"]),
        sha256=sha,
        cache_key=key,
        raw_path=raw_path,
        metadata_path=metadata_path,
        data=data,
    )

