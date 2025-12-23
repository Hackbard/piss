import asyncio
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from scraper.config import get_settings
from scraper.sources.dip.client import get_dip_client
from scraper.sources.dip.types import DipPerson, DipPersonListResponse
from scraper.utils.hashing import sha256_hash_json
from scraper.utils.time import utc_now_iso

logger = logging.getLogger(__name__)

settings = get_settings()


def normalize_endpoint(endpoint: str) -> str:
    return endpoint.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "_")


def hash_params(params: Dict[str, Any]) -> str:
    sorted_params = json.dumps(params, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(sorted_params.encode("utf-8")).hexdigest()[:16]


def get_dip_cache_path(endpoint: str, params_hash: str) -> Path:
    safe_endpoint = normalize_endpoint(endpoint)
    cache_dir = settings.scraper_cache_dir / "dip" / safe_endpoint / params_hash
    return cache_dir


def get_dip_latest_path(endpoint: str) -> Path:
    safe_endpoint = normalize_endpoint(endpoint)
    return settings.scraper_cache_dir / "dip" / safe_endpoint / "latest.json"


async def ingest_person_list(
    wahlperiode: List[int],
    run_id: str,
    force: bool = False,
) -> List[DipPerson]:
    client = get_dip_client()
    all_persons: List[DipPerson] = []
    cursor: Optional[str] = None
    page = 0

    while True:
        params: Dict[str, Any] = {"f.wahlperiode": wahlperiode, "limit": 100}
        if cursor:
            params["cursor"] = cursor

        params_hash = hash_params(params)
        endpoint = "/person"
        cache_path = get_dip_cache_path(endpoint, params_hash)
        raw_path = cache_path / "raw.json"
        metadata_path = cache_path / "metadata.json"

        if not force and raw_path.exists():
            response_json = json.loads(raw_path.read_text(encoding="utf-8"))
            logger.info(f"Cache hit for DIP person list WP {wahlperiode}, cursor: {cursor}")
        else:
            try:
                response_json = await client.fetch_person_list(
                    wahlperiode=wahlperiode, cursor=cursor
                )
            except Exception as e:
                if "401" in str(e) or "Unauthorized" in str(e):
                    raise ValueError(
                        "DIP API authentication failed. Please set DIP_API_KEY in .env file."
                    ) from e
                raise

            cache_path.mkdir(parents=True, exist_ok=True)
            sha256 = sha256_hash_json(response_json)
            retrieved_at = utc_now_iso()

            raw_path.write_text(
                json.dumps(response_json, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            logger.info(f"Cache miss - fetched and cached DIP person list WP {wahlperiode}, cursor: {cursor}")

            metadata = {
                "request_params": params,
                "response_headers": {},
                "retrieved_at": retrieved_at,
                "sha256": sha256,
                "url": f"{client.base_url}{endpoint}",
                "endpoint": endpoint,
                "page": page,
                "cursor": cursor,
            }
            metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

        for doc in response_json.get("documents", []):
            if "fraktion" in doc and isinstance(doc["fraktion"], list):
                doc["fraktion"] = doc["fraktion"][0] if doc["fraktion"] else None

        response = DipPersonListResponse(**response_json)
        all_persons.extend(response.documents)

        if not response.cursor or response.cursor == cursor:
            break

        cursor = response.cursor
        page += 1

    return all_persons


def ingest_person_list_sync(
    wahlperiode: List[int],
    run_id: str,
    force: bool = False,
) -> List[DipPerson]:
    return asyncio.run(ingest_person_list(wahlperiode, run_id, force=force))

