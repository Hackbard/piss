import asyncio
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

import yaml

from scraper.config import get_settings
from scraper.mediawiki.client import get_client
from scraper.mediawiki.types import (
    CachedResponseMetadata,
    LatestCacheManifest,
    MediaWikiParseResponse,
    MediaWikiQueryResponse,
)
from scraper.utils.hashing import sha256_hash_json
from scraper.utils.time import utc_now_iso

settings = get_settings()
SEEDS_FILE = Path("config/seeds.yaml")


def normalize_title(title: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", title).strip("_")


def get_cache_path(page_title: str, revision_id: int, endpoint_kind: str) -> Path:
    safe_title = normalize_title(page_title)
    cache_dir = settings.scraper_cache_dir / "mediawiki" / safe_title / str(revision_id) / endpoint_kind
    return cache_dir


def get_latest_manifest_path(page_title: str) -> Path:
    safe_title = normalize_title(page_title)
    return settings.scraper_cache_dir / "mediawiki" / safe_title / "latest.json"


def get_manifest_path(run_id: str) -> Path:
    manifests_dir = settings.scraper_cache_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    return manifests_dir / f"{run_id}.json"


def load_seeds() -> Dict[str, Any]:
    if not SEEDS_FILE.exists():
        raise FileNotFoundError(f"Seeds file not found: {SEEDS_FILE}")
    with open(SEEDS_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_seeds() -> None:
    seeds = load_seeds()
    if not isinstance(seeds, dict):
        raise ValueError("Seeds must be a dictionary")

    seen_keys = set()
    required_fields = {"key", "page_title", "expected_time_range"}

    for seed_key, seed_data in seeds.items():
        if not isinstance(seed_data, dict):
            raise ValueError(f"Seed {seed_key} must be a dictionary")

        for field in required_fields:
            if field not in seed_data:
                raise ValueError(f"Seed {seed_key} missing required field: {field}")

        if seed_data["key"] != seed_key:
            raise ValueError(f"Seed {seed_key} key mismatch: {seed_data['key']}")

        if seed_data["key"] in seen_keys:
            raise ValueError(f"Duplicate seed key: {seed_data['key']}")
        seen_keys.add(seed_data["key"])

        time_range = seed_data["expected_time_range"]
        if not isinstance(time_range, dict) or "start" not in time_range or "end" not in time_range:
            raise ValueError(f"Seed {seed_key} expected_time_range must have start and end")


def get_seed(seed_key: str) -> Dict[str, Any]:
    seeds = load_seeds()
    if seed_key not in seeds:
        raise ValueError(f"Seed not found: {seed_key}")
    return seeds[seed_key]


async def fetch_and_cache_parse(
    page_title: str, run_id: str, force: bool = False, revalidate: bool = False
) -> Optional[MediaWikiParseResponse]:
    """Fetch and cache parse response, handling cache hits and revalidation."""
    client = get_client()

    if revalidate:
        query_response = await client.fetch_query(page_title)
        query_data = MediaWikiQueryResponse(**extract_query_data(query_response))
        current_revision = query_data.revision_id
        if current_revision is None:
            raise ValueError(f"Could not get revision for {page_title}")

        latest_path = get_latest_manifest_path(page_title)
        if latest_path.exists():
            latest = LatestCacheManifest(**json.loads(latest_path.read_text()))
            if latest.revision_id == current_revision:
                cache_path = get_cache_path(page_title, current_revision, "parse")
                raw_path = cache_path / "raw.json"
                if raw_path.exists():
                    return load_cached_parse_response(raw_path)
        force = True

    latest_path = get_latest_manifest_path(page_title)
    if not force and latest_path.exists():
        latest = LatestCacheManifest(**json.loads(latest_path.read_text()))
        cache_path = get_cache_path(page_title, latest.revision_id, "parse")
        raw_path = cache_path / "raw.json"
        if raw_path.exists():
            return load_cached_parse_response(raw_path)

    response_json = await client.fetch_parse(page_title, include_sections=True)
    parse_data = response_json.get("parse", {})
    page_id = parse_data.get("pageid", 0)
    revision_id = parse_data.get("revid", 0)
    html = parse_data.get("text", {}).get("*", "")
    displaytitle = parse_data.get("displaytitle")

    cache_path = get_cache_path(page_title, revision_id, "parse")
    cache_path.mkdir(parents=True, exist_ok=True)

    raw_path = cache_path / "raw.json"
    metadata_path = cache_path / "metadata.json"

    sha256 = sha256_hash_json(response_json)
    retrieved_at = utc_now_iso()

    raw_path.write_text(json.dumps(response_json, ensure_ascii=False, indent=2), encoding="utf-8")

    metadata = CachedResponseMetadata(
        request_params={"action": "parse", "page": page_title},
        response_headers={},
        retrieved_at=retrieved_at,
        sha256=sha256,
        url=f"{client.BASE_URL}?action=parse&page={page_title}",
        page_title=page_title,
        page_id=page_id,
        revision_id=revision_id,
        endpoint_kind="parse",
    )
    metadata_path.write_text(metadata.model_dump_json(indent=2), encoding="utf-8")

    latest_manifest = LatestCacheManifest(
        revision_id=revision_id,
        retrieved_at=retrieved_at,
        sha256=sha256,
        endpoint_kind="parse",
    )
    latest_path.write_text(latest_manifest.model_dump_json(indent=2), encoding="utf-8")
    
    # Update evidence index
    from scraper.cache.evidence_index import update_evidence_index
    from scraper.utils.ids import generate_evidence_id
    
    evidence_id = generate_evidence_id(page_id, revision_id, "parse", sha256)
    update_evidence_index(
        evidence_id=evidence_id,
        source_kind="mediawiki",
        cache_metadata_path=metadata_path,
        cache_raw_path=raw_path,
        page_title=page_title,
        page_id=page_id,
        revision_id=revision_id,
        sha256=sha256,
    )

    return MediaWikiParseResponse(
        parse=parse_data,
        page_id=page_id,
        revision_id=revision_id,
        page_title=page_title,
        html=html,
        displaytitle=displaytitle,
    )


def extract_query_data(response_json: Dict[str, Any]) -> Dict[str, Any]:
    pages = response_json.get("query", {}).get("pages", {})
    if not pages:
        raise ValueError("No pages in query response")

    page_data = list(pages.values())[0]
    page_id = page_data.get("pageid", 0)
    revisions = page_data.get("revisions", [])
    revision_id = None
    revision_timestamp = None
    if revisions:
        rev = revisions[0]
        revision_id = rev.get("revid")
        revision_timestamp = rev.get("timestamp")

    return {
        "pages": pages,
        "page_id": page_id,
        "revision_id": revision_id,
        "revision_timestamp": revision_timestamp,
    }


def load_cached_parse_response(raw_path: Path) -> MediaWikiParseResponse:
    response_json = json.loads(raw_path.read_text(encoding="utf-8"))
    parse_data = response_json.get("parse", {})
    return MediaWikiParseResponse(
        parse=parse_data,
        page_id=parse_data.get("pageid", 0),
        revision_id=parse_data.get("revid", 0),
        page_title=parse_data.get("title", ""),
        html=parse_data.get("text", {}).get("*", ""),
        displaytitle=parse_data.get("displaytitle"),
    )


def get_cached_parse_response(seed_key: str) -> Optional[MediaWikiParseResponse]:
    seed = get_seed(seed_key)
    page_title = seed["page_title"]
    latest_path = get_latest_manifest_path(page_title)
    if not latest_path.exists():
        return None

    latest = LatestCacheManifest(**json.loads(latest_path.read_text()))
    cache_path = get_cache_path(page_title, latest.revision_id, "parse")
    raw_path = cache_path / "raw.json"
    if not raw_path.exists():
        return None

    return load_cached_parse_response(raw_path)


def get_cached_metadata(page_title: str) -> Optional[CachedResponseMetadata]:
    """Get cached metadata for a page title."""
    latest_path = get_latest_manifest_path(page_title)
    if not latest_path.exists():
        return None

    latest = LatestCacheManifest(**json.loads(latest_path.read_text()))
    cache_path = get_cache_path(page_title, latest.revision_id, "parse")
    metadata_path = cache_path / "metadata.json"
    if not metadata_path.exists():
        return None

    return CachedResponseMetadata(**json.loads(metadata_path.read_text()))


def fetch_legislature_page(seed_key: str, run_id: str, force: bool = False, revalidate: bool = False) -> None:
    seed = get_seed(seed_key)
    page_title = seed["page_title"]
    response = asyncio.run(fetch_and_cache_parse(page_title, run_id, force=force, revalidate=revalidate))
    if not response:
        raise ValueError(f"Failed to fetch page: {page_title}")


def fetch_person_page(page_title: str, run_id: str, force: bool = False, revalidate: bool = False) -> None:
    asyncio.run(fetch_and_cache_parse(page_title, run_id, force=force, revalidate=revalidate))

