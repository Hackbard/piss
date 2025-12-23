import asyncio
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from bs4 import BeautifulSoup

from scraper.cache.mediawiki_cache import normalize_title
from scraper.config import get_settings
from scraper.mediawiki.client import get_client
from scraper.seeds.registry import get_registry_hash, load_registry
from scraper.utils.hashing import sha256_hash_json
from scraper.utils.time import utc_now_iso

logger = logging.getLogger(__name__)
settings = get_settings()


def normalize_title_for_key(title: str) -> str:
    """Normalize Wikipedia title for use in seed key."""
    return title.replace(" ", "_").replace("/", "_")


def extract_legislature_number(title: str) -> Optional[int]:
    """Extract legislature number from title like 'Liste der Mitglieder ... (17. Wahlperiode)'."""
    # Match patterns like "(17. Wahlperiode)", "(18. Wahlperiode)", etc.
    match = re.search(r"\((\d+)\.\s*Wahlperiode\)", title)
    if match:
        return int(match.group(1))
    return None


def validate_member_list_table(html: str, expected_keywords: List[str]) -> Tuple[bool, Optional[str]]:
    """Validate that HTML contains a member list table with expected keywords."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Find all tables
    tables = soup.find_all("table")
    if not tables:
        return False, "No tables found"
    
    # Check each table for member list characteristics
    for table in tables:
        header_row = table.find("tr")
        if not header_row:
            continue
        
        headers = header_row.find_all(["th", "td"])
        header_texts = [h.get_text().strip().lower() for h in headers]
        
        # Check for expected keywords
        found_keywords = []
        for keyword in expected_keywords:
            keyword_lower = keyword.lower()
            if any(keyword_lower in ht for ht in header_texts):
                found_keywords.append(keyword)
        
        # Must have at least "Name" and one of "Partei" or "Wahlkreis"
        has_name = any("name" in ht for ht in header_texts)
        has_party = any("partei" in ht or "fraktion" in ht for ht in header_texts)
        has_wahlkreis = any("wahlkreis" in ht for ht in header_texts)
        
        if has_name and (has_party or has_wahlkreis):
            # Check if table has actual data rows (not just header)
            rows = table.find_all("tr")[1:]
            if len(rows) > 0:
                return True, None
    
    return False, "No valid member list table found (missing Name + Partei/Wahlkreis columns)"


async def discover_landtage_seeds(
    registry_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
    pin_revisions: bool = True,
    force: bool = False,
) -> Dict[str, Any]:
    """Discover seeds for all landtage from registry."""
    run_id = str(uuid4())
    manifest: Dict[str, Any] = {
        "run_id": run_id,
        "started_at": utc_now_iso(),
        "registry_path": str(registry_path or settings.scraper_registry_path),
        "registry_hash": get_registry_hash(registry_path),
        "search_queries": [],
        "found_titles": [],
        "validated": [],
        "rejected": [],
        "output_file": None,
        "seed_count": 0,
        "errors": [],
    }

    try:
        registry = load_registry(registry_path)
        client = get_client()
        all_seeds: Dict[str, Dict[str, Any]] = {}
        seen_titles: Set[str] = set()
        seen_page_ids: Set[int] = set()

        expected_keywords = registry.defaults.get("expected_table_keywords", ["Name", "Partei", "Wahlkreis"])

        for landtag_key, landtag_entry in registry.landtage.items():
            logger.info(f"Discovering seeds for {landtag_entry.state} ({landtag_key})")

            # Search for member list pages
            found_titles_for_landtag: List[Dict[str, Any]] = []
            
            for search_query in landtag_entry.member_list_search:
                manifest["search_queries"].append({
                    "landtag": landtag_key,
                    "query": search_query,
                })

                try:
                    # Fetch search results (with caching)
                    search_params = {"query": search_query, "limit": 50}
                    params_hash = sha256_hash_json(search_params)
                    safe_title = normalize_title(f"search_{search_query}")
                    cache_path = settings.scraper_cache_dir / "mediawiki" / safe_title / params_hash[:16] / "search"
                    raw_path = cache_path / "raw.json"
                    metadata_path = cache_path / "metadata.json"

                    if not force and raw_path.exists() and metadata_path.exists():
                        with open(raw_path, "r", encoding="utf-8") as f:
                            search_response = json.load(f)
                        logger.info(f"Cache hit for search: {search_query}")
                    else:
                        search_response = await client.fetch_search(search_query, limit=50)
                        sha256 = sha256_hash_json(search_response)
                        retrieved_at = utc_now_iso()
                        
                        cache_path.mkdir(parents=True, exist_ok=True)
                        with open(raw_path, "w", encoding="utf-8") as f:
                            json.dump(search_response, f, ensure_ascii=False, indent=2)
                        
                        metadata = {
                            "request_params": search_params,
                            "response_headers": {},
                            "retrieved_at": retrieved_at,
                            "sha256": sha256,
                            "source_url": f"{client.BASE_URL}?action=query&list=search&srsearch={search_query}",
                            "endpoint_kind": "search",
                        }
                        with open(metadata_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=2)
                        
                        logger.info(f"Fetched search results for: {search_query}")

                    # Extract titles from search results
                    search_results = search_response.get("query", {}).get("search", [])
                    for result in search_results:
                        title = result.get("title", "")
                        snippet = result.get("snippet", "")
                        
                        if title and title not in seen_titles:
                            # Extract legislature number
                            legislature_number = extract_legislature_number(title)
                            if not legislature_number:
                                # Try to extract from snippet
                                legislature_number = extract_legislature_number(snippet)
                            
                            if legislature_number:
                                found_titles_for_landtag.append({
                                    "title": title,
                                    "snippet": snippet,
                                    "legislature_number": legislature_number,
                                })
                                seen_titles.add(title)

                except Exception as e:
                    logger.error(f"Search failed for {search_query}: {e}")
                    manifest["errors"].append(f"Search failed for {landtag_key}/{search_query}: {e}")
                    continue

            # Sort by legislature number for determinism
            found_titles_for_landtag.sort(key=lambda x: (x["legislature_number"], x["title"]))
            manifest["found_titles"].extend([
                {
                    "landtag": landtag_key,
                    "title": t["title"],
                    "legislature_number": t["legislature_number"],
                }
                for t in found_titles_for_landtag
            ])

            # Validate each found title
            for title_info in found_titles_for_landtag:
                title = title_info["title"]
                legislature_number = title_info["legislature_number"]
                
                try:
                    # Get page info and revision
                    query_params = {"page_title": title}
                    query_params_hash = sha256_hash_json(query_params)
                    safe_title = normalize_title(f"query_{title}")
                    query_cache_path = settings.scraper_cache_dir / "mediawiki" / safe_title / query_params_hash[:16] / "query"
                    query_raw_path = query_cache_path / "raw.json"
                    query_metadata_path = query_cache_path / "metadata.json"

                    if not force and query_raw_path.exists() and query_metadata_path.exists():
                        with open(query_raw_path, "r", encoding="utf-8") as f:
                            query_response = json.load(f)
                        logger.info(f"Cache hit for query: {title}")
                    else:
                        query_response = await client.fetch_query(title)
                        sha256 = sha256_hash_json(query_response)
                        retrieved_at = utc_now_iso()
                        
                        query_cache_path.mkdir(parents=True, exist_ok=True)
                        with open(query_raw_path, "w", encoding="utf-8") as f:
                            json.dump(query_response, f, ensure_ascii=False, indent=2)
                        
                        metadata = {
                            "request_params": query_params,
                            "response_headers": {},
                            "retrieved_at": retrieved_at,
                            "sha256": sha256,
                            "source_url": f"{client.BASE_URL}?action=query&prop=info|revisions&titles={title}",
                            "endpoint_kind": "query",
                        }
                        with open(query_metadata_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=2)
                        
                        logger.info(f"Fetched query info for: {title}")

                    # Extract page_id and revision_id
                    pages = query_response.get("query", {}).get("pages", {})
                    page_id = None
                    revision_id = None
                    
                    for page_data in pages.values():
                        page_id = page_data.get("pageid")
                        revisions = page_data.get("revisions", [])
                        if revisions:
                            revision_id = revisions[0].get("revid")
                        break

                    if not page_id:
                        manifest["rejected"].append({
                            "title": title,
                            "reason": "Page not found",
                        })
                        continue

                    if page_id in seen_page_ids:
                        logger.info(f"Skipping duplicate page_id {page_id}: {title}")
                        continue

                    # Fetch parse to validate table
                    parse_params = {"page_title": title, "include_sections": True}
                    parse_params_hash = sha256_hash_json(parse_params)
                    safe_title = normalize_title(title)
                    # Use revision_id 0 as placeholder, we'll get real one from response
                    parse_cache_path = settings.scraper_cache_dir / "mediawiki" / safe_title / "0" / "parse"
                    parse_raw_path = parse_cache_path / "raw.json"
                    parse_metadata_path = parse_cache_path / "metadata.json"

                    if not force and parse_raw_path.exists() and parse_metadata_path.exists():
                        with open(parse_raw_path, "r", encoding="utf-8") as f:
                            parse_response = json.load(f)
                        logger.info(f"Cache hit for parse: {title}")
                    else:
                        parse_response = await client.fetch_parse(title, include_sections=True)
                        parse_data = parse_response.get("parse", {})
                        actual_revision_id = parse_data.get("revid", 0)
                        
                        # Save to proper cache location with real revision_id
                        from scraper.cache.mediawiki_cache import get_cache_path
                        actual_cache_path = get_cache_path(title, actual_revision_id, "parse")
                        actual_cache_path.mkdir(parents=True, exist_ok=True)
                        
                        sha256 = sha256_hash_json(parse_response)
                        retrieved_at = utc_now_iso()
                        
                        with open(actual_cache_path / "raw.json", "w", encoding="utf-8") as f:
                            json.dump(parse_response, f, ensure_ascii=False, indent=2)
                        
                        metadata = {
                            "request_params": parse_params,
                            "response_headers": {},
                            "retrieved_at": retrieved_at,
                            "sha256": sha256,
                            "source_url": f"{client.BASE_URL}?action=parse&page={title}",
                            "endpoint_kind": "parse",
                            "page_title": title,
                            "page_id": parse_data.get("pageid", 0),
                            "revision_id": actual_revision_id,
                        }
                        with open(actual_cache_path / "metadata.json", "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=2)
                        
                        logger.info(f"Fetched parse for: {title}")

                    # Validate table
                    parse_data = parse_response.get("parse", {})
                    html = parse_data.get("text", {}).get("*", "")
                    
                    is_valid, reason = validate_member_list_table(html, expected_keywords)
                    
                    if not is_valid:
                        manifest["rejected"].append({
                            "title": title,
                            "reason": reason or "Validation failed",
                        })
                        continue

                    # Create seed
                    seed_key = f"{landtag_entry.key_prefix}{legislature_number}"
                    
                    # Extract time range from title or use defaults
                    # For now, we'll leave it empty and let the user fill it in
                    seed_data: Dict[str, Any] = {
                        "key": seed_key,
                        "page_title": title,
                        "expected_time_range": {
                            "start": "",
                            "end": "",
                        },
                        "hints": {
                            "parliament": landtag_entry.parliament,
                            "state": landtag_entry.state,
                            "legislature_number": legislature_number,
                            "section_keywords": ["Mitglieder", "Abgeordnete"],
                            "expected_table_keywords": expected_keywords,
                        },
                    }

                    if pin_revisions and page_id and revision_id:
                        seed_data["page_id"] = page_id
                        seed_data["revision_id"] = revision_id

                    all_seeds[seed_key] = seed_data
                    seen_page_ids.add(page_id)
                    
                    manifest["validated"].append({
                        "seed_key": seed_key,
                        "title": title,
                        "page_id": page_id,
                        "revision_id": revision_id,
                        "legislature_number": legislature_number,
                    })

                    logger.info(f"✓ Validated seed: {seed_key} - {title}")

                except Exception as e:
                    logger.error(f"Validation failed for {title}: {e}")
                    manifest["rejected"].append({
                        "title": title,
                        "reason": f"Error: {e}",
                    })
                    manifest["errors"].append(f"Validation error for {title}: {e}")
                    continue

        # Write seeds to output file
        if output_path is None:
            output_path = settings.scraper_seeds_landtage_path

        # Ensure output directory exists and is writable
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # If output is in /app/config (read-only), redirect to /data/exports
        if str(output_path).startswith("/app/config"):
            output_path = settings.scraper_export_dir / "seeds_landtage.yaml"
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        import yaml
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(all_seeds, f, allow_unicode=True, sort_keys=True, default_flow_style=False)

        manifest["output_file"] = str(output_path)
        manifest["seed_count"] = len(all_seeds)
        manifest["completed_at"] = utc_now_iso()
        manifest["status"] = "success"

        logger.info(f"✓ Discovery complete: {len(all_seeds)} seeds written to {output_path}")

    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        manifest["errors"].append(str(e))
        manifest["status"] = "error"
        manifest["completed_at"] = utc_now_iso()
        raise

    finally:
        # Save manifest
        manifest_path = settings.scraper_cache_dir / "manifests" / f"discover_{run_id}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    return manifest

