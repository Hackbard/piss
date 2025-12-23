import json
from pathlib import Path
from typing import Any, Dict, Optional

from scraper.config import get_settings
from scraper.utils.ids import generate_evidence_id

settings = get_settings()


def get_evidence_index_path() -> Path:
    """Get path to evidence index file."""
    index_dir = settings.scraper_cache_dir / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    return index_dir / "evidence_index.jsonl"


def update_evidence_index(
    evidence_id: str,
    source_kind: str,
    cache_metadata_path: Path,
    cache_raw_path: Path,
    page_title: Optional[str] = None,
    page_id: Optional[int] = None,
    revision_id: Optional[int] = None,
    sha256: Optional[str] = None,
    endpoint: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Update evidence index with new entry (idempotent: overwrites if exists).
    
    Writes to evidence_index.jsonl (JSONL format, one entry per line).
    
    Note: snippet_ref is no longer stored in evidence index (Evidence is page-level).
    Row-level snippet_refs are stored in EvidenceRef on entities (Person, Mandate, etc.).
    """
    index_path = get_evidence_index_path()
    
    entry = {
        "evidence_id": evidence_id,
        "source_kind": source_kind,
        "cache_metadata_path": str(cache_metadata_path),
        "cache_raw_path": str(cache_raw_path),
        "page_title": page_title,
        "page_id": page_id,
        "revision_id": revision_id,
        "sha256": sha256,
        "endpoint": endpoint,
        "params": params,
    }
    
    # Load existing index
    existing = {}
    if index_path.exists():
        with open(index_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                    existing[e.get("evidence_id")] = e
                except json.JSONDecodeError:
                    continue
    
    # Update entry (idempotent: overwrites)
    existing[evidence_id] = entry
    
    # Write back
    with open(index_path, "w", encoding="utf-8") as f:
        for e in existing.values():
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

