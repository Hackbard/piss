import json
from pathlib import Path
from typing import Dict, List, Optional

from scraper.config import get_settings
from scraper.evidence.types import ResolvedEvidence
from scraper.evidence.snippets import extract_snippet
from scraper.utils.url import build_wikipedia_canonical_url, build_dip_canonical_url

settings = get_settings()


def load_evidence_index() -> Dict[str, Dict[str, any]]:
    """
    Load evidence index from /data/cache/index/evidence_index.jsonl
    
    Returns: dict mapping evidence_id -> index entry
    """
    index_path = settings.scraper_cache_dir / "index" / "evidence_index.jsonl"
    
    if not index_path.exists():
        return {}
    
    index = {}
    with open(index_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                evidence_id = entry.get("evidence_id")
                if evidence_id:
                    index[evidence_id] = entry
            except json.JSONDecodeError:
                continue
    
    return index


def scan_cache_for_evidence_id(evidence_id: str) -> Optional[Dict[str, any]]:
    """
    Best-effort scan of cache to find evidence_id.
    This is slow and should only be used as fallback when index lookup fails.
    """
    from scraper.config import get_settings
    from scraper.utils.ids import generate_evidence_id
    from scraper.utils.hashing import sha256_hash_json
    
    settings = get_settings()
    cache_dir = settings.scraper_cache_dir / "mediawiki"
    
    if not cache_dir.exists():
        return None
    
    # Scan mediawiki cache directories
    for page_dir in cache_dir.iterdir():
        if not page_dir.is_dir():
            continue
        
        for revision_dir in page_dir.iterdir():
            if not revision_dir.is_dir():
                continue
            
            parse_dir = revision_dir / "parse"
            if not parse_dir.exists():
                continue
            
            metadata_path = parse_dir / "metadata.json"
            raw_path = parse_dir / "raw.json"
            
            if not metadata_path.exists() or not raw_path.exists():
                continue
            
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                
                with open(raw_path, "r", encoding="utf-8") as rf:
                    raw_data = json.load(rf)
                
                page_id = metadata.get("page_id", 0)
                revision_id = metadata.get("revision_id", 0)
                
                # Always compute sha256 from raw data for consistency
                sha256 = sha256_hash_json(raw_data)
                
                # Generate evidence_id and check if it matches
                computed_id = generate_evidence_id(page_id, revision_id, "parse", sha256)
                if computed_id == evidence_id:
                    return {
                        "evidence_id": evidence_id,
                        "source_kind": "mediawiki",
                        "cache_metadata_path": str(metadata_path),
                        "cache_raw_path": str(raw_path),
                        "page_title": metadata.get("page_title"),
                        "page_id": page_id,
                        "revision_id": revision_id,
                        "sha256": sha256,
                    }
            except (IOError, json.JSONDecodeError, KeyError) as e:
                continue
    
    return None


def resolve_from_file_cache(
    evidence_id: str,
    with_snippet: bool = False,
    snippet_max_len: int = 500,
    prefer_snippet: str = "table_row",
    snippet_ref: Optional[Dict[str, Any]] = None,  # Row-level snippet_ref from EvidenceRef
    purpose: Optional[str] = None,  # Purpose from EvidenceRef
) -> Optional[ResolvedEvidence]:
    """
    Resolve evidence from file cache using evidence index.
    Falls back to cache scan if not found in index.
    """
    index = load_evidence_index()
    entry = index.get(evidence_id)
    
    if not entry:
        # Fallback: best-effort cache scan (slow, but works for old data)
        entry = scan_cache_for_evidence_id(evidence_id)
        if not entry:
            return None
    
    source_kind = entry.get("source_kind", "other")
    cache_metadata_path = entry.get("cache_metadata_path")
    cache_raw_path = entry.get("cache_raw_path")
    
    if not cache_metadata_path or not Path(cache_metadata_path).exists():
        return None
    
    # Load metadata
    try:
        with open(cache_metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except (IOError, json.JSONDecodeError):
        return None
    
    page_title = entry.get("page_title") or metadata.get("page_title")
    page_id = entry.get("page_id") or metadata.get("page_id")
    revision_id = entry.get("revision_id") or metadata.get("revision_id")
    retrieved_at_utc = metadata.get("retrieved_at")
    sha256 = entry.get("sha256") or metadata.get("sha256")
    source_url = metadata.get("url")
    
    # Use snippet_ref from parameter (EvidenceRef) if provided, otherwise fallback to legacy entry
    # Note: Evidence index no longer stores snippet_ref (page-level), but legacy entries may have it
    effective_snippet_ref = snippet_ref if snippet_ref is not None else entry.get("snippet_ref")
    
    # Build canonical URL
    if source_kind == "mediawiki":
        canonical_url = build_wikipedia_canonical_url(page_title or "", revision_id)
    elif source_kind == "dip":
        endpoint = entry.get("endpoint") or metadata.get("endpoint", "")
        params = entry.get("params") or metadata.get("request_params", {})
        canonical_url = build_dip_canonical_url(endpoint, params)
    else:
        canonical_url = source_url or ""
    
    # Extract snippet if requested
    snippet = None
    snippet_source = None
    if with_snippet and cache_raw_path and Path(cache_raw_path).exists():
        try:
            with open(cache_raw_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            
            html = None
            if source_kind == "mediawiki":
                html = raw_data.get("parse", {}).get("text", {}).get("*", "")
            
            # snippet_ref can be Dict (new format) or string (legacy)
            snippet, snippet_source = extract_snippet(html, effective_snippet_ref, snippet_max_len, prefer=prefer_snippet)
        except (IOError, json.JSONDecodeError, KeyError):
            pass
    
    return ResolvedEvidence(
        evidence_id=evidence_id,
        source_kind=source_kind,
        page_title=page_title,
        page_id=page_id,
        revision_id=revision_id,
        retrieved_at_utc=retrieved_at_utc,
        sha256=sha256,
        source_url=source_url,
        canonical_url=canonical_url,
        cache_metadata_path=str(cache_metadata_path) if cache_metadata_path else None,
        cache_raw_path=str(cache_raw_path) if cache_raw_path else None,
        snippet=snippet,
        snippet_source=snippet_source,
        snippet_ref=effective_snippet_ref,  # From EvidenceRef (parameter) or legacy entry
        purpose=purpose,  # From EvidenceRef
    )

