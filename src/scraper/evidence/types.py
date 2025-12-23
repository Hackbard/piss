from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ResolvedEvidence(BaseModel):
    evidence_id: str = Field(..., description="Evidence ID")
    source_kind: str = Field(..., description="Source kind: mediawiki, dip, other")
    page_title: Optional[str] = Field(None, description="Page title (for MediaWiki)")
    page_id: Optional[int] = Field(None, description="Page ID (for MediaWiki)")
    revision_id: Optional[int] = Field(None, description="Revision ID (for MediaWiki)")
    retrieved_at_utc: Optional[str] = Field(None, description="UTC timestamp when retrieved")
    sha256: Optional[str] = Field(None, description="SHA256 hash of source data")
    source_url: Optional[str] = Field(None, description="Original source URL")
    canonical_url: str = Field(..., description="Canonical URL (with oldid for Wikipedia)")
    cache_metadata_path: Optional[str] = Field(None, description="Path to metadata.json in cache")
    cache_raw_path: Optional[str] = Field(None, description="Path to raw.json in cache")
    snippet: Optional[str] = Field(None, description="Extracted snippet (cleaned)")
    snippet_source: Optional[str] = Field(None, description="Snippet source: lead_paragraph, table_row, etc.")
    snippet_ref: Optional[Dict[str, Any]] = Field(None, description="Row-level snippet_ref from EvidenceRef (Dict for table_row or None)")
    purpose: Optional[str] = Field(None, description="Purpose of this evidence reference: membership_row, person_page_intro, etc.")

