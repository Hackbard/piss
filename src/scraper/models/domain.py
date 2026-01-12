from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Provenance(BaseModel):
    evidence_ids: List[str] = Field(default_factory=list, description="List of evidence IDs")
    source_page_title: str = Field(..., description="Source Wikipedia page title")
    source_page_id: int = Field(..., description="Source Wikipedia page ID")
    revision_id: int = Field(..., description="Revision ID")
    retrieved_at: str = Field(..., description="UTC timestamp when retrieved")
    sha256: str = Field(..., description="SHA256 hash of source data")


class Event(BaseModel):
    event_type: str = Field(..., description="Type of event")
    description: str = Field(..., description="Event description")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs supporting this event")
    date: Optional[str] = Field(None, description="Event date (ISO format)")


class EvidenceRef(BaseModel):
    """Entity-level reference to Evidence with optional row-level snippet reference."""
    evidence_id: str = Field(..., description="Evidence ID (page-level)")
    snippet_ref: Optional[Dict[str, Any]] = Field(None, description="Row-level snippet reference (e.g. table_row with table_index, row_index)")
    purpose: Optional[str] = Field(None, description="Purpose of this evidence reference: membership_row, person_page_intro, etc.")
    confidence: Optional[float] = Field(None, description="Confidence score 0..1")
    created_at: Optional[str] = Field(None, description="Creation timestamp (UTC ISO)")


class Person(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    name: str = Field(..., description="Full name")
    wikipedia_title: Optional[str] = Field(None, description="Wikipedia page title")
    wikipedia_url: Optional[str] = Field(None, description="Wikipedia URL")
    normalized_name: Optional[str] = Field(None, description="Normalized name for deduplication")
    birth_date: Optional[str] = Field(None, description="Birth date (ISO format, only if extracted from hard sources: span.bday or time datetime)")
    birth_date_status: str = Field(default="unknown", description="Birth date extraction status: unknown, extracted, not_present")
    death_date: Optional[str] = Field(None, description="Death date (ISO format)")
    intro: Optional[str] = Field(None, description="Introduction text from Wikipedia")
    evidence_refs: List[EvidenceRef] = Field(default_factory=list, description="Entity-level evidence references with row-level snippet_refs")
    evidence_ids: List[str] = Field(default_factory=list, description="Legacy: Evidence IDs (derived from evidence_refs for backward compatibility)")
    unstructured_evidence: Optional[List[Dict[str, Any]]] = Field(
        None, description="Unstructured evidence snippets"
    )
    provenance: Optional[Provenance] = Field(None, description="Provenance information")
    data_quality_flags: List[str] = Field(default_factory=list, description="Data quality flags, e.g. ['missing_birth_date']")
    
    def model_post_init(self, __context: Any) -> None:
        """Derive evidence_ids from evidence_refs if not set (backward compatibility)."""
        if not self.evidence_ids and self.evidence_refs:
            self.evidence_ids = list(set([ref.evidence_id for ref in self.evidence_refs]))


class Parliament(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    name: str = Field(..., description="Parliament name")
    level: str = Field(..., description="Level: federal or state")
    state_code: Optional[str] = Field(None, description="State code (e.g. 'NI' for Niedersachsen)")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


class Party(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    code: str = Field(..., description="Party code (e.g. 'SPD', 'CDU')")
    name: str = Field(..., description="Party name")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


class Legislature(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    parliament_id: str = Field(..., description="Parliament ID")
    name: str = Field(..., description="Legislature name (e.g. '17. Landtag Niedersachsen')")
    start_date: Optional[str] = Field(None, description="Start date (ISO format YYYY-MM-DD, or None)")
    end_date: Optional[str] = Field(None, description="End date (ISO format YYYY-MM-DD, or None = offen)")
    start_date_raw: Optional[str] = Field(None, description="Original raw start_date value if normalization failed")
    end_date_raw: Optional[str] = Field(None, description="Original raw end_date value if normalization failed")
    start_date_source: Optional[str] = Field(None, description="Source of start_date (e.g. 'wikipedia_list')")
    end_date_source: Optional[str] = Field(None, description="Source of end_date (e.g. 'wikipedia_list')")
    source_url: Optional[str] = Field(None, description="Evidence URL for the legislature list page (canonical with oldid)")
    wikipedia_title: Optional[str] = Field(None, description="Wikipedia page title for the legislature list page")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


class Mandate(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    person_id: str = Field(..., description="Person ID")
    parliament_id: str = Field(..., description="Parliament ID")
    legislature_id: str = Field(..., description="Legislature ID")
    party_code: Optional[str] = Field(None, description="Party code (e.g. 'SPD', 'CDU')")
    start_date: Optional[str] = Field(None, description="Start date (ISO format YYYY-MM-DD, or None)")
    end_date: Optional[str] = Field(None, description="End date (ISO format YYYY-MM-DD, or None = offen)")
    start_date_raw: Optional[str] = Field(None, description="Original raw start_date value if normalization failed")
    end_date_raw: Optional[str] = Field(None, description="Original raw end_date value if normalization failed")
    start_date_source: Optional[str] = Field(None, description="Source of start_date (e.g. 'legislature', 'person')")
    end_date_source: Optional[str] = Field(None, description="Source of end_date (e.g. 'legislature', 'person')")
    role: Optional[str] = Field(None, description="Role (e.g. 'MdL', 'MdB')")
    wahlkreis: Optional[str] = Field(None, description="Electoral district")
    events: List[Event] = Field(default_factory=list, description="Events related to mandate")
    notes: Optional[str] = Field(None, description="Additional notes")
    evidence_refs: List[EvidenceRef] = Field(default_factory=list, description="Entity-level evidence references (preferred: membership_row with table_row snippet_ref)")
    evidence_ids: List[str] = Field(default_factory=list, description="Legacy: Evidence IDs (derived from evidence_refs for backward compatibility)")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")
    
    def model_post_init(self, __context: Any) -> None:
        """Derive evidence_ids from evidence_refs if not set (backward compatibility)."""
        if not self.evidence_ids and self.evidence_refs:
            self.evidence_ids = list(set([ref.evidence_id for ref in self.evidence_refs]))


class Evidence(BaseModel):
    """Page-level Evidence (immutable, represents entire page/response)."""
    id: str = Field(..., description="Deterministic UUID5 ID")
    url: str = Field(..., description="Source URL")
    retrieved_at: str = Field(..., description="Retrieval timestamp (UTC ISO)")
    content_hash: str = Field(..., description="Content hash (SHA256)")
    source_type: Optional[str] = Field(None, description="Source type (e.g. 'wikipedia', 'parliament_site', 'dip')")
    locator: Optional[str] = Field(None, description="Locator (e.g. section/table_row/selector)")
    snapshot_path: Optional[str] = Field(None, description="Path to cached HTML/JSON snapshot")
    endpoint_kind: Optional[str] = Field(None, description="Legacy: parse or query")
    page_title: Optional[str] = Field(None, description="Legacy: Page title")
    page_id: Optional[int] = Field(None, description="Legacy: Page ID")
    revision_id: Optional[int] = Field(None, description="Legacy: Revision ID")
    source_url: Optional[str] = Field(None, description="Legacy: Source URL")
    sha256: Optional[str] = Field(None, description="Legacy: SHA256 hash")
    # snippet_ref removed: Evidence is page-level, row-level refs belong to EvidenceRef


class LegislatureMember(BaseModel):
    seed_key: str = Field(..., description="Seed key")
    page_title: str = Field(..., description="Page title")
    page_id: int = Field(..., description="Page ID")
    revision_id: int = Field(..., description="Revision ID")
    members: List[tuple[Person, Mandate]] = Field(..., description="List of (Person, Mandate) tuples")
    evidence_id: str = Field(..., description="Main evidence ID")


class CanonicalPerson(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    display_name: str = Field(..., description="Display name")
    identifiers: Dict[str, Optional[str]] = Field(
        default_factory=dict,
        description="Identifiers: wikipedia_title, wikipedia_page_id, dip_person_id",
    )
    created_at: Optional[str] = Field(None, description="Creation timestamp (UTC ISO)")
    updated_at: Optional[str] = Field(None, description="Update timestamp (UTC ISO)")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Dict[str, Any]] = Field(None, description="Provenance summary")


class WikipediaPersonRecord(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID based on wikipedia_title + revision")
    wikipedia_title: str = Field(..., description="Wikipedia page title")
    wikipedia_url: str = Field(..., description="Wikipedia URL")
    page_id: int = Field(..., description="Wikipedia page ID")
    revision_id: int = Field(..., description="Revision ID")
    name: str = Field(..., description="Extracted name")
    birth_date: Optional[str] = Field(None, description="Birth date (ISO format)")
    death_date: Optional[str] = Field(None, description="Death date (ISO format)")
    intro: Optional[str] = Field(None, description="Introduction text")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Dict[str, Any]] = Field(None, description="Provenance information")


class DipPersonRecord(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID based on dip_person_id + payload sha256")
    dip_person_id: int = Field(..., description="DIP person ID")
    vorname: Optional[str] = Field(None, description="First name")
    nachname: Optional[str] = Field(None, description="Last name")
    namenszusatz: Optional[str] = Field(None, description="Name suffix")
    titel: Optional[str] = Field(None, description="Title")
    fraktion: Optional[str] = Field(None, description="Party/Fraktion")
    wahlperiode: List[int] = Field(default_factory=list, description="Wahlperioden")
    person_roles: Optional[List[Dict[str, Any]]] = Field(None, description="Person roles")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Dict[str, Any]] = Field(None, description="Provenance information")


class PersonLinkAssertion(BaseModel):
    id: str = Field(
        ...,
        description="Deterministic UUID5 ID based on wikipedia record + dip record + ruleset_version",
    )
    wikipedia_person_ref: str = Field(..., description="Wikipedia person record ID or title")
    dip_person_ref: str = Field(..., description="DIP person ID (as string)")
    ruleset_version: str = Field(default="ruleset_v1", description="Ruleset version")
    method: str = Field(..., description="override or ruleset")
    score: float = Field(..., description="Match score 0..1")
    status: str = Field(..., description="accepted, pending, or rejected")
    reason: Optional[str] = Field(None, description="Reason for status")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs from both sides")
    created_at: str = Field(..., description="Creation timestamp (UTC ISO)")

