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


class Person(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    name: str = Field(..., description="Full name")
    wikipedia_title: str = Field(..., description="Wikipedia page title")
    wikipedia_url: str = Field(..., description="Wikipedia URL")
    birth_date: Optional[str] = Field(None, description="Birth date (ISO format, only if extracted from hard sources: span.bday or time datetime)")
    birth_date_status: str = Field(default="unknown", description="Birth date extraction status: unknown, extracted, not_present")
    death_date: Optional[str] = Field(None, description="Death date (ISO format)")
    intro: Optional[str] = Field(None, description="Introduction text from Wikipedia")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs (must include both member list and person page if intro present)")
    unstructured_evidence: Optional[List[Dict[str, Any]]] = Field(
        None, description="Unstructured evidence snippets"
    )
    provenance: Optional[Provenance] = Field(None, description="Provenance information")
    data_quality_flags: List[str] = Field(default_factory=list, description="Data quality flags, e.g. ['missing_birth_date']")


class Party(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    name: str = Field(..., description="Party name")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


class Legislature(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    parliament: str = Field(..., description="Parliament name")
    state: str = Field(..., description="State/region")
    number: int = Field(..., description="Legislature number")
    start_date: str = Field(..., description="Start date (ISO format)")
    end_date: str = Field(..., description="End date (ISO format)")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


class Mandate(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    person_id: str = Field(..., description="Person ID")
    legislature_id: Optional[str] = Field(None, description="Legislature ID")
    party_name: Optional[str] = Field(None, description="Party name")
    wahlkreis: Optional[str] = Field(None, description="Electoral district")
    start_date: Optional[str] = Field(None, description="Start date (ISO format)")
    end_date: Optional[str] = Field(None, description="End date (ISO format)")
    role: str = Field(default="member", description="Role in legislature")
    events: List[Event] = Field(default_factory=list, description="Events related to mandate")
    notes: Optional[str] = Field(None, description="Additional notes")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


class Evidence(BaseModel):
    id: str = Field(..., description="Deterministic UUID5 ID")
    endpoint_kind: str = Field(..., description="parse or query")
    page_title: str = Field(..., description="Page title")
    page_id: int = Field(..., description="Page ID")
    revision_id: int = Field(..., description="Revision ID")
    source_url: str = Field(..., description="Source URL")
    retrieved_at: str = Field(..., description="UTC timestamp")
    sha256: str = Field(..., description="SHA256 hash")
    snippet_ref: Optional[str] = Field(None, description="Reference to snippet (CSS selector, row index, etc.)")


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

