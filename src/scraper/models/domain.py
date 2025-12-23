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
    birth_date: Optional[str] = Field(None, description="Birth date (ISO format)")
    death_date: Optional[str] = Field(None, description="Death date (ISO format)")
    intro: Optional[str] = Field(None, description="Introduction text from Wikipedia")
    evidence_ids: List[str] = Field(default_factory=list, description="Evidence IDs")
    unstructured_evidence: Optional[List[Dict[str, Any]]] = Field(
        None, description="Unstructured evidence snippets"
    )
    provenance: Optional[Provenance] = Field(None, description="Provenance information")


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

