from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ParliamentType(str, Enum):
    """High-level parliament types in scope (Germany-only)."""

    BUND = "bund"
    LAND = "land"
    BUNDESRAT = "bundesrat"


class DatePrecision(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    UNKNOWN = "unknown"


class SourceSystem(str, Enum):
    """Source systems for ingestion and provenance."""

    WIKIDATA = "wikidata"
    WIKIPEDIA = "wikipedia"
    DIP = "dip"  # Deutscher Bundestag (DIP API)
    OFFICIAL_OTHER = "official_other"


class TimeInterval(BaseModel):
    """Inclusive interval; `end_date=None` means open-ended."""

    model_config = ConfigDict(extra="forbid")

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    start_date_raw: Optional[str] = None
    end_date_raw: Optional[str] = None
    start_date_precision: DatePrecision = DatePrecision.UNKNOWN
    end_date_precision: DatePrecision = DatePrecision.UNKNOWN

    @model_validator(mode="after")
    def validate_order(self) -> "TimeInterval":
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be >= start_date")
        return self


class ExternalPersonIds(BaseModel):
    """Aggregated external identifiers for a canonical person."""

    model_config = ConfigDict(extra="forbid")

    wikidata_qid: Optional[str] = Field(None, description="Wikidata QID, e.g. Q12345")
    wikipedia_pageid: Optional[int] = Field(None, description="Wikipedia pageid (MediaWiki)")
    wikipedia_title: Optional[str] = Field(None, description="Wikipedia page title (normalized)")
    dip_person_id: Optional[int] = Field(None, description="DIP person id (Bundestag)")
    bundes_source_id: Optional[str] = Field(
        None, description="Generic ID for additional official federal sources"
    )


class PersonSource(BaseModel):
    """Per-source provenance record for a canonical person.

    Stores how/when the source record was fetched and how it maps to the canonical person.
    """

    model_config = ConfigDict(extra="forbid")

    source_system: SourceSystem
    source_person_id: str = Field(
        ..., description="Native source identifier (stringified), e.g. QID/pageid/dip_person_id"
    )
    fetched_at: datetime = Field(..., description="UTC timestamp when fetched")
    source_urls: list[str] = Field(default_factory=list, description="Canonical URLs (oldid/revision-pinned when possible)")
    raw_snapshot_path: Optional[str] = Field(
        None, description="Path to raw snapshot (for reproducibility)"
    )
    normalized_snapshot_path: Optional[str] = Field(
        None, description="Path to normalized snapshot (for reproducibility)"
    )
    extra: dict[str, Any] = Field(default_factory=dict, description="Source-specific metadata")


class LegislaturePeriod(BaseModel):
    """Legislature/term (Wahlperiode/Legislaturperiode) in scope."""

    model_config = ConfigDict(extra="forbid")

    pis_legislature_period_id: str
    parliament_type: ParliamentType
    parliament_code: str = Field(
        ...,
        description="Stable parliament code (e.g. BT for Bundestag, BR for Bundesrat, state code for Landtag).",
    )
    state_code: Optional[str] = Field(
        None, description="Required for Landtag; null for Bund/Bundesrat."
    )
    term_number: Optional[int] = Field(None, description="Wahlperiode / term number when applicable")
    name: str
    election_date: Optional[date] = None
    interval: TimeInterval = Field(default_factory=TimeInterval)
    sources: list[PersonSource] = Field(default_factory=list, description="Provenance for this period")


class Membership(BaseModel):
    """Parliament membership / mandate-like record with time range and context."""

    model_config = ConfigDict(extra="forbid")

    pis_membership_id: str
    pis_person_id: str

    parliament_type: ParliamentType
    parliament_code: str
    state_code: Optional[str] = None

    legislature_period_id: Optional[str] = Field(
        None, description="Links to LegislaturePeriod when known"
    )

    party_code: Optional[str] = None
    party_name: Optional[str] = None
    faction_name: Optional[str] = None
    role: Optional[str] = Field(None, description="E.g. MdB, MdL, member, chair, etc.")

    interval: TimeInterval = Field(default_factory=TimeInterval)
    sources: list[PersonSource] = Field(default_factory=list)


class OfficeRole(BaseModel):
    """Executive/government office held by a person (minister, head of government, etc.)."""

    model_config = ConfigDict(extra="forbid")

    pis_office_role_id: str
    pis_person_id: str

    level: str = Field(..., description="federal|state")
    state_code: Optional[str] = None

    office_title: str = Field(..., description="Office title, e.g. Bundesminister der Finanzen")
    portfolio: Optional[str] = Field(None, description="Optional portfolio/department")
    interval: TimeInterval = Field(default_factory=TimeInterval)
    sources: list[PersonSource] = Field(default_factory=list)


class Person(BaseModel):
    """Canonical person record: exactly one per real person (dedup enforced by reconcile layer)."""

    model_config = ConfigDict(extra="forbid")

    pis_person_id: str = Field(..., description="Internal canonical person id (unique)")

    display_name: str
    aliases: list[str] = Field(default_factory=list)

    birth_date: Optional[date] = None
    death_date: Optional[date] = None
    birth_place: Optional[str] = None

    external_ids: ExternalPersonIds = Field(default_factory=ExternalPersonIds)
    sources: list[PersonSource] = Field(default_factory=list, description="Must contain at least one source")

    memberships: list[Membership] = Field(default_factory=list)
    office_roles: list[OfficeRole] = Field(default_factory=list)

    persona_summary: Optional[str] = Field(
        None, description="LLM-friendly, factual summary (built in rag layer)"
    )
    facts: dict[str, Any] = Field(default_factory=dict, description="Additional structured facts")

    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_sources_non_empty(self) -> "Person":
        if not self.sources:
            raise ValueError("Person must have at least one PersonSource")
        return self

