from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ParliamentType(StrEnum):
    """High-level parliament types in scope (Germany-only)."""

    BUND = "bund"
    LAND = "land"
    BUNDESRAT = "bundesrat"


class DatePrecision(StrEnum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    UNKNOWN = "unknown"


class SourceSystem(StrEnum):
    """Source systems for ingestion and provenance."""

    WIKIDATA = "wikidata"
    WIKIPEDIA = "wikipedia"
    DIP = "dip"  # Deutscher Bundestag (DIP API)
    OFFICIAL_OTHER = "official_other"


class TimeInterval(BaseModel):
    """Inclusive interval; `end_date=None` means open-ended."""

    model_config = ConfigDict(extra="forbid")

    start_date: date | None = None
    end_date: date | None = None
    start_date_raw: str | None = None
    end_date_raw: str | None = None
    start_date_precision: DatePrecision = DatePrecision.UNKNOWN
    end_date_precision: DatePrecision = DatePrecision.UNKNOWN

    @model_validator(mode="after")
    def validate_order(self) -> TimeInterval:
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be >= start_date")
        return self


class ExternalPersonIds(BaseModel):
    """Aggregated external identifiers for a canonical person."""

    model_config = ConfigDict(extra="forbid")

    wikidata_qid: str | None = Field(None, description="Wikidata QID, e.g. Q12345")
    wikipedia_pageid: int | None = Field(None, description="Wikipedia pageid (MediaWiki)")
    wikipedia_title: str | None = Field(None, description="Wikipedia page title (normalized)")
    dip_person_id: int | None = Field(None, description="DIP person id (Bundestag)")
    bundes_source_id: str | None = Field(None, description="Generic ID for additional official federal sources")


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
    raw_snapshot_path: str | None = Field(None, description="Path to raw snapshot (for reproducibility)")
    normalized_snapshot_path: str | None = Field(None, description="Path to normalized snapshot (for reproducibility)")
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
    state_code: str | None = Field(None, description="Required for Landtag; null for Bund/Bundesrat.")
    term_number: int | None = Field(None, description="Wahlperiode / term number when applicable")
    name: str
    election_date: date | None = None
    interval: TimeInterval = Field(default_factory=TimeInterval)
    sources: list[PersonSource] = Field(default_factory=list, description="Provenance for this period")


class Membership(BaseModel):
    """Parliament membership / mandate-like record with time range and context."""

    model_config = ConfigDict(extra="forbid")

    pis_membership_id: str
    pis_person_id: str

    parliament_type: ParliamentType
    parliament_code: str
    state_code: str | None = None

    legislature_period_id: str | None = Field(None, description="Links to LegislaturePeriod when known")

    party_code: str | None = None
    party_name: str | None = None
    faction_name: str | None = None
    role: str | None = Field(None, description="E.g. MdB, MdL, member, chair, etc.")

    interval: TimeInterval = Field(default_factory=TimeInterval)
    sources: list[PersonSource] = Field(default_factory=list)


class OfficeRole(BaseModel):
    """Executive/government office held by a person (minister, head of government, etc.)."""

    model_config = ConfigDict(extra="forbid")

    pis_office_role_id: str
    pis_person_id: str

    level: str = Field(..., description="federal|state")
    state_code: str | None = None

    office_title: str = Field(..., description="Office title, e.g. Bundesminister der Finanzen")
    portfolio: str | None = Field(None, description="Optional portfolio/department")
    interval: TimeInterval = Field(default_factory=TimeInterval)
    sources: list[PersonSource] = Field(default_factory=list)


class Person(BaseModel):
    """Canonical person record: exactly one per real person (dedup enforced by reconcile layer)."""

    model_config = ConfigDict(extra="forbid")

    pis_person_id: str = Field(..., description="Internal canonical person id (unique)")

    display_name: str
    aliases: list[str] = Field(default_factory=list)

    birth_date: date | None = None
    death_date: date | None = None
    birth_place: str | None = None

    external_ids: ExternalPersonIds = Field(default_factory=ExternalPersonIds)
    sources: list[PersonSource] = Field(default_factory=list, description="Must contain at least one source")

    memberships: list[Membership] = Field(default_factory=list)
    office_roles: list[OfficeRole] = Field(default_factory=list)

    persona_summary: str | None = Field(None, description="LLM-friendly, factual summary (built in rag layer)")
    facts: dict[str, Any] = Field(default_factory=dict, description="Additional structured facts")

    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_sources_non_empty(self) -> Person:
        if not self.sources:
            raise ValueError("Person must have at least one PersonSource")
        return self

