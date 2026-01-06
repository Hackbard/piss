from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class SortField(str, Enum):
    PERSON_NAME = "person_name"
    START_DATE = "start_date"
    END_DATE = "end_date"
    PARTY_CODE = "party_code"


class SortDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class MandateQueryFilter(BaseModel):
    parliament_id: Optional[str] = Field(None, description="Parliament ID filter")
    legislature_id: Optional[str] = Field(None, description="Legislature ID filter")
    party_code: Optional[str] = Field(None, description="Party code filter (e.g. 'SPD', 'CDU')")
    from_date: Optional[date] = Field(None, description="Start date filter (inclusive)")
    to_date: Optional[date] = Field(None, description="End date filter (inclusive)")
    person_id: Optional[str] = Field(None, description="Person ID filter")
    person_name_contains: Optional[str] = Field(None, description="Person name contains filter (case-insensitive)")
    limit: int = Field(default=200, ge=1, le=1000, description="Maximum number of results")
    offset: int = Field(default=0, ge=0, description="Offset for pagination")
    sort: SortField = Field(default=SortField.PERSON_NAME, description="Sort field")
    sort_direction: SortDirection = Field(default=SortDirection.ASC, description="Sort direction")

    @field_validator("from_date", "to_date", mode="before")
    @classmethod
    def parse_date(cls, v: str | date | None) -> date | None:
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            try:
                return date.fromisoformat(v)
            except ValueError:
                raise ValueError(f"Invalid date format: {v}. Expected YYYY-MM-DD")
        return None

    @field_validator("to_date")
    @classmethod
    def validate_date_range(cls, v: Optional[date], info) -> Optional[date]:
        from_date = info.data.get("from_date") if hasattr(info, "data") else None
        if from_date and v and v < from_date:
            raise ValueError("to_date must be >= from_date")
        return v


class MandateRow(BaseModel):
    person_id: str = Field(..., description="Person ID")
    person_name: str = Field(..., description="Person full name")
    wikipedia_title: Optional[str] = Field(None, description="Wikipedia page title")
    mandate_id: str = Field(..., description="Mandate ID")
    legislature_id: str = Field(..., description="Legislature ID")
    legislature_name: Optional[str] = Field(None, description="Legislature name")
    parliament_id: str = Field(..., description="Parliament ID")
    start_date: date = Field(..., description="Mandate start date")
    end_date: Optional[date] = Field(None, description="Mandate end date (None = open-ended)")
    party_code: Optional[str] = Field(None, description="Party code")
    evidence_urls: list[str] = Field(default_factory=list, description="Deduplicated evidence URLs (never null, may be empty)")

    @field_validator("evidence_urls", mode="before")
    @classmethod
    def normalize_evidence_urls(cls, v: list[str] | None) -> list[str]:
        if v is None:
            return []
        return sorted(list(set(url for url in v if url)))


class MandateQueryResult(BaseModel):
    rows: list[MandateRow] = Field(default_factory=list, description="Query results")
    total: Optional[int] = Field(None, description="Total count (if available, None if not computed)")
    applied_filter: MandateQueryFilter = Field(..., description="Applied filter (normalized)")


class LegislatureStats(BaseModel):
    legislature_id: str = Field(..., description="Legislature ID")
    legislature_name: str = Field(..., description="Legislature name")
    total_seats: Optional[int] = Field(None, description="Total seats (if available)")
    party_seats: dict[str, int] = Field(default_factory=dict, description="Party code -> seat count")
    party_vote_share: dict[str, float] = Field(default_factory=dict, description="Party code -> vote share (0.0-1.0)")
    evidence_urls: list[str] = Field(default_factory=list, description="Deduplicated evidence URLs for statistics source")

    @field_validator("evidence_urls", mode="before")
    @classmethod
    def normalize_evidence_urls(cls, v: list[str] | None) -> list[str]:
        if v is None:
            return []
        return sorted(list(set(url for url in v if url)))


class PersonDTO(BaseModel):
    person_id: str = Field(..., description="Person ID")
    name: str = Field(..., description="Full name")
    wikipedia_title: Optional[str] = Field(None, description="Wikipedia page title")
    wikipedia_url: Optional[str] = Field(None, description="Wikipedia URL")
    birth_date: Optional[date] = Field(None, description="Birth date")
    death_date: Optional[date] = Field(None, description="Death date")
    intro: Optional[str] = Field(None, description="Introduction text")
    evidence_urls: list[str] = Field(default_factory=list, description="Deduplicated evidence URLs")

    @field_validator("evidence_urls", mode="before")
    @classmethod
    def normalize_evidence_urls(cls, v: list[str] | None) -> list[str]:
        if v is None:
            return []
        return sorted(list(set(url for url in v if url)))

