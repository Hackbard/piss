from datetime import date, datetime
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class SortField(str, Enum):
    PERSON_NAME = "person_name"
    START_DATE = "start_date"
    END_DATE = "end_date"
    PARTY_CODE = "party_code"


class SortDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class MandateSearchRequest(BaseModel):
    parliament_id: Optional[str] = None
    legislature_id: Optional[str] = None
    party_code: Optional[str] = None
    from_date: Optional[date] = None
    to_date: Optional[date] = None
    person_id: Optional[str] = None
    person_name_contains: Optional[str] = None
    limit: int = Field(default=200, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    sort: SortField = Field(default=SortField.PERSON_NAME)
    sort_dir: SortDirection = Field(default=SortDirection.ASC, alias="sort_dir")
    strict_evidence: bool = Field(default=True)

    @field_validator("party_code")
    @classmethod
    def normalize_party_code(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return v.upper()
        return v

    @field_validator("to_date")
    @classmethod
    def validate_date_range(cls, v: Optional[date], info) -> Optional[date]:
        from_date = info.data.get("from_date") if hasattr(info, "data") else None
        if from_date and v and v < from_date:
            raise ValueError("to_date must be >= from_date")
        return v

    model_config = {"populate_by_name": True}


class LegislatureStatsRequest(BaseModel):
    legislature_id: str
    strict_evidence: bool = Field(default=True)


class PersonLookupRequest(BaseModel):
    person_id: Optional[str] = None
    name_contains: Optional[str] = None
    limit: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def validate_one_of(self):
        if not self.person_id and not self.name_contains:
            raise ValueError("Must specify either person_id or name_contains")
        return self


class ToolMeta(BaseModel):
    tool: str
    executed_at: datetime
    request_id: UUID
    result_hash: Optional[str] = None
    data_version: Optional[str] = None
    warnings: list[str] = Field(default_factory=list)
    active_only: Optional[bool] = None
    as_of: Optional[str] = None
    coverage_degraded: Optional[bool] = None
    excluded_due_to_missing_start_date_count: Optional[int] = None
    excluded_due_to_missing_legislature_start_date_count: Optional[int] = None


class MandateRowResponse(BaseModel):
    person_id: str
    person_name: str
    wikipedia_title: Optional[str] = None
    mandate_id: str
    parliament_id: str
    legislature_id: str
    legislature: str
    start_date: date
    end_date: Optional[date] = None
    party_code: Optional[str] = None
    evidence_urls: list[str] = Field(default_factory=list)


class MandateSearchResponse(BaseModel):
    meta: ToolMeta
    applied_filter: dict
    total: Optional[int] = None
    rows: list[MandateRowResponse]


class LegislatureStatsResponse(BaseModel):
    meta: ToolMeta
    legislature_id: str
    legislature_name: str
    parliament_id: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    total_seats: Optional[int] = None
    party_seats: dict[str, int] = Field(default_factory=dict)
    party_vote_share: dict[str, float] = Field(default_factory=dict)
    evidence_urls: list[str] = Field(default_factory=list)


class PersonResponse(BaseModel):
    person_id: str
    name: str
    wikipedia_title: Optional[str] = None
    wikipedia_url: Optional[str] = None
    birth_date: Optional[date] = None
    death_date: Optional[date] = None
    intro: Optional[str] = None
    evidence_urls: list[str] = Field(default_factory=list)


class PersonLookupResponse(BaseModel):
    meta: ToolMeta
    persons: list[PersonResponse]


class ParliamentCoverageRequest(BaseModel):
    parliament_ids: Optional[list[str]] = Field(default=None, description="Optional list of parliament_ids to filter. If None or empty, returns all.")


class ParliamentCoverageRowResponse(BaseModel):
    parliament_id: str
    mandates_count: int
    min_start: Optional[str] = None
    max_end: Optional[str] = None
    invalid_start_count: int = 0
    invalid_end_count: int = 0
    missing_evidence_count: int = 0


class ParliamentCoverageResponse(BaseModel):
    meta: ToolMeta
    applied_filter: dict
    rows: list[ParliamentCoverageRowResponse]


class ErrorResponse(BaseModel):
    error: str
    error_code: str
    request_id: Optional[UUID] = None
    details: Optional[dict] = None

