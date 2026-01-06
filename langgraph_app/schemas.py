"""Pydantic models for Intent, Tool requests/responses."""

from datetime import date
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class IntentType(str, Enum):
    """Intent types for user queries."""

    MANDATES_LIST = "MANDATES_LIST"
    LEGISLATURE_STATS = "LEGISLATURE_STATS"
    COMBINED_MANDATES_AND_STATS = "COMBINED_MANDATES_AND_STATS"
    PERSON_LOOKUP = "PERSON_LOOKUP"


class GroupBy(str, Enum):
    """Grouping options for results."""

    NONE = "NONE"
    LEGISLATURE = "LEGISLATURE"
    PARTY = "PARTY"


class OutputFormat(str, Enum):
    """Output format options."""

    BULLETS = "BULLETS"
    TABLE = "TABLE"
    JSON = "JSON"


class Metric(str, Enum):
    """Available metrics for computation."""

    SEAT_SHARE_PERCENT = "SEAT_SHARE_PERCENT"
    TOTAL_SEATS = "TOTAL_SEATS"
    PARTY_COUNT = "PARTY_COUNT"


class UserIntent(BaseModel):
    """Structured intent parsed from user query."""

    intent_type: IntentType
    needs_clarification: bool = False
    clarifying_question: Optional[str] = None

    filters: dict[str, Any] = Field(default_factory=dict)

    parliament_id: Optional[str] = None
    legislature_id: Optional[str] = None
    party_code: Optional[str] = None
    from_date: Optional[date] = None
    to_date: Optional[date] = None
    person_name_contains: Optional[str] = None

    group_by: Optional[GroupBy] = None
    metrics: list[Metric] = Field(default_factory=list)
    output_format: OutputFormat = OutputFormat.BULLETS
    strict_evidence: bool = True

    def model_post_init(self, __context: Any) -> None:
        """Normalize filters after init."""
        if self.parliament_id:
            self.filters["parliament_id"] = self.parliament_id
        if self.legislature_id:
            self.filters["legislature_id"] = self.legislature_id
        if self.party_code:
            self.filters["party_code"] = self.party_code.upper()
        if self.from_date:
            self.filters["from_date"] = self.from_date.isoformat()
        if self.to_date:
            self.filters["to_date"] = self.to_date.isoformat()
        if self.person_name_contains:
            self.filters["person_name_contains"] = self.person_name_contains


class ToolCall(BaseModel):
    """Represents a tool call to be executed."""

    tool_name: str
    params: dict[str, Any]


class ToolResult(BaseModel):
    """Result from a tool execution."""

    tool_name: str
    request_id: Optional[str] = None
    data: dict[str, Any]
    error: Optional[str] = None


class ComputedResult(BaseModel):
    """Result from deterministic computation."""

    computed_metrics: dict[str, Any] = Field(default_factory=dict)
    grouped_data: Optional[dict[str, Any]] = None
    raw_data: dict[str, Any] = Field(default_factory=dict)



