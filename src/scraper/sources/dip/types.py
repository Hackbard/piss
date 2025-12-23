from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class DipPerson(BaseModel):
    id: int = Field(..., description="DIP person ID")
    vorname: Optional[str] = Field(None, description="First name")
    nachname: Optional[str] = Field(None, description="Last name")
    namenszusatz: Optional[str] = Field(None, description="Name suffix")
    titel: Optional[str] = Field(None, description="Title")
    fraktion: Optional[str] = Field(None, description="Party/Fraktion")
    wahlperiode: List[int] = Field(default_factory=list, description="Wahlperioden")
    person_roles: Optional[List[Dict[str, Any]]] = Field(None, description="Person roles")

    @field_validator("fraktion", mode="before")
    @classmethod
    def normalize_fraktion(cls, v: Any) -> Optional[str]:
        if isinstance(v, list):
            return v[0] if v else None
        return v


class DipPersonListResponse(BaseModel):
    numFound: int = Field(..., description="Total number of results")
    cursor: Optional[str] = Field(None, description="Cursor for pagination")
    documents: List[DipPerson] = Field(default_factory=list, description="Person documents")


class DipPersonDetailResponse(BaseModel):
    person: DipPerson = Field(..., description="Person detail")

