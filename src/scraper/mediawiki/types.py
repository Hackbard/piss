from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class MediaWikiParseResponse(BaseModel):
    parse: Dict[str, Any] = Field(..., description="Parse response from MediaWiki API")
    page_id: int = Field(..., description="Wikipedia page ID")
    revision_id: int = Field(..., description="Revision ID")
    page_title: str = Field(..., description="Page title")
    html: str = Field(..., description="Parsed HTML content")
    displaytitle: Optional[str] = Field(None, description="Display title")


class MediaWikiQueryResponse(BaseModel):
    pages: Dict[str, Any] = Field(..., description="Query response pages")
    page_id: int = Field(..., description="Wikipedia page ID")
    revision_id: Optional[int] = Field(None, description="Current revision ID")
    revision_timestamp: Optional[str] = Field(None, description="Revision timestamp")


class CachedResponseMetadata(BaseModel):
    request_params: Dict[str, Any] = Field(..., description="Request parameters")
    response_headers: Dict[str, Any] = Field(default_factory=dict, description="Response headers")
    retrieved_at: str = Field(..., description="UTC timestamp when retrieved")
    sha256: str = Field(..., description="SHA256 hash of raw response")
    url: str = Field(..., description="Request URL")
    page_title: str = Field(..., description="Page title")
    page_id: int = Field(..., description="Page ID")
    revision_id: int = Field(..., description="Revision ID")
    endpoint_kind: str = Field(..., description="parse or query")


class LatestCacheManifest(BaseModel):
    revision_id: int = Field(..., description="Latest revision ID")
    retrieved_at: str = Field(..., description="UTC timestamp")
    sha256: str = Field(..., description="SHA256 hash")
    endpoint_kind: str = Field(..., description="parse or query")

