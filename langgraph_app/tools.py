"""Tools for the minimal LangGraph MVP runner (Laravel Tool-Gateway)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from langgraph_app.settings import TOOL_BASE_URL


_coverage_cache: dict[str, dict[str, Any]] | None = None


@dataclass
class CoverageRow:
    parliament_id: str
    mandate_count: int
    open_end_count: int
    min_start: date | None
    max_end: date | None
    max_observed: date | None
    invalid_date_count: int
    missing_evidence_count: int


class ToolGatewayError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def members_list(
    *,
    parliament_id: str,
    party_code: str,
    from_date: str,
    to_date: str,
    limit: int = 200,
    offset: int = 0,
    strict_evidence: bool = True,
) -> dict[str, Any]:
    url = f"{TOOL_BASE_URL.rstrip('/')}/members/list"
    payload: dict[str, Any] = {
        "parliament_id": parliament_id,
        "party_code": party_code,
        "from_date": from_date,
        "to_date": to_date,
        "limit": limit,
        "offset": offset,
        "strict_evidence": strict_evidence,
    }

    try:
        response = httpx.post(url, json=payload, timeout=30.0)
    except httpx.RequestError as e:
        raise ToolGatewayError(f"members.list network error: {e}") from e

    if response.status_code != 200:
        body = response.text
        body_excerpt = body[:2_000] + ("..." if len(body) > 2_000 else "")
        raise ToolGatewayError(
            f"members.list failed ({response.status_code}) at {url}: {body_excerpt}",
            status_code=response.status_code,
        )

    try:
        data = response.json()
    except ValueError as e:
        raise ToolGatewayError("members.list returned invalid JSON") from e

    if not isinstance(data, dict):
        raise ToolGatewayError("members.list returned unexpected JSON shape (expected object)")

    return data


def parliaments_coverage(parliament_ids: list[str] | None = None) -> dict[str, Any]:
    """Get coverage statistics per parliament_id with caching per CLI run.
    
    Returns dict with:
    - rows: list of coverage data per parliament
    - data_as_of: server date (YYYY-MM-DD) from meta.executed_at or current date
    - meta: metadata from response
    """
    global _coverage_cache
    
    cache_key = "all" if not parliament_ids else ",".join(sorted(parliament_ids))
    
    if _coverage_cache is None:
        _coverage_cache = {}
    
    if cache_key in _coverage_cache:
        return _coverage_cache[cache_key]
    
    url = f"{TOOL_BASE_URL.rstrip('/')}/parliaments/coverage"
    
    try:
        if parliament_ids:
            params = {"parliament_ids": ",".join(parliament_ids)}
            response = httpx.get(url, params=params, timeout=30.0)
        else:
            response = httpx.get(url, timeout=30.0)
    except httpx.RequestError:
        try:
            payload: dict[str, Any] = {}
            if parliament_ids:
                payload["parliament_ids"] = parliament_ids
            response = httpx.post(url, json=payload, timeout=30.0)
        except httpx.RequestError as e:
            raise ToolGatewayError(f"parliaments.coverage network error: {e}") from e
    
    if response.status_code != 200:
        body = response.text
        body_excerpt = body[:2_000] + ("..." if len(body) > 2_000 else "")
        raise ToolGatewayError(
            f"parliaments.coverage failed ({response.status_code}) at {url}: {body_excerpt}",
            status_code=response.status_code,
        )
    
    try:
        data = response.json()
    except ValueError as e:
        raise ToolGatewayError("parliaments.coverage returned invalid JSON") from e
    
    if not isinstance(data, dict):
        raise ToolGatewayError("parliaments.coverage returned unexpected JSON shape (expected object)")
    
    meta = data.get("meta", {})
    executed_at = meta.get("executed_at")
    if executed_at:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(executed_at.replace("Z", "+00:00"))
            data["data_as_of"] = dt.date().isoformat()
        except (ValueError, AttributeError):
            from datetime import date
            data["data_as_of"] = date.today().isoformat()
    else:
        from datetime import date
        data["data_as_of"] = date.today().isoformat()
    
    _coverage_cache[cache_key] = data
    return data


