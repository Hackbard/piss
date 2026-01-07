"""Tools for the minimal LangGraph MVP runner (Laravel Tool-Gateway)."""

from __future__ import annotations

from typing import Any

import httpx

from langgraph_app.settings import TOOL_BASE_URL


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


