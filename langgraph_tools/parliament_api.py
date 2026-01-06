import json
from typing import Any, Optional

import httpx


class ParliamentAPIError(Exception):
    """Base exception for Parliament API errors."""

    def __init__(self, message: str, tool_name: str, request_id: Optional[str] = None):
        super().__init__(message)
        self.tool_name = tool_name
        self.request_id = request_id


class ParliamentAPIClient:
    """Thin wrapper for Parliament Tool API (LangGraph-ready)."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: float = 30.0,
        max_retries: int = 2,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.client = httpx.AsyncClient(timeout=timeout)

    async def _request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[dict] = None,
        tool_name: str = "",
    ) -> dict:
        """Make HTTP request with retries."""
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.client.request(method, url, json=json_data)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 422:
                    error_data = e.response.json()
                    detail = error_data.get("detail", {})
                    request_id = detail.get("request_id")
                    error_code = detail.get("error_code", "VALIDATION_ERROR")
                    raise ParliamentAPIError(
                        f"{tool_name} failed: {detail.get('error', str(e))}",
                        tool_name,
                        request_id,
                    ) from e
                if attempt < self.max_retries:
                    continue
                raise ParliamentAPIError(
                    f"{tool_name} failed: {e}",
                    tool_name,
                    None,
                ) from e
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    continue
                raise ParliamentAPIError(
                    f"{tool_name} network error: {e}",
                    tool_name,
                    None,
                ) from e

    async def mandates_search(
        self,
        parliament_id: Optional[str] = None,
        legislature_id: Optional[str] = None,
        party_code: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        person_id: Optional[str] = None,
        person_name_contains: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
        sort: str = "person_name",
        sort_dir: str = "ASC",
        strict_evidence: bool = True,
    ) -> dict:
        """
        Search mandates.
        
        Returns dict with meta, applied_filter, total, rows.
        """
        payload = {
            "parliament_id": parliament_id,
            "legislature_id": legislature_id,
            "party_code": party_code,
            "from_date": from_date,
            "to_date": to_date,
            "person_id": person_id,
            "person_name_contains": person_name_contains,
            "limit": limit,
            "offset": offset,
            "sort": sort,
            "sort_dir": sort_dir,
            "strict_evidence": strict_evidence,
        }
        
        payload = {k: v for k, v in payload.items() if v is not None}
        
        return await self._request(
            "POST",
            "/api/tools/mandates/search",
            json_data=payload,
            tool_name="mandates.search",
        )

    async def legislature_stats(
        self,
        legislature_id: str,
        strict_evidence: bool = True,
    ) -> dict:
        """
        Get legislature statistics.
        
        Returns dict with meta, legislature_id, legislature_name, total_seats, party_seats, evidence_urls.
        """
        payload = {
            "legislature_id": legislature_id,
            "strict_evidence": strict_evidence,
        }
        
        return await self._request(
            "POST",
            "/api/tools/legislatures/stats",
            json_data=payload,
            tool_name="legislature.stats",
        )

    async def person_lookup(
        self,
        person_id: Optional[str] = None,
        name_contains: Optional[str] = None,
        limit: int = 20,
    ) -> dict:
        """
        Lookup person by ID or search by name.
        
        Returns dict with meta, persons.
        """
        if not person_id and not name_contains:
            raise ValueError("Must specify either person_id or name_contains")
        
        payload = {
            "person_id": person_id,
            "name_contains": name_contains,
            "limit": limit,
        }
        
        payload = {k: v for k, v in payload.items() if v is not None}
        
        return await self._request(
            "POST",
            "/api/tools/persons/lookup",
            json_data=payload,
            tool_name="person.lookup",
        )

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


_client: Optional[ParliamentAPIClient] = None


def get_client(base_url: Optional[str] = None) -> ParliamentAPIClient:
    """Get or create global client instance."""
    global _client
    if _client is None:
        _client = ParliamentAPIClient(base_url=base_url or "http://localhost:8000")
    return _client


async def mandates_search(**kwargs) -> dict:
    """Convenience function for mandates.search tool."""
    client = get_client()
    return await client.mandates_search(**kwargs)


async def legislature_stats(legislature_id: str, strict_evidence: bool = True) -> dict:
    """Convenience function for legislature.stats tool."""
    client = get_client()
    return await client.legislature_stats(legislature_id, strict_evidence)


async def person_lookup(person_id: Optional[str] = None, name_contains: Optional[str] = None, limit: int = 20) -> dict:
    """Convenience function for person.lookup tool."""
    client = get_client()
    return await client.person_lookup(person_id, name_contains, limit)

