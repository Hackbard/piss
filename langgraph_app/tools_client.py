"""HTTP client for Tool API with schema validation."""

import json
from pathlib import Path
from typing import Any, Optional

import httpx
from jsonschema import Draft7Validator, RefResolver, ValidationError

from langgraph_app.config import OrchestratorConfig


class ToolContractViolation(Exception):
    """Raised when tool response violates contract schema."""

    def __init__(self, message: str, tool_name: str, request_id: Optional[str] = None):
        super().__init__(message)
        self.tool_name = tool_name
        self.request_id = request_id


class ToolsClient:
    """HTTP client for Tool API with contract validation."""

    def __init__(self, config: OrchestratorConfig):
        self.config = config
        self.base_url = config.tool_base_url.rstrip("/")
        self.timeout = config.tool_timeout_seconds
        self.client = httpx.Client(timeout=self.timeout)
        self._schemas_cache: dict[str, dict] = {}

    def _load_schema(self, schema_name: str) -> dict:
        """Load JSON schema from contracts directory."""
        if schema_name in self._schemas_cache:
            return self._schemas_cache[schema_name]

        schema_path = Path(__file__).parent.parent / "contracts" / "tools" / f"{schema_name}.json"
        if not schema_path.exists():
            raise ValueError(f"Schema not found: {schema_path}")

        with open(schema_path, "r") as f:
            schema = json.load(f)

        self._schemas_cache[schema_name] = schema
        return schema

    def _validate_request(self, tool_name: str, payload: dict[str, Any]) -> None:
        """Validate request payload against schema."""
        schema_name = f"{tool_name.replace('.', '_')}.request"
        try:
            schema = self._load_schema(schema_name)
        except ValueError:
            return

        base_path = Path(__file__).parent.parent / "contracts" / "tools"
        resolver = RefResolver(
            base_uri=f"file://{base_path}/",
            referrer=schema,
        )

        validator = Draft7Validator(schema, resolver=resolver)
        try:
            validator.validate(payload)
        except ValidationError as e:
            raise ToolContractViolation(
                f"Request validation failed: {e.message}",
                tool_name,
                None,
            ) from e

    def _validate_response(self, tool_name: str, response_data: dict[str, Any], request_id: Optional[str]) -> None:
        """Validate response against schema."""
        schema_name = f"{tool_name.replace('.', '_')}.response"
        try:
            schema = self._load_schema(schema_name)
        except ValueError:
            return

        base_path = Path(__file__).parent.parent / "contracts" / "tools"
        resolver = RefResolver(
            base_uri=f"file://{base_path}/",
            referrer=schema,
        )

        validator = Draft7Validator(schema, resolver=resolver)
        try:
            validator.validate(response_data)
        except ValidationError as e:
            raise ToolContractViolation(
                f"Response validation failed: {e.message}",
                tool_name,
                request_id,
            ) from e

    def _clamp_limit(self, limit: Optional[int]) -> int:
        """Clamp limit to valid range."""
        if limit is None:
            return 200
        return max(1, min(1000, limit))

    def _clamp_offset(self, offset: Optional[int]) -> int:
        """Clamp offset to valid range."""
        if offset is None:
            return 0
        return max(0, offset)

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[dict[str, Any]] = None,
        tool_name: str = "",
    ) -> dict[str, Any]:
        """Make HTTP request with retries and validation."""
        url = f"{self.base_url}{endpoint}"

        if json_data:
            if "limit" in json_data:
                json_data["limit"] = self._clamp_limit(json_data.get("limit"))
            if "offset" in json_data:
                json_data["offset"] = self._clamp_offset(json_data.get("offset"))

            self._validate_request(tool_name, json_data)

        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                response = self.client.request(method, url, json=json_data)
                response.raise_for_status()
                result = response.json()

                request_id = result.get("meta", {}).get("request_id")
                self._validate_response(tool_name, result, request_id)

                return result
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 422:
                    error_data = e.response.json()
                    detail = error_data.get("detail", {})
                    request_id = detail.get("request_id")
                    raise ToolContractViolation(
                        f"{tool_name} failed: {detail.get('error', str(e))}",
                        tool_name,
                        request_id,
                    ) from e
                if attempt < max_retries:
                    continue
                raise ToolContractViolation(
                    f"{tool_name} failed: {e}",
                    tool_name,
                    None,
                ) from e
            except httpx.RequestError as e:
                if attempt < max_retries:
                    continue
                raise ToolContractViolation(
                    f"{tool_name} network error: {e}",
                    tool_name,
                    None,
                ) from e

    def mandates_search(
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
    ) -> dict[str, Any]:
        """Search mandates."""
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

        return self._request(
            "POST",
            "/mandates/search",
            json_data=payload,
            tool_name="mandates.search",
        )

    def legislature_stats(
        self,
        legislature_id: str,
        strict_evidence: bool = True,
    ) -> dict[str, Any]:
        """Get legislature statistics."""
        payload = {
            "legislature_id": legislature_id,
            "strict_evidence": strict_evidence,
        }

        return self._request(
            "POST",
            "/legislatures/stats",
            json_data=payload,
            tool_name="legislature.stats",
        )

    def person_lookup(
        self,
        person_id: Optional[str] = None,
        name_contains: Optional[str] = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Lookup person by ID or search by name."""
        if not person_id and not name_contains:
            raise ValueError("Must specify either person_id or name_contains")

        payload = {
            "person_id": person_id,
            "name_contains": name_contains,
            "limit": limit,
        }

        payload = {k: v for k, v in payload.items() if v is not None}

        return self._request(
            "POST",
            "/persons/lookup",
            json_data=payload,
            tool_name="person.lookup",
        )

    def close(self) -> None:
        """Close HTTP client."""
        self.client.close()



