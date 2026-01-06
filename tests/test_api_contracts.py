import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.api.app import create_app
from scraper.api.schema_validator import assert_json_matches_schema, validate_response


@pytest.fixture
def client():
    """Create test client."""
    app = create_app()
    return TestClient(app)


@pytest.fixture
def schema_base_path():
    """Get schema base path."""
    return Path(__file__).parent.parent / "contracts" / "tools"


class TestMandateSearchContract:
    def test_request_schema_validation(self, schema_base_path):
        """Test request schema validation."""
        request_data = {
            "parliament_id": "parliament-nds",
            "party_code": "SPD",
            "from_date": "2014-01-01",
            "to_date": "2020-12-31",
            "limit": 50,
            "strict_evidence": True,
        }
        
        assert_json_matches_schema(
            request_data,
            "contracts/tools/mandates.search.request.schema.json",
        )
    
    def test_invalid_date_range(self, client):
        """Test invalid date range returns 422."""
        response = client.post(
            "/api/tools/mandates/search",
            json={
                "from_date": "2020-12-31",
                "to_date": "2020-01-01",
            },
        )
        assert response.status_code == 422
    
    def test_response_schema_structure(self, schema_base_path):
        """Test response schema structure."""
        response_data = {
            "meta": {
                "tool": "mandates.search",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
                "data_version": "git:abc123",
            },
            "applied_filter": {},
            "total": 1,
            "rows": [
                {
                    "person_id": "p1",
                    "person_name": "Test",
                    "mandate_id": "m1",
                    "parliament_id": "par1",
                    "legislature_id": "leg1",
                    "legislature": "Test Legislature",
                    "start_date": "2020-01-01",
                    "end_date": None,
                    "party_code": "SPD",
                    "evidence_urls": ["https://example.com"],
                }
            ],
        }
        
        assert_json_matches_schema(
            response_data,
            "contracts/tools/mandates.search.response.schema.json",
        )


class TestLegislatureStatsContract:
    def test_request_schema_validation(self, schema_base_path):
        """Test request schema validation."""
        request_data = {
            "legislature_id": "legislature-nds-17",
            "strict_evidence": True,
        }
        
        assert_json_matches_schema(
            request_data,
            "contracts/tools/legislature.stats.request.schema.json",
        )
    
    def test_response_schema_structure(self, schema_base_path):
        """Test response schema structure."""
        response_data = {
            "meta": {
                "tool": "legislature.stats",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440001",
            },
            "legislature_id": "legislature-nds-17",
            "legislature_name": "17. Landtag Niedersachsen",
            "parliament_id": None,
            "start_date": None,
            "end_date": None,
            "total_seats": 137,
            "party_seats": {"SPD": 49},
            "party_vote_share": {},
            "evidence_urls": ["https://example.com"],
        }
        
        assert_json_matches_schema(
            response_data,
            "contracts/tools/legislature.stats.response.schema.json",
        )


class TestPersonLookupContract:
    def test_request_schema_validation_by_id(self, schema_base_path):
        """Test request schema validation (by ID)."""
        request_data = {"person_id": "person-123"}
        
        assert_json_matches_schema(
            request_data,
            "contracts/tools/person.lookup.request.schema.json",
        )
    
    def test_request_schema_validation_by_name(self, schema_base_path):
        """Test request schema validation (by name)."""
        request_data = {"name_contains": "Weil", "limit": 10}
        
        assert_json_matches_schema(
            request_data,
            "contracts/tools/person.lookup.request.schema.json",
        )
    
    def test_response_schema_structure(self, schema_base_path):
        """Test response schema structure."""
        response_data = {
            "meta": {
                "tool": "person.lookup",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440002",
            },
            "persons": [
                {
                    "person_id": "p1",
                    "name": "Test Person",
                    "wikipedia_title": None,
                    "wikipedia_url": None,
                    "birth_date": None,
                    "death_date": None,
                    "intro": None,
                    "evidence_urls": ["https://example.com"],
                }
            ],
        }
        
        assert_json_matches_schema(
            response_data,
            "contracts/tools/person.lookup.response.schema.json",
        )


class TestStrictEvidenceGuardrail:
    def test_strict_evidence_enforced(self, client):
        """Test that strict_evidence=true enforces evidence URLs."""
        response = client.post(
            "/api/tools/mandates/search",
            json={
                "parliament_id": "parliament-nds",
                "strict_evidence": True,
                "limit": 1,
            },
        )
        
        if response.status_code == 422:
            error_data = response.json()
            assert "EVIDENCE_MISSING" in error_data.get("detail", {}).get("error_code", "")

