"""Tests for mandates.search response shape and telemetry extraction."""

import json
import pytest
from unittest.mock import Mock, patch

from langgraph_app.tools_client import ToolsClient, LegacyResponseShapeError
from langgraph_tools.parliament_api import ParliamentAPIClient, ParliamentAPIError
from langgraph_app.config import OrchestratorConfig


class TestMandatesSearchResponseShape:
    """Test new response shape validation and legacy shape detection."""
    
    def test_new_response_shape_valid(self):
        """Test that new response shape is accepted."""
        response_data = {
            "meta": {
                "tool": "mandates.search",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
            },
            "applied_filter": {"parliament_id": "HH", "limit": 5},
            "total": None,
            "rows": [
                {
                    "person_id": "person-123",
                    "person_name": "Test Person",
                    "mandate_id": "mandate-456",
                    "parliament_id": "HH",
                    "legislature_id": "leg-789",
                    "legislature": "Test Legislature",
                    "start_date": "2020-01-01",
                    "party_code": "SPD",
                    "evidence_urls": ["https://example.com"],
                }
            ],
        }
        
        config = OrchestratorConfig(
            tool_base_url="http://localhost:8000",
            tool_timeout_seconds=30,
        )
        client = ToolsClient(config)
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            result = client.mandates_search(parliament_id="HH", limit=5)
            
            assert "meta" in result
            assert "applied_filter" in result
            assert "rows" in result
            assert result["meta"]["tool"] == "mandates.search"
    
    def test_legacy_response_shape_raises_error(self):
        """Test that old {tool, data} response shape raises clear error."""
        legacy_response = {
            "tool": "mandates.search",
            "data": {
                "rows": [],
            },
        }
        
        config = OrchestratorConfig(
            tool_base_url="http://localhost:8000",
            tool_timeout_seconds=30,
        )
        client = ToolsClient(config)
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = legacy_response
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            with pytest.raises(LegacyResponseShapeError) as exc_info:
                client.mandates_search(parliament_id="HH", limit=5)
            
            assert "mandates.search" in str(exc_info.value)
            assert "meta, applied_filter, rows" in str(exc_info.value)
            assert exc_info.value.tool_name == "mandates.search"
    
    def test_missing_required_fields_raises_error(self):
        """Test that missing meta/applied_filter/rows raises error."""
        invalid_response = {
            "meta": {"tool": "mandates.search"},
        }
        
        config = OrchestratorConfig(
            tool_base_url="http://localhost:8000",
            tool_timeout_seconds=30,
        )
        client = ToolsClient(config)
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = invalid_response
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            with pytest.raises(LegacyResponseShapeError) as exc_info:
                client.mandates_search(parliament_id="HH", limit=5)
            
            assert "mandates.search" in str(exc_info.value)


class TestMandatesSearchTelemetry:
    """Test telemetry extraction from mandates.search responses."""
    
    def test_telemetry_extraction_active_only_true(self):
        """Test that telemetry fields are extracted when active_only=true."""
        response_data = {
            "meta": {
                "tool": "mandates.search",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
                "active_only": True,
                "as_of": "2020-01-01",
                "coverage_degraded": True,
                "excluded_due_to_missing_start_date_count": 123,
                "excluded_due_to_missing_legislature_start_date_count": 45,
            },
            "applied_filter": {"parliament_id": "HH", "active_only": True, "as_of": "2020-01-01"},
            "total": None,
            "rows": [],
        }
        
        config = OrchestratorConfig(
            tool_base_url="http://localhost:8000",
            tool_timeout_seconds=30,
        )
        client = ToolsClient(config)
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            result = client.mandates_search(parliament_id="HH", active_only=True, as_of="2020-01-01")
            
            meta = result["meta"]
            assert meta["active_only"] is True
            assert meta["as_of"] == "2020-01-01"
            assert meta["coverage_degraded"] is True
            assert meta["excluded_due_to_missing_start_date_count"] == 123
            assert meta["excluded_due_to_missing_legislature_start_date_count"] == 45
    
    def test_telemetry_optional_when_active_only_false(self):
        """Test that telemetry fields are optional when active_only=false."""
        response_data = {
            "meta": {
                "tool": "mandates.search",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
            },
            "applied_filter": {"parliament_id": "HH"},
            "total": 42,
            "rows": [],
        }
        
        config = OrchestratorConfig(
            tool_base_url="http://localhost:8000",
            tool_timeout_seconds=30,
        )
        client = ToolsClient(config)
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            result = client.mandates_search(parliament_id="HH", active_only=False)
            
            meta = result["meta"]
            assert "active_only" not in meta or meta.get("active_only") is None
            assert "as_of" not in meta or meta.get("as_of") is None


class TestParliamentAPIClientTelemetry:
    """Test ParliamentAPIClient handles new response shape and telemetry."""
    
    @pytest.mark.asyncio
    async def test_parliament_api_client_new_shape(self):
        """Test ParliamentAPIClient accepts new response shape."""
        response_data = {
            "meta": {
                "tool": "mandates.search",
                "executed_at": "2024-01-15T10:30:00Z",
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
            },
            "applied_filter": {"parliament_id": "HH"},
            "total": None,
            "rows": [],
        }
        
        client = ParliamentAPIClient(base_url="http://localhost:8000")
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            result = await client.mandates_search(parliament_id="HH", limit=5)
            
            assert "meta" in result
            assert "applied_filter" in result
            assert "rows" in result
    
    @pytest.mark.asyncio
    async def test_parliament_api_client_legacy_shape_error(self):
        """Test ParliamentAPIClient raises error for legacy shape."""
        legacy_response = {
            "tool": "mandates.search",
            "data": {"rows": []},
        }
        
        client = ParliamentAPIClient(base_url="http://localhost:8000")
        
        with patch.object(client.client, "request") as mock_request:
            mock_response = Mock()
            mock_response.json.return_value = legacy_response
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response
            
            with pytest.raises(ParliamentAPIError) as exc_info:
                await client.mandates_search(parliament_id="HH", limit=5)
            
            assert "Legacy response shape" in str(exc_info.value) or "Invalid response shape" in str(exc_info.value)
