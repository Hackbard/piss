"""Tests for coverage-aware defaulting and as_of clamping."""

from datetime import date
from unittest.mock import Mock, patch

import pytest

from langgraph_app.tools import CoverageRow, parliaments_coverage, ToolGatewayError


def test_coverage_clamp_base_today_less_than_max_end():
    base_today = date(2026, 1, 8)
    max_end = date(2026, 1, 3)
    
    as_of = min(base_today, max_end)
    
    assert as_of == date(2026, 1, 3)


def test_coverage_clamp_base_today_greater_than_max_end():
    base_today = date(2026, 1, 8)
    max_end = date(2026, 2, 1)
    
    as_of = min(base_today, max_end)
    
    assert as_of == date(2026, 1, 8)


def test_coverage_clamp_max_end_none():
    base_today = date(2026, 1, 8)
    max_end = None
    
    as_of = base_today
    
    assert as_of == date(2026, 1, 8)


def test_coverage_clamp_coverage_row_missing():
    base_today = date(2026, 1, 8)
    coverage = {}
    
    coverage_missing = not coverage or coverage.get("mandates_count", 0) == 0
    as_of = base_today
    
    assert as_of == date(2026, 1, 8)
    assert coverage_missing is True


def test_parliaments_coverage_extracts_data_as_of():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "meta": {
            "executed_at": "2026-01-08T12:00:00Z",
            "tool": "parliaments.coverage",
        },
        "rows": [
            {
                "parliament_id": "NI",
                "mandates_count": 100,
                "max_end": "2022-11-08",
            }
        ],
    }
    
    with patch("langgraph_app.tools.httpx.get", return_value=mock_response):
        result = parliaments_coverage(["NI"])
        
        assert result["data_as_of"] == "2026-01-08"


def test_parliaments_coverage_fallback_to_today_if_no_executed_at():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "meta": {},
        "rows": [],
    }
    
    with patch("langgraph_app.tools.httpx.get", return_value=mock_response):
        with patch("langgraph_app.tools.date") as mock_date:
            mock_date.today.return_value = date(2026, 1, 8)
            result = parliaments_coverage()
            
            assert result["data_as_of"] == "2026-01-08"


def test_parliaments_coverage_fails_fast_on_network_error():
    with patch("langgraph_app.tools.httpx.get", side_effect=Exception("Network error")):
        with patch("langgraph_app.tools.httpx.post", side_effect=Exception("Network error")):
            with pytest.raises(ToolGatewayError, match="network error"):
                parliaments_coverage(["NI"])


def test_coverage_clamp_per_parliament():
    base_today = date(2026, 1, 8)
    coverage_data = {
        "rows": [
            {
                "parliament_id": "NI",
                "mandates_count": 100,
                "max_end": "2022-11-08",
            },
            {
                "parliament_id": "HH",
                "mandates_count": 50,
                "max_end": "2026-01-03",
            },
        ],
        "data_as_of": "2026-01-08",
    }
    
    coverage_by_pid = {
        row["parliament_id"]: row for row in coverage_data["rows"]
    }
    
    as_of_by_parliament = {}
    for pid in ["NI", "HH"]:
        coverage = coverage_by_pid.get(pid, {})
        max_end_str = coverage.get("max_end")
        
        if max_end_str:
            max_end_date = date.fromisoformat(max_end_str[:10])
            as_of_date = min(base_today, max_end_date)
            as_of_by_parliament[pid] = as_of_date.isoformat()
        else:
            as_of_by_parliament[pid] = base_today.isoformat()
    
    assert as_of_by_parliament["NI"] == "2022-11-08"
    assert as_of_by_parliament["HH"] == "2026-01-03"


def test_coverage_clamp_active_only_without_explicit_date():
    from langgraph_app.graph import members_list_plan_llm_node
    
    mock_coverage_response = {
        "data_as_of": "2026-01-08",
        "rows": [
            {
                "parliament_id": "NI",
                "mandates_count": 100,
                "max_end": "2022-11-08",
            }
        ],
    }
    
    mock_llm_response = Mock()
    mock_llm_response.content = '{"parliament_ids": ["NI"], "party_code": "SPD", "active_only": true}'
    
    state = {
        "question": "Aktuelle SPD-Mitglieder in Niedersachsen",
        "parliament_ids": [],
        "active_only": False,
        "resolved_from_date": None,
        "resolved_to_date": None,
        "tool_base_input": None,
        "answer": None,
    }
    
    with patch("langgraph_app.graph.parliaments_coverage", return_value=mock_coverage_response):
        with patch("langgraph_app.graph.ChatOpenAI") as mock_llm_class:
            mock_llm = Mock()
            mock_llm.invoke.return_value = mock_llm_response
            mock_llm_class.return_value = mock_llm
            
            result = members_list_plan_llm_node(state)
            
            assert result["as_of_by_parliament"]["NI"] == "2022-11-08"
            assert result["coverage_missing_by_parliament"]["NI"] is False
            assert result["tool_base_input"]["from_date"] is None
            assert result["tool_base_input"]["to_date"] is None


def test_coverage_clamp_active_only_with_explicit_date():
    from langgraph_app.graph import members_list_plan_llm_node
    
    mock_llm_response = Mock()
    mock_llm_response.content = '{"parliament_ids": ["NI"], "party_code": "SPD", "active_only": true, "to_date": "2020-12-31"}'
    
    state = {
        "question": "Aktuelle SPD-Mitglieder in Niedersachsen am 2020-12-31",
        "parliament_ids": [],
        "active_only": False,
        "resolved_from_date": None,
        "resolved_to_date": None,
        "tool_base_input": None,
        "answer": None,
    }
    
    with patch("langgraph_app.graph.ChatOpenAI") as mock_llm_class:
        mock_llm = Mock()
        mock_llm.invoke.return_value = mock_llm_response
        mock_llm_class.return_value = mock_llm
        
        result = members_list_plan_llm_node(state)
        
        assert result["resolved_from_date"] == "2020-12-31"
        assert result["resolved_to_date"] == "2020-12-31"
        assert "as_of_by_parliament" not in result or result.get("as_of_by_parliament") is None




