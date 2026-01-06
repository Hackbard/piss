"""E2E tests for LangGraph orchestrator with stubbed tools."""

from unittest.mock import MagicMock, patch

import pytest

from langgraph_app.config import OrchestratorConfig
from langgraph_app.graph import GraphState, create_graph


@pytest.fixture
def config():
    """Create test config."""
    return OrchestratorConfig(
        tool_base_url="http://localhost:8000/api/tools",
        ollama_base_url="http://localhost:11434/v1",
        ollama_model="test-model",
        openai_api_key="test-key",
    )


@pytest.fixture
def stubbed_mandates_response():
    """Stubbed mandates.search response."""
    return {
        "meta": {
            "tool": "mandates.search",
            "executed_at": "2024-01-15T10:30:00Z",
            "request_id": "550e8400-e29b-41d4-a716-446655440000",
        },
        "applied_filter": {
            "parliament_id": "parliament-nds",
            "party_code": "SPD",
        },
        "total": 1,
        "rows": [
            {
                "person_id": "person-123",
                "person_name": "Stephan Weil",
                "wikipedia_title": "Stephan_Weil",
                "mandate_id": "mandate-456",
                "parliament_id": "parliament-nds",
                "legislature_id": "legislature-nds-17",
                "legislature": "17. Landtag Niedersachsen",
                "start_date": "2013-01-20",
                "end_date": "2017-11-14",
                "party_code": "SPD",
                "evidence_urls": [
                    "https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"
                ],
            }
        ],
    }


@pytest.fixture
def stubbed_legislature_stats_response():
    """Stubbed legislature.stats response."""
    return {
        "meta": {
            "tool": "legislature.stats",
            "executed_at": "2024-01-15T10:30:00Z",
            "request_id": "550e8400-e29b-41d4-a716-446655440001",
        },
        "legislature_id": "legislature-nds-17",
        "legislature_name": "17. Landtag Niedersachsen",
        "total_seats": 137,
        "party_seats": {
            "SPD": 49,
            "CDU": 54,
        },
        "party_vote_share": {},
        "evidence_urls": [
            "https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"
        ],
    }


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
@patch("langgraph_app.nodes.tool_executor.ToolsClient")
@patch("langgraph_app.nodes.response_composer.ChatOpenAI")
@pytest.mark.asyncio
async def test_graph_e2e_mandates_list(
    mock_response_llm_class,
    mock_tools_client_class,
    mock_intent_llm_class,
    config,
    stubbed_mandates_response,
):
    """Test E2E flow for MANDATES_LIST intent."""
    mock_intent_llm = MagicMock()
    mock_intent_response = MagicMock()
    mock_intent_response.content = '{"intent_type": "MANDATES_LIST", "parliament_id": "parliament-nds", "party_code": "SPD", "from_date": "2014-01-01", "to_date": "2020-12-31", "output_format": "BULLETS", "strict_evidence": true}'
    mock_intent_llm.invoke.return_value = mock_intent_response
    mock_intent_llm_class.return_value = mock_intent_llm

    mock_tools_client = MagicMock()
    mock_tools_client.mandates_search.return_value = stubbed_mandates_response
    mock_tools_client_class.return_value = mock_tools_client

    mock_response_llm = MagicMock()
    mock_response_response = MagicMock()
    mock_response_response.content = "Stephan Weil (SPD) war Mitglied im 17. Landtag Niedersachsen (2013-2017).\n\nEvidence: https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"
    mock_response_llm.invoke.return_value = mock_response_response
    mock_response_llm_class.return_value = mock_response_llm

    graph = create_graph(config)

    initial_state: GraphState = {
        "question": "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-01-01 und 2020-12-31",
        "intent": None,
        "tool_calls": [],
        "tool_results": [],
        "computed": None,
        "evidence_gate_passed": True,
        "evidence_gate_error": None,
        "policy_decision": None,
        "policy_warnings": [],
        "safe_answer_plan": {},
        "final_answer": None,
    }

    result = await graph.ainvoke(initial_state)

    assert result["final_answer"] is not None
    assert "Stephan Weil" in result["final_answer"]
    assert "evidence" in result["final_answer"].lower() or "https://" in result["final_answer"]


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
@patch("langgraph_app.nodes.tool_executor.ToolsClient")
@patch("langgraph_app.nodes.response_composer.ChatOpenAI")
@pytest.mark.asyncio
async def test_graph_e2e_legislature_stats(
    mock_response_llm_class,
    mock_tools_client_class,
    mock_intent_llm_class,
    config,
    stubbed_legislature_stats_response,
):
    """Test E2E flow for LEGISLATURE_STATS intent."""
    mock_intent_llm = MagicMock()
    mock_intent_response = MagicMock()
    mock_intent_response.content = '{"intent_type": "LEGISLATURE_STATS", "legislature_id": "legislature-nds-17", "metrics": ["SEAT_SHARE_PERCENT"], "output_format": "BULLETS", "strict_evidence": true}'
    mock_intent_llm.invoke.return_value = mock_intent_response
    mock_intent_llm_class.return_value = mock_intent_llm

    mock_tools_client = MagicMock()
    mock_tools_client.legislature_stats.return_value = stubbed_legislature_stats_response
    mock_tools_client_class.return_value = mock_tools_client

    mock_response_llm = MagicMock()
    mock_response_response = MagicMock()
    mock_response_response.content = "Der 17. Landtag Niedersachsen hatte 137 Sitze. SPD: 35.8%, CDU: 39.4%.\n\nEvidence: https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"
    mock_response_llm.invoke.return_value = mock_response_response
    mock_response_llm_class.return_value = mock_response_llm

    graph = create_graph(config)

    initial_state: GraphState = {
        "question": "Wie hoch war der Sitzanteil der SPD im 17. Landtag Niedersachsen?",
        "intent": None,
        "tool_calls": [],
        "tool_results": [],
        "computed": None,
        "evidence_gate_passed": True,
        "evidence_gate_error": None,
        "policy_decision": None,
        "policy_warnings": [],
        "safe_answer_plan": {},
        "final_answer": None,
    }

    result = await graph.ainvoke(initial_state)

    assert result["final_answer"] is not None
    assert "SPD" in result["final_answer"] or "35.8" in result["final_answer"]
    assert "evidence" in result["final_answer"].lower() or "https://" in result["final_answer"]


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
@patch("langgraph_app.nodes.tool_executor.ToolsClient")
@patch("langgraph_app.nodes.response_composer.ChatOpenAI")
@pytest.mark.asyncio
async def test_graph_e2e_policy_refuses_ranking(
    mock_response_llm_class,
    mock_tools_client_class,
    mock_intent_llm_class,
    config,
    stubbed_legislature_stats_response,
):
    """Test E2E flow with policy guard refusing ranking questions."""
    config.policy_mode = "NEUTRAL_STRICT"

    mock_intent_llm = MagicMock()
    mock_intent_response = MagicMock()
    mock_intent_response.content = '{"intent_type": "LEGISLATURE_STATS", "legislature_id": "legislature-nds-17", "metrics": ["SEAT_SHARE_PERCENT"], "output_format": "BULLETS", "strict_evidence": true}'
    mock_intent_llm.invoke.return_value = mock_intent_response
    mock_intent_llm_class.return_value = mock_intent_llm

    mock_tools_client = MagicMock()
    mock_tools_client.legislature_stats.return_value = stubbed_legislature_stats_response
    mock_tools_client_class.return_value = mock_tools_client

    mock_response_llm = MagicMock()
    mock_response_response = MagicMock()
    mock_response_response.content = "Ich kann keine wertenden Rankings geben."
    mock_response_llm.invoke.return_value = mock_response_response
    mock_response_llm_class.return_value = mock_response_llm

    graph = create_graph(config)

    initial_state: GraphState = {
        "question": "Wer ist die schlimmste Partei?",
        "intent": None,
        "tool_calls": [],
        "tool_results": [],
        "computed": None,
        "evidence_gate_passed": True,
        "evidence_gate_error": None,
        "policy_decision": None,
        "policy_warnings": [],
        "safe_answer_plan": {},
        "final_answer": None,
    }

    result = await graph.ainvoke(initial_state)

    assert result["final_answer"] is not None
    assert result.get("policy_decision") == "REFUSE_RANKING" or "ranking" in result["final_answer"].lower() or "wertend" in result["final_answer"].lower()


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
@patch("langgraph_app.nodes.tool_executor.ToolsClient")
@patch("langgraph_app.nodes.response_composer.ChatOpenAI")
@pytest.mark.asyncio
async def test_graph_e2e_policy_debug_explain_queries(
    mock_response_llm_class,
    mock_tools_client_class,
    mock_intent_llm_class,
    config,
    stubbed_mandates_response,
):
    """Test E2E flow with debug explain queries enabled."""
    config.debug_explain_queries = True

    mock_intent_llm = MagicMock()
    mock_intent_response = MagicMock()
    mock_intent_response.content = '{"intent_type": "MANDATES_LIST", "parliament_id": "parliament-nds", "party_code": "SPD", "from_date": "2014-01-01", "to_date": "2020-12-31", "output_format": "BULLETS", "strict_evidence": true}'
    mock_intent_llm.invoke.return_value = mock_intent_response
    mock_intent_llm_class.return_value = mock_intent_llm

    mock_tools_client = MagicMock()
    mock_tools_client.mandates_search.return_value = stubbed_mandates_response
    mock_tools_client_class.return_value = mock_tools_client

    mock_response_llm = MagicMock()
    mock_response_response = MagicMock()
    mock_response_response.content = "Stephan Weil (SPD) war Mitglied."
    mock_response_llm.invoke.return_value = mock_response_response
    mock_response_llm_class.return_value = mock_response_llm

    graph = create_graph(config)

    initial_state: GraphState = {
        "question": "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-01-01 und 2020-12-31",
        "intent": None,
        "tool_calls": [],
        "tool_results": [],
        "computed": None,
        "evidence_gate_passed": True,
        "evidence_gate_error": None,
        "policy_decision": None,
        "policy_warnings": [],
        "safe_answer_plan": {},
        "final_answer": None,
    }

    result = await graph.ainvoke(initial_state)

    assert result["final_answer"] is not None
    assert "[Developer Debug]" in result["final_answer"] or "Explain Query" in result["final_answer"]
    assert "Intent" in result["final_answer"] or "Tool Calls" in result["final_answer"]


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
@patch("langgraph_app.nodes.tool_executor.ToolsClient")
@patch("langgraph_app.nodes.response_composer.ChatOpenAI")
@pytest.mark.asyncio
async def test_graph_e2e_policy_no_debug_output(
    mock_response_llm_class,
    mock_tools_client_class,
    mock_intent_llm_class,
    config,
    stubbed_mandates_response,
):
    """Test E2E flow without debug explain queries."""
    config.debug_explain_queries = False

    mock_intent_llm = MagicMock()
    mock_intent_response = MagicMock()
    mock_intent_response.content = '{"intent_type": "MANDATES_LIST", "parliament_id": "parliament-nds", "party_code": "SPD", "from_date": "2014-01-01", "to_date": "2020-12-31", "output_format": "BULLETS", "strict_evidence": true}'
    mock_intent_llm.invoke.return_value = mock_intent_response
    mock_intent_llm_class.return_value = mock_intent_llm

    mock_tools_client = MagicMock()
    mock_tools_client.mandates_search.return_value = stubbed_mandates_response
    mock_tools_client_class.return_value = mock_tools_client

    mock_response_llm = MagicMock()
    mock_response_response = MagicMock()
    mock_response_response.content = "Stephan Weil (SPD) war Mitglied."
    mock_response_llm.invoke.return_value = mock_response_response
    mock_response_llm_class.return_value = mock_response_llm

    graph = create_graph(config)

    initial_state: GraphState = {
        "question": "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-01-01 und 2020-12-31",
        "intent": None,
        "tool_calls": [],
        "tool_results": [],
        "computed": None,
        "evidence_gate_passed": True,
        "evidence_gate_error": None,
        "policy_decision": None,
        "policy_warnings": [],
        "safe_answer_plan": {},
        "final_answer": None,
    }

    result = await graph.ainvoke(initial_state)

    assert result["final_answer"] is not None
    assert "[Developer Debug]" not in result["final_answer"]
    assert "Explain Query" not in result["final_answer"]



