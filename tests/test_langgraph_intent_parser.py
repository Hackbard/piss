"""Tests for Intent Parser Node."""

import json
from unittest.mock import MagicMock, patch

import pytest

from langgraph_app.config import OrchestratorConfig
from langgraph_app.nodes.intent_parser import parse_intent
from langgraph_app.schemas import IntentType, OutputFormat


@pytest.fixture
def config():
    """Create test config."""
    return OrchestratorConfig(
        tool_base_url="http://localhost:8000/api/tools",
        ollama_base_url="http://localhost:11434/v1",
        ollama_model="test-model",
        openai_api_key="test-key",
    )


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
def test_parse_intent_mandates_list(mock_llm_class, config):
    """Test parsing MANDATES_LIST intent."""
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = json.dumps({
        "intent_type": "MANDATES_LIST",
        "parliament_id": "parliament-nds",
        "party_code": "SPD",
        "from_date": "2014-01-01",
        "to_date": "2020-12-31",
        "output_format": "BULLETS",
        "strict_evidence": True,
    })
    mock_llm.invoke.return_value = mock_response
    mock_llm_class.return_value = mock_llm

    intent = parse_intent("Alle SPD-Mitglieder im Landtag Niedersachsen 2014-2020", config)

    assert intent.intent_type == IntentType.MANDATES_LIST
    assert intent.parliament_id == "parliament-nds"
    assert intent.party_code == "SPD"
    assert intent.from_date.isoformat() == "2014-01-01"
    assert intent.to_date.isoformat() == "2020-12-31"
    assert intent.output_format == OutputFormat.BULLETS
    assert not intent.needs_clarification


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
def test_parse_intent_needs_clarification(mock_llm_class, config):
    """Test parsing intent with clarification needed."""
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = json.dumps({
        "intent_type": "MANDATES_LIST",
        "needs_clarification": True,
        "clarifying_question": "Welches Parlament meinen Sie?",
        "strict_evidence": True,
    })
    mock_llm.invoke.return_value = mock_response
    mock_llm_class.return_value = mock_llm

    intent = parse_intent("Alle Mitglieder", config)

    assert intent.needs_clarification
    assert intent.clarifying_question == "Welches Parlament meinen Sie?"


@patch("langgraph_app.nodes.intent_parser.ChatOpenAI")
def test_parse_intent_invalid_json_fallback(mock_llm_class, config):
    """Test fallback on invalid JSON."""
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = "invalid json"
    mock_llm.invoke.return_value = mock_response
    mock_llm_class.return_value = mock_llm

    intent = parse_intent("Test question", config)

    assert intent.needs_clarification
    assert intent.clarifying_question is not None



