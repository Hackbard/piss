"""Tests for Router Node."""

from langgraph_app.nodes.router import router_node
from langgraph_app.schemas import IntentType, UserIntent


def test_router_mandates_list():
    """Test routing MANDATES_LIST intent."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            parliament_id="parliament-nds",
            party_code="SPD",
            strict_evidence=True,
        )
    }

    result = router_node(state)

    assert len(result["tool_calls"]) == 1
    assert result["tool_calls"][0]["tool_name"] == "mandates.search"
    assert result["tool_calls"][0]["params"]["parliament_id"] == "parliament-nds"
    assert result["tool_calls"][0]["params"]["party_code"] == "SPD"


def test_router_legislature_stats():
    """Test routing LEGISLATURE_STATS intent."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.LEGISLATURE_STATS,
            legislature_id="legislature-nds-17",
            strict_evidence=True,
        )
    }

    result = router_node(state)

    assert len(result["tool_calls"]) == 1
    assert result["tool_calls"][0]["tool_name"] == "legislature.stats"
    assert result["tool_calls"][0]["params"]["legislature_id"] == "legislature-nds-17"


def test_router_needs_clarification():
    """Test routing when clarification needed."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            needs_clarification=True,
            strict_evidence=True,
        )
    }

    result = router_node(state)

    assert len(result["tool_calls"]) == 0


def test_router_person_lookup():
    """Test routing PERSON_LOOKUP intent."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.PERSON_LOOKUP,
            person_name_contains="Weil",
            strict_evidence=True,
        )
    }

    result = router_node(state)

    assert len(result["tool_calls"]) == 1
    assert result["tool_calls"][0]["tool_name"] == "person.lookup"
    assert result["tool_calls"][0]["params"]["name_contains"] == "Weil"



