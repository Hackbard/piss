"""Tests for Evidence Gate Node."""

from langgraph_app.nodes.evidence_gate import evidence_gate_node
from langgraph_app.schemas import UserIntent


def test_evidence_gate_passed():
    """Test evidence gate passes with valid evidence."""
    state = {
        "intent": UserIntent(
            intent_type="MANDATES_LIST",
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": ["https://example.com/evidence1"],
                        }
                    ]
                },
            }
        ],
    }

    result = evidence_gate_node(state)

    assert result["evidence_gate_passed"] is True


def test_evidence_gate_failed_missing_evidence():
    """Test evidence gate fails with missing evidence."""
    state = {
        "intent": UserIntent(
            intent_type="MANDATES_LIST",
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": [],
                        }
                    ]
                },
            }
        ],
    }

    result = evidence_gate_node(state)

    assert result["evidence_gate_passed"] is False
    assert "evidence_gate_error" in result
    assert "mandate mandate-1" in result["evidence_gate_error"]


def test_evidence_gate_legislature_stats():
    """Test evidence gate for legislature stats."""
    state = {
        "intent": UserIntent(
            intent_type="LEGISLATURE_STATS",
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "legislature.stats",
                "data": {
                    "legislature_id": "legislature-nds-17",
                    "evidence_urls": ["https://example.com/evidence1"],
                },
            }
        ],
    }

    result = evidence_gate_node(state)

    assert result["evidence_gate_passed"] is True


def test_evidence_gate_not_strict():
    """Test evidence gate passes when strict_evidence=False."""
    state = {
        "intent": UserIntent(
            intent_type="MANDATES_LIST",
            strict_evidence=False,
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "evidence_urls": [],
                        }
                    ]
                },
            }
        ],
    }

    result = evidence_gate_node(state)

    assert result["evidence_gate_passed"] is True



