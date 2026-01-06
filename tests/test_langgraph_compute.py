"""Tests for Compute Node."""

from langgraph_app.nodes.compute import compute_node
from langgraph_app.schemas import GroupBy, IntentType, Metric, UserIntent


def test_compute_seat_share_percent():
    """Test computing seat share percentage."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.LEGISLATURE_STATS,
            metrics=[Metric.SEAT_SHARE_PERCENT],
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "legislature.stats",
                "data": {
                    "total_seats": 100,
                    "party_seats": {
                        "SPD": 40,
                        "CDU": 60,
                    },
                },
            }
        ],
    }

    result = compute_node(state)

    computed = result["computed"]
    assert "seat_share_percent" in computed["computed_metrics"]
    assert computed["computed_metrics"]["seat_share_percent"]["SPD"] == 40.0
    assert computed["computed_metrics"]["seat_share_percent"]["CDU"] == 60.0


def test_compute_group_by_legislature():
    """Test grouping mandates by legislature."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            group_by=GroupBy.LEGISLATURE,
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "legislature_id": "legislature-nds-17",
                        },
                        {
                            "mandate_id": "mandate-2",
                            "legislature_id": "legislature-nds-18",
                        },
                    ]
                },
            }
        ],
    }

    result = compute_node(state)

    computed = result["computed"]
    assert computed["grouped_data"] is not None
    assert "by_legislature" in computed["grouped_data"]
    assert "legislature-nds-17" in computed["grouped_data"]["by_legislature"]
    assert "legislature-nds-18" in computed["grouped_data"]["by_legislature"]


def test_compute_group_by_party():
    """Test grouping mandates by party."""
    state = {
        "intent": UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            group_by=GroupBy.PARTY,
            strict_evidence=True,
        ),
        "tool_results": [
            {
                "tool_name": "mandates.search",
                "data": {
                    "rows": [
                        {
                            "mandate_id": "mandate-1",
                            "party_code": "SPD",
                        },
                        {
                            "mandate_id": "mandate-2",
                            "party_code": "CDU",
                        },
                    ]
                },
            }
        ],
    }

    result = compute_node(state)

    computed = result["computed"]
    assert computed["grouped_data"] is not None
    assert "by_party" in computed["grouped_data"]
    assert "SPD" in computed["grouped_data"]["by_party"]
    assert "CDU" in computed["grouped_data"]["by_party"]



