"""Tests for the minimal members.list MVP runner."""

import os

import httpx
import pytest

from langgraph_app.graph import members_list_plan_node
from langgraph_app.tools import members_list


def test_plan_node_default_question_parses_tool_input():
    state = {
        "question": "Gib mir alle SPD-Abgeordneten im Landtag Niedersachsen zwischen 2014 und 2020.",
        "tool_input": None,
        "tool_result": None,
        "answer": None,
    }

    out = members_list_plan_node(state)

    assert out["answer"] is None
    assert out["tool_input"] == {
        "parliament_id": "NI",
        "party_code": "SPD",
        "from_date": "2014-01-01",
        "to_date": "2020-12-31",
        "limit": 200,
        "offset": 0,
        "strict_evidence": True,
    }


@pytest.mark.integration
def test_members_list_integration_hits_tool_gateway_if_available():
    base_url = os.getenv("PISS_TOOL_BASE_URL", "http://localhost:8000/api/tools")
    url = f"{base_url.rstrip('/')}/members/list"

    try:
        httpx.get(url, timeout=1.5)
    except httpx.RequestError:
        pytest.skip("Tool-Gateway nicht erreichbar (PISS_TOOL_BASE_URL).")

    result = members_list(
        parliament_id="NI",
        party_code="SPD",
        from_date="2014-01-01",
        to_date="2020-12-31",
        limit=5,
        offset=0,
        strict_evidence=True,
    )

    assert isinstance(result, dict)


