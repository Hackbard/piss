"""Tests for the minimal members.list MVP runner."""

import os

import httpx
import pytest

from langgraph_app.graph import format_member_row, members_list_answer_node, members_list_plan_node
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


def test_format_member_row_with_active_dates_and_mandate_note():
    row = {
        "person_name": "Alptekin Kirci",
        "wikipedia_title": "Alptekin_Kirci",
        "active_first_start_date": "2017-10-15",
        "active_last_end_date": "2020-12-31",
        "first_start_date": "2017-10-15",
        "last_end_date": "2022-11-08",
    }

    result = format_member_row(row)

    assert "Alptekin Kirci" in result
    assert "Alptekin_Kirci" in result
    assert "2017-10-15" in result
    assert "2020-12-31" in result
    assert "(Mandat bis 2022-11-08)" in result
    assert result.startswith("- ")


def test_format_member_row_fallback_to_raw_dates():
    row = {
        "person_name": "Test Person",
        "wikipedia_title": "Test_Person",
        "first_start_date": "2017-11-15",
        "last_end_date": "2020-12-31",
    }

    result = format_member_row(row)

    assert "Test Person" in result
    assert "2017-11-15" in result
    assert "2020-12-31" in result
    assert "(Mandat bis" not in result


def test_format_member_row_with_open_mandate():
    row = {
        "person_name": "Open Person",
        "wikipedia_title": "Open_Person",
        "active_first_start_date": "2017-11-15",
        "active_last_end_date": None,
        "first_start_date": "2017-11-15",
        "last_end_date": None,
    }

    result = format_member_row(row)

    assert "Open Person" in result
    assert "2017-11-15" in result
    assert "… (offen)" in result


def test_format_member_row_no_mandate_note_when_dates_equal():
    row = {
        "person_name": "Test Person",
        "wikipedia_title": "Test_Person",
        "active_first_start_date": "2017-11-15",
        "active_last_end_date": "2020-12-31",
        "first_start_date": "2017-11-15",
        "last_end_date": "2020-12-31",
    }

    result = format_member_row(row)

    assert "Test Person" in result
    assert "(Mandat bis" not in result


def test_answer_node_uses_active_dates_with_mandate_note():
    state = {
        "question": "Test",
        "tool_input": {
            "parliament_id": "NI",
            "party_code": "SPD",
            "from_date": "2017-01-01",
            "to_date": "2020-12-31",
        },
        "tool_result": {
            "members": [
                {
                    "person_name": "Alptekin Kirci",
                    "wikipedia_title": "Alptekin_Kirci",
                    "active_first_start_date": "2017-10-15",
                    "active_last_end_date": "2020-12-31",
                    "first_start_date": "2017-10-15",
                    "last_end_date": "2022-11-08",
                }
            ]
        },
        "answer": None,
    }

    out = members_list_answer_node(state)

    assert out["answer"] is not None
    assert "Alptekin Kirci" in out["answer"]
    assert "Alptekin_Kirci" in out["answer"]
    assert "2017-10-15" in out["answer"]
    assert "2020-12-31" in out["answer"]
    assert "(Mandat bis 2022-11-08)" in out["answer"]


def test_answer_node_fallback_to_raw_dates_when_active_missing():
    state = {
        "question": "Test",
        "tool_input": {
            "parliament_id": "NI",
            "party_code": "SPD",
            "from_date": "2017-01-01",
            "to_date": "2020-12-31",
        },
        "tool_result": {
            "members": [
                {
                    "person_name": "Test Person",
                    "wikipedia_title": "Test_Person",
                    "first_start_date": "2017-11-15",
                    "last_end_date": "2020-12-31",
                }
            ]
        },
        "answer": None,
    }

    out = members_list_answer_node(state)

    assert out["answer"] is not None
    assert "Test Person" in out["answer"]
    assert "2017-11-15" in out["answer"]
    assert "2020-12-31" in out["answer"]
    assert "(Mandat bis" not in out["answer"]


def test_answer_node_no_mandate_note_when_dates_equal():
    state = {
        "question": "Test",
        "tool_input": {
            "parliament_id": "NI",
            "party_code": "SPD",
            "from_date": "2017-01-01",
            "to_date": "2020-12-31",
        },
        "tool_result": {
            "members": [
                {
                    "person_name": "Test Person",
                    "wikipedia_title": "Test_Person",
                    "active_first_start_date": "2017-11-15",
                    "active_last_end_date": "2020-12-31",
                    "first_start_date": "2017-11-15",
                    "last_end_date": "2020-12-31",
                }
            ]
        },
        "answer": None,
    }

    out = members_list_answer_node(state)

    assert out["answer"] is not None
    assert "Test Person" in out["answer"]
    assert "(Mandat bis" not in out["answer"]


