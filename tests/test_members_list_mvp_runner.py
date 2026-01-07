"""Tests for the minimal members.list MVP runner."""

import os

import httpx
import pytest

from langgraph_app.graph import (
    PARLIAMENT_ALIASES,
    PARTY_ALIASES,
    _merge_member_rows,
    _parse_date_range,
    _parse_members_list_tool_input,
    format_member_row,
    members_list_answer_node,
    members_list_plan_node,
)
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


def test_parse_party_aliases():
    assert _parse_members_list_tool_input("SPD Mitglieder")["party_code"] == "SPD"
    assert _parse_members_list_tool_input("cdu Mitglieder")["party_code"] == "CDU"
    assert _parse_members_list_tool_input("Die Grünen")["party_code"] == "GRUENE"
    assert _parse_members_list_tool_input("gruene Mitglieder")["party_code"] == "GRUENE"
    assert _parse_members_list_tool_input("FDP Mitglieder")["party_code"] == "FDP"
    assert _parse_members_list_tool_input("afd Mitglieder")["party_code"] == "AFD"
    assert _parse_members_list_tool_input("Linke Mitglieder")["party_code"] == "LINKE"


def test_parse_parliament_aliases():
    assert _parse_members_list_tool_input("Niedersachsen")["parliament_id"] == "NI"
    assert _parse_members_list_tool_input("Landtag Niedersachsen")["parliament_id"] == "NI"
    assert _parse_members_list_tool_input("Bundestag")["parliament_id"] == "BT"
    assert _parse_members_list_tool_input("Hessen")["parliament_id"] == "HE"
    assert _parse_members_list_tool_input("hessischer landtag")["parliament_id"] == "HE"
    assert _parse_members_list_tool_input("Bayern")["parliament_id"] == "BY"
    assert _parse_members_list_tool_input("bw")["parliament_id"] == "BW"


def test_parse_date_range_dash():
    from_date, to_date = _parse_date_range("2014-2020")
    assert from_date == "2014-01-01"
    assert to_date == "2020-12-31"


def test_parse_date_range_em_dash():
    from_date, to_date = _parse_date_range("2014–2020")
    assert from_date == "2014-01-01"
    assert to_date == "2020-12-31"


def test_parse_date_range_zwischen():
    from_date, to_date = _parse_date_range("zwischen 2014 und 2020")
    assert from_date == "2014-01-01"
    assert to_date == "2020-12-31"


def test_parse_date_range_ab():
    from datetime import datetime

    from_date, to_date = _parse_date_range("ab 2014")
    assert from_date == "2014-01-01"
    assert to_date == datetime.now().date().isoformat()


def test_parse_date_range_bis():
    from_date, to_date = _parse_date_range("bis 2020")
    assert from_date == "0001-01-01"
    assert to_date == "2020-12-31"


def test_parse_date_range_single_year():
    from_date, to_date = _parse_date_range("2018")
    assert from_date == "2018-01-01"
    assert to_date == "2018-12-31"


def test_parse_date_range_two_years():
    from_date, to_date = _parse_date_range("2018 2021")
    assert from_date == "2018-01-01"
    assert to_date == "2021-12-31"


def test_parse_tool_input_complete():
    result = _parse_members_list_tool_input("Liste CDU im Bundestag 2018-2021")
    assert result["parliament_id"] == "BT"
    assert result["party_code"] == "CDU"
    assert result["from_date"] == "2018-01-01"
    assert result["to_date"] == "2021-12-31"
    assert result["limit"] == 200
    assert result["offset"] == 0
    assert result["strict_evidence"] is True


def test_parse_tool_input_partial():
    result = _parse_members_list_tool_input("SPD")
    assert result.get("party_code") == "SPD"
    assert result.get("parliament_id") is None
    assert result.get("from_date") is None
    assert result.get("to_date") is None


def test_merge_member_rows_dedupe():
    rows = [
        {
            "person_id": "1",
            "person_name": "Test Person",
            "active_first_start_date": "2018-01-01",
            "active_last_end_date": "2020-12-31",
            "evidence_urls": ["https://example.com/1"],
        },
        {
            "person_id": "1",
            "person_name": "Test Person",
            "active_first_start_date": "2017-06-01",
            "active_last_end_date": "2021-12-31",
            "evidence_urls": ["https://example.com/2"],
        },
    ]

    merged = _merge_member_rows(rows)
    assert len(merged) == 1
    assert merged[0]["active_first_start_date"] == "2017-06-01"
    assert merged[0]["active_last_end_date"] == "2021-12-31"
    assert len(merged[0]["evidence_urls"]) == 2


def test_merge_member_rows_multiple_persons():
    rows = [
        {"person_id": "1", "person_name": "Person 1"},
        {"person_id": "2", "person_name": "Person 2"},
        {"person_id": "1", "person_name": "Person 1"},
    ]

    merged = _merge_member_rows(rows)
    assert len(merged) == 2
    person_ids = {r["person_id"] for r in merged}
    assert person_ids == {"1", "2"}


def test_merge_member_rows_max_sources():
    rows = [
        {
            "person_id": "1",
            "evidence_urls": [f"https://example.com/{i}" for i in range(30)],
        },
    ]

    merged = _merge_member_rows(rows, max_sources=10)
    assert len(merged[0]["evidence_urls"]) == 10


