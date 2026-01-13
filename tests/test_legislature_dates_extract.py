from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.parsers.legislature_dates import extract_legislature_dates, extract_constituting_session_date_from_text


def _response(*, wikitext: str | None = None, html: str = "") -> MediaWikiParseResponse:
    return MediaWikiParseResponse(
        parse={},
        page_id=1,
        revision_id=2,
        page_title="Test",
        html=html,
        wikitext=wikitext,
        displaytitle=None,
    )


def test_extracts_dates_from_wikitext_kv_lines_with_german_months():
    wt = """
{{Infobox}}
| Beginn = 15. Oktober 2017
| Ende = 08.11.2022
"""
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date == "2017-10-15"
    assert dates.end_date == "2022-11-08"
    assert dates.start_date_precision == "day"
    assert dates.end_date_precision == "day"


def test_stores_raw_when_only_year_is_present():
    wt = """
| Beginn = 2017
| Ende = 2022
"""
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date is None
    assert dates.end_date is None
    assert dates.start_date_raw == "2017"
    assert dates.end_date_raw == "2022"
    assert dates.start_date_precision == "year"
    assert dates.end_date_precision == "year"


def test_extracts_from_prose_iso_and_ddmm():
    wt = "Die Wahlperiode begann am 2017-10-15 und endete am 08.11.2022."
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date == "2017-10-15"
    assert dates.end_date == "2022-11-08"
    assert dates.start_date_precision == "day"
    assert dates.end_date_precision == "day"


def test_extracts_range_in_parentheses_with_month_year_as_raw():
    wt = "Diese Wahlperiode (November 1986 &#160;–&#160; Mai 1987) war kurz."
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date is None
    assert dates.end_date is None
    assert dates.start_date_raw == "November 1986"
    assert dates.end_date_raw == "Mai 1987"
    assert dates.start_date_precision == "month"
    assert dates.end_date_precision == "month"


def test_extract_constituting_session_date_german_month():
    text = "Die konstituierende Sitzung fand am 26. März 2025 statt."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso == "2025-03-26"
    assert raw is not None
    assert "konstituierende sitzung" in raw.lower()


def test_extract_constituting_session_date_numeric():
    text = "Die konstituierende Sitzung fand am 26.03.2025 statt."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso == "2025-03-26"
    assert raw is not None


def test_extract_constituting_session_date_with_ref():
    text = "Die konstituierende Sitzung fand am 26. März 2025 statt.[1]"
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso == "2025-03-26"
    assert raw is not None


def test_extract_constituting_session_date_konstituierenden():
    text = "Die konstituierenden Sitzung fand am 1. Januar 2000 statt."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso == "2000-01-01"
    assert raw is not None


def test_extract_constituting_session_date_erste_sitzung():
    text = "Die erste Sitzung wurde konstituierend am 15. Februar 2010 abgehalten."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso == "2010-02-15"
    assert raw is not None


def test_extract_constituting_session_date_konstituierung_alone():
    text = "Die Konstituierung fand am 10. Dezember 2023 statt."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso == "2023-12-10"
    assert raw is not None


def test_extract_constituting_session_date_rejects_month_only():
    text = "Die konstituierende Sitzung fand im März 2025 statt."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso is None
    assert raw is None


def test_extract_constituting_session_date_rejects_year_only():
    text = "Die konstituierende Sitzung fand 2025 statt."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso is None
    assert raw is None


def test_extract_constituting_session_date_rejects_unrelated_dates():
    text = "Die Wahl fand am 15. Oktober 2024 statt. Die konstituierende Sitzung war später."
    start_iso, raw = extract_constituting_session_date_from_text(text)
    assert start_iso is None
    assert raw is None


def test_extract_constituting_session_date_in_wikitext_lead():
    wt = "Die konstituierende Sitzung fand am 26. März 2025 statt.\n\n== Mitglieder =="
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date == "2025-03-26"
    assert dates.start_date_precision == "day"
    assert dates.start_date_raw is not None


def test_extract_constituting_session_date_takes_priority_over_other_patterns():
    wt = "Die konstituierende Sitzung fand am 26. März 2025 statt.\n\n| Beginn = 2017"
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date == "2025-03-26"
    assert dates.start_date_precision == "day"


