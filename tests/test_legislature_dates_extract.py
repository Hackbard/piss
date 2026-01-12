from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.parsers.legislature_dates import extract_legislature_dates


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


def test_extracts_from_prose_iso_and_ddmm():
    wt = "Die Wahlperiode begann am 2017-10-15 und endete am 08.11.2022."
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date == "2017-10-15"
    assert dates.end_date == "2022-11-08"


def test_extracts_range_in_parentheses_with_month_year_as_raw():
    wt = "Diese Wahlperiode (November 1986 &#160;–&#160; Mai 1987) war kurz."
    dates = extract_legislature_dates(_response(wikitext=wt))
    assert dates.start_date is None
    assert dates.end_date is None
    assert dates.start_date_raw == "November 1986"
    assert dates.end_date_raw == "Mai 1987"


