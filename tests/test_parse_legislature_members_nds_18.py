import json
from pathlib import Path

import pytest

from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.parsers.legislature_members import parse_legislature_members


@pytest.fixture
def sample_response_18():
    html_content = """
    <table class="wikitable sortable">
    <tr><th>Name</th><th>Partei</th><th>Wahlkreis</th></tr>
    <tr><td><a href="/wiki/Test_Person_18" title="Test Person 18">Test Person 18</a></td><td>CDU</td><td>Braunschweig</td></tr>
    </table>
    """
    return MediaWikiParseResponse(
        parse={"pageid": 789012, "revid": 67890, "title": "Test Page 18", "text": {"*": html_content}},
        page_id=789012,
        revision_id=67890,
        page_title="Test Page 18",
        html=html_content,
        displaytitle="Test Page 18",
    )


def test_parse_legislature_members_nds_18(sample_response_18, tmp_path, monkeypatch):
    import scraper.cache.mediawiki_cache as cache_module

    seeds_content = """
nds_lt_18:
  key: nds_lt_18
  page_title: "Test Page 18"
  expected_time_range:
    start: "2017-11-15"
    end: "2022-10-08"
  hints:
    parliament: "Niedersächsischer Landtag"
    state: "Niedersachsen"
    legislature_number: 18
"""
    seeds_file = tmp_path / "seeds.yaml"
    seeds_file.write_text(seeds_content)
    monkeypatch.setattr(cache_module, "SEEDS_FILE", seeds_file)

    result = parse_legislature_members(sample_response_18, "nds_lt_18")

    assert result.seed_key == "nds_lt_18"
    assert len(result.members) > 0
    person, mandate = result.members[0]
    assert "Test Person 18" in person.name
    assert mandate.party_name == "CDU"

