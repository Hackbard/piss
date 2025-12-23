import json
from pathlib import Path

import pytest

from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.parsers.legislature_members import parse_legislature_members


@pytest.fixture
def sample_response():
    fixture_path = Path(__file__).parent / "fixtures" / "mediawiki" / "nds_lt_17" / "12345" / "parse" / "raw.json"
    data = json.loads(fixture_path.read_text())
    return MediaWikiParseResponse(
        parse=data["parse"],
        page_id=data["parse"]["pageid"],
        revision_id=data["parse"]["revid"],
        page_title=data["parse"]["title"],
        html=data["parse"]["text"]["*"],
        displaytitle=data["parse"]["displaytitle"],
    )


def test_parse_legislature_members_nds_17(sample_response, tmp_path, monkeypatch):
    import scraper.cache.mediawiki_cache as cache_module

    seeds_content = """
nds_lt_17:
  key: nds_lt_17
  page_title: "Liste der Mitglieder des Niedersächsischen Landtages (17. Wahlperiode)"
  expected_time_range:
    start: "2013-01-20"
    end: "2017-11-14"
  hints:
    parliament: "Niedersächsischer Landtag"
    state: "Niedersachsen"
    legislature_number: 17
"""
    seeds_file = tmp_path / "seeds.yaml"
    seeds_file.write_text(seeds_content)
    monkeypatch.setattr(cache_module, "SEEDS_FILE", seeds_file)

    result = parse_legislature_members(sample_response, "nds_lt_17")

    assert result.seed_key == "nds_lt_17"
    assert len(result.members) > 0
    person, mandate = result.members[0]
    assert person.name == "Test Person"
    assert person.wikipedia_title == "Test_Person"
    assert mandate.party_name == "SPD"
    assert mandate.wahlkreis == "Hannover"

