import pytest

from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.parsers.person_page import parse_person_page


def test_parse_person_infobox():
    html_content = """
    <div class="mw-parser-output">
    <p>This is an introduction paragraph.</p>
    <p>This is another paragraph.</p>
    <table class="infobox">
    <tr><th>Geboren</th><td><time datetime="1980-01-01">1. Januar 1980</time></td></tr>
    <tr><th>Gestorben</th><td><time datetime="2020-12-31">31. Dezember 2020</time></td></tr>
    </table>
    </div>
    """
    response = MediaWikiParseResponse(
        parse={"pageid": 111, "revid": 222, "title": "Test_Person", "text": {"*": html_content}},
        page_id=111,
        revision_id=222,
        page_title="Test_Person",
        html=html_content,
        displaytitle="Test Person",
    )

    person = parse_person_page(response)

    assert person.wikipedia_title == "Test_Person"
    assert person.birth_date == "1980-01-01"
    assert person.death_date == "2020-12-31"
    assert "introduction" in person.intro.lower() or person.intro

