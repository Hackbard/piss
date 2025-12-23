import pytest

from scraper.models.domain import DipPersonRecord, WikipediaPersonRecord
from scraper.reconcile.wiki_dip import reconcile_wiki_dip


def test_reconcile_ruleset_v1_unique_match():
    wiki_record = WikipediaPersonRecord(
        id="wiki-1",
        wikipedia_title="Max_Mustermann",
        wikipedia_url="https://de.wikipedia.org/wiki/Max_Mustermann",
        page_id=123,
        revision_id=456,
        name="Max Mustermann",
        birth_date=None,
        death_date=None,
        intro=None,
        evidence_ids=["ev1"],
    )

    dip_record = DipPersonRecord(
        id="dip-1",
        dip_person_id=11000001,
        vorname="Max",
        nachname="Mustermann",
        namenszusatz=None,
        titel=None,
        fraktion="CDU/CSU",
        wahlperiode=[19],
        person_roles=None,
        evidence_ids=["ev2"],
    )

    canonical_persons, assertions = reconcile_wiki_dip([wiki_record], [dip_record], use_overrides=False)

    assert len(canonical_persons) == 1
    assert len(assertions) == 1
    assert assertions[0].status == "accepted"
    assert assertions[0].score >= 0.95
    assert canonical_persons[0].identifiers["wikipedia_title"] == "Max_Mustermann"
    assert canonical_persons[0].identifiers["dip_person_id"] == "11000001"

