import pytest

from scraper.models.domain import DipPersonRecord, WikipediaPersonRecord
from scraper.reconcile.wiki_dip import reconcile_wiki_dip


def test_reconcile_ruleset_v1_ambiguous_pending():
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

    dip_record1 = DipPersonRecord(
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

    dip_record2 = DipPersonRecord(
        id="dip-2",
        dip_person_id=11000002,
        vorname="Max",
        nachname="Mustermann",
        namenszusatz="Jr.",
        titel=None,
        fraktion="SPD",
        wahlperiode=[19],
        person_roles=None,
        evidence_ids=["ev3"],
    )

    canonical_persons, assertions = reconcile_wiki_dip(
        [wiki_record], [dip_record1, dip_record2], use_overrides=False
    )

    assert len(canonical_persons) == 0
    assert len(assertions) >= 1
    assert all(a.status == "pending" for a in assertions)

