import yaml
from pathlib import Path
from unittest.mock import patch

import pytest

from scraper.models.domain import DipPersonRecord, WikipediaPersonRecord
from scraper.reconcile.wiki_dip import reconcile_wiki_dip


def test_link_overrides_apply(tmp_path, monkeypatch):
    overrides_path = tmp_path / "link_overrides.yaml"
    overrides_path.write_text(
        """
overrides:
  "Max_Mustermann":
    dip_person_id: 11000001
    status: "accepted"
    reason: "Manual override test"
"""
    )

    monkeypatch.setattr("scraper.reconcile.wiki_dip.Path", lambda x: overrides_path if "link_overrides" in str(x) else Path(x))

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

    canonical_persons, assertions = reconcile_wiki_dip([wiki_record], [dip_record], use_overrides=True)

    assert len(canonical_persons) == 1
    assert len(assertions) == 1
    assert assertions[0].status == "accepted"
    assert assertions[0].method == "override"
    assert assertions[0].score == 1.0

