from datetime import datetime, timezone

from pis.models import ExternalPersonIds, Person, PersonSource, SourceSystem
from pis.reconcile.wikidata_dip import reconcile_wikidata_dip


def _wd(pid: str, name: str) -> Person:
    now = datetime.now(tz=timezone.utc)
    return Person(
        pis_person_id=pid,
        display_name=name,
        created_at=now,
        updated_at=now,
        external_ids=ExternalPersonIds(wikidata_qid="Q1"),
        sources=[PersonSource(source_system=SourceSystem.WIKIDATA, source_person_id="Q1", fetched_at=now)],
    )


def _dip(pid: str, name: str) -> Person:
    now = datetime.now(tz=timezone.utc)
    return Person(
        pis_person_id=pid,
        display_name=name,
        created_at=now,
        updated_at=now,
        external_ids=ExternalPersonIds(dip_person_id=1),
        sources=[PersonSource(source_system=SourceSystem.DIP, source_person_id="1", fetched_at=now)],
    )


def test_reconcile_accepts_unique_exact_name_match_and_merges_sources():
    wd = _wd("wd1", "Max Mustermann")
    dip = _dip("dip1", "Max Mustermann")

    canonical, report = reconcile_wikidata_dip(wikidata_persons=[wd], dip_persons=[dip])
    assert len(report.accepted_links) == 1
    assert report.accepted_links[0].wikidata_pis_person_id == "wd1"
    merged = [p for p in canonical if p.pis_person_id == "wd1"][0]
    assert len(merged.sources) == 2

