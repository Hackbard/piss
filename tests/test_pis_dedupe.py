from datetime import datetime, timezone

from pis.models import Person, PersonSource, SourceSystem
from pis.reconcile.dedupe import dedupe_persons_by_pis_id


def _p(pid: str) -> Person:
    now = datetime.now(tz=timezone.utc)
    return Person(
        pis_person_id=pid,
        display_name="X",
        created_at=now,
        updated_at=now,
        sources=[
            PersonSource(
                source_system=SourceSystem.WIKIDATA,
                source_person_id="Q1",
                fetched_at=now,
            )
        ],
    )


def test_dedupe_separates_duplicates():
    a1 = _p("a")
    a2 = _p("a")
    b = _p("b")
    res = dedupe_persons_by_pis_id([a1, b, a2])
    assert [p.pis_person_id for p in res.unique_persons] == ["a", "b"]
    assert [p.pis_person_id for p in res.dupe_persons] == ["a"]

