from datetime import UTC, datetime

from pis.models import Person, PersonSource, SourceSystem
from pis.rag.persona import build_persona_summary, ensure_persona


def test_build_persona_summary_includes_name_and_birth():
    now = datetime.now(tz=UTC)
    p = Person(
        pis_person_id="p1",
        display_name="Test Person",
        birth_date=None,
        created_at=now,
        updated_at=now,
        facts={"wikipedia_intro": "Test intro."},
        sources=[PersonSource(source_system=SourceSystem.WIKIDATA, source_person_id="Q1", fetched_at=now)],
    )
    s = build_persona_summary(p)
    assert "Test Person" in s
    assert "Test intro." in s


def test_ensure_persona_sets_persona_summary_when_missing():
    now = datetime.now(tz=UTC)
    p = Person(
        pis_person_id="p1",
        display_name="Test Person",
        created_at=now,
        updated_at=now,
        sources=[PersonSource(source_system=SourceSystem.WIKIDATA, source_person_id="Q1", fetched_at=now)],
    )
    p2 = ensure_persona(p)
    assert p2.persona_summary

