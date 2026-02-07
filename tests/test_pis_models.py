from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from pis.models import Person, PersonSource, SourceSystem, TimeInterval


def test_time_interval_rejects_inverted_range():
    with pytest.raises(ValidationError):
        TimeInterval(start_date=date(2020, 1, 2), end_date=date(2020, 1, 1))


def test_person_requires_at_least_one_source():
    with pytest.raises(ValidationError):
        Person(
            pis_person_id="pis:person:1",
            display_name="Test Person",
            created_at=datetime.now(tz=timezone.utc),
            updated_at=datetime.now(tz=timezone.utc),
            sources=[],
        )


def test_person_accepts_sources():
    p = Person(
        pis_person_id="pis:person:1",
        display_name="Test Person",
        created_at=datetime.now(tz=timezone.utc),
        updated_at=datetime.now(tz=timezone.utc),
        sources=[
            PersonSource(
                source_system=SourceSystem.WIKIDATA,
                source_person_id="Q1",
                fetched_at=datetime.now(tz=timezone.utc),
            )
        ],
    )
    assert p.pis_person_id == "pis:person:1"

