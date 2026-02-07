from datetime import date, datetime

from pis.models import OfficeRole, PersonSource, SourceSystem, TimeInterval
from pis.qa.temporal import find_office_role_overlaps, interval_overlaps


def test_interval_overlaps_open_end():
    a = TimeInterval(start_date=date(2020, 1, 1), end_date=None)
    b = TimeInterval(start_date=date(2021, 1, 1), end_date=date(2021, 12, 31))
    assert interval_overlaps(a, b) is True


def test_find_office_role_overlaps_detects_overlap():
    src = PersonSource(
        source_system=SourceSystem.DIP,
        source_person_id="1",
        fetched_at=datetime(2026, 1, 1),
    )
    r1 = OfficeRole(
        pis_office_role_id="r1",
        pis_person_id="p1",
        level="federal",
        office_title="X",
        interval=TimeInterval(start_date=date(2020, 1, 1), end_date=date(2020, 12, 31)),
        sources=[src],
    )
    r2 = OfficeRole(
        pis_office_role_id="r2",
        pis_person_id="p1",
        level="federal",
        office_title="Y",
        interval=TimeInterval(start_date=date(2020, 6, 1), end_date=date(2021, 1, 1)),
        sources=[src],
    )
    issues = find_office_role_overlaps("p1", [r1, r2])
    assert len(issues) == 1

