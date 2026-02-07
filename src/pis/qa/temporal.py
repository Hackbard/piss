from __future__ import annotations

from dataclasses import dataclass

from pis.models import OfficeRole, TimeInterval


def interval_overlaps(a: TimeInterval, b: TimeInterval) -> bool:
    """Inclusive overlap; open-ended end_date counts as infinity."""
    if not a.start_date or not b.start_date:
        return False
    a_end = a.end_date
    b_end = b.end_date
    # open-ended interval overlaps if it starts before the other ends
    if a_end is None and b_end is None:
        return True
    if a_end is None:
        return a.start_date <= (b_end or a.start_date)
    if b_end is None:
        return b.start_date <= a_end
    return a.start_date <= b_end and b.start_date <= a_end


@dataclass(frozen=True)
class OverlapIssue:
    pis_person_id: str
    kind: str  # e.g. "office_role"
    left_id: str
    right_id: str


def find_office_role_overlaps(pis_person_id: str, roles: list[OfficeRole]) -> list[OverlapIssue]:
    issues: list[OverlapIssue] = []
    for i in range(len(roles)):
        for j in range(i + 1, len(roles)):
            if interval_overlaps(roles[i].interval, roles[j].interval):
                issues.append(
                    OverlapIssue(
                        pis_person_id=pis_person_id,
                        kind="office_role",
                        left_id=roles[i].pis_office_role_id,
                        right_id=roles[j].pis_office_role_id,
                    )
                )
    return issues

