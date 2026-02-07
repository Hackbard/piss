from __future__ import annotations

from dataclasses import dataclass

from pis.models import Person


@dataclass(frozen=True)
class DedupeResult:
    unique_persons: list[Person]
    dupe_persons: list[Person]


def dedupe_persons_by_pis_id(persons: list[Person]) -> DedupeResult:
    """Enforce canonical uniqueness by `pis_person_id`.

    - Keeps the first occurrence
    - Returns duplicates separately for reporting
    """
    seen: set[str] = set()
    unique: list[Person] = []
    dupes: list[Person] = []
    for p in persons:
        if p.pis_person_id in seen:
            dupes.append(p)
            continue
        seen.add(p.pis_person_id)
        unique.append(p)
    return DedupeResult(unique_persons=unique, dupe_persons=dupes)

