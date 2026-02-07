from __future__ import annotations

from datetime import datetime, timezone

from pis.ids import pis_person_id_from_dip_person_id
from pis.ingest.dip import DipPersonRow
from pis.models import ExternalPersonIds, Person, PersonSource, SourceSystem


def _display_name(row: DipPersonRow) -> str:
    parts: list[str] = []
    if row.titel:
        parts.append(str(row.titel).strip())
    name_parts: list[str] = []
    if row.vorname:
        name_parts.append(str(row.vorname).strip())
    if row.nachname:
        name_parts.append(str(row.nachname).strip())
    if row.namenszusatz:
        name_parts.append(str(row.namenszusatz).strip())
    if name_parts:
        parts.append(" ".join(name_parts))
    return " ".join([p for p in parts if p]).strip() or f"DIP:{row.dip_person_id}"


def dip_row_to_person(
    *,
    row: DipPersonRow,
    fetched_at: datetime,
    raw_snapshot_path: str | None,
    normalized_snapshot_path: str | None,
    source_url: str | None,
) -> Person:
    pis_person_id = pis_person_id_from_dip_person_id(row.dip_person_id)
    now = datetime.now(tz=timezone.utc)
    return Person(
        pis_person_id=pis_person_id,
        display_name=_display_name(row),
        aliases=[],
        external_ids=ExternalPersonIds(dip_person_id=row.dip_person_id),
        sources=[
            PersonSource(
                source_system=SourceSystem.DIP,
                source_person_id=str(row.dip_person_id),
                fetched_at=fetched_at,
                source_urls=[source_url] if source_url else [],
                raw_snapshot_path=raw_snapshot_path,
                normalized_snapshot_path=normalized_snapshot_path,
                extra={"fraktion": row.fraktion, "wahlperiode": row.wahlperiode},
            )
        ],
        memberships=[],
        office_roles=[],
        facts={"dip_fraktion": row.fraktion, "dip_wahlperioden": row.wahlperiode},
        created_at=now,
        updated_at=now,
    )

