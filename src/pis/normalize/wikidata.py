from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from pis.ids import pis_person_id_from_wikidata_qid
from pis.ingest.wikidata import WikidataPersonRow
from pis.models import ExternalPersonIds, Person, PersonSource, SourceSystem


def _parse_date(iso: str | None) -> date | None:
    if not iso:
        return None
    try:
        return date.fromisoformat(iso[:10])
    except ValueError:
        return None


def wikidata_row_to_person(
    *,
    row: WikidataPersonRow,
    fetched_at: datetime,
    raw_snapshot_path: str | None,
    normalized_snapshot_path: str | None,
    source_url: str | None,
) -> Person:
    pis_person_id = pis_person_id_from_wikidata_qid(row.qid)
    sources = [
        PersonSource(
            source_system=SourceSystem.WIKIDATA,
            source_person_id=row.qid,
            fetched_at=fetched_at,
            source_urls=[source_url] if source_url else [],
            raw_snapshot_path=raw_snapshot_path,
            normalized_snapshot_path=normalized_snapshot_path,
            extra={"dewiki_title": row.dewiki_title},
        )
    ]
    now = datetime.now(tz=UTC)
    return Person(
        pis_person_id=pis_person_id,
        display_name=row.label,
        aliases=[],
        birth_date=_parse_date(row.birth_date),
        death_date=_parse_date(row.death_date),
        external_ids=ExternalPersonIds(
            wikidata_qid=row.qid,
            wikipedia_title=row.dewiki_title,
        ),
        sources=sources,
        memberships=[],
        office_roles=[],
        facts={},
        created_at=now,
        updated_at=now,
    )


def person_to_index_doc(person: Person) -> dict[str, Any]:
    """Convert canonical Person to a Meilisearch document (JSON-serializable)."""
    return {
        "pis_person_id": person.pis_person_id,
        "display_name": person.display_name,
        "aliases": person.aliases,
        "birth_date": person.birth_date.isoformat() if person.birth_date else None,
        "death_date": person.death_date.isoformat() if person.death_date else None,
        "external_ids": person.external_ids.model_dump(),
        "persona_summary": person.persona_summary,
        "facts": person.facts,
        "provenance": {
            "sources": [
                {
                    "source_system": s.source_system.value,
                    "source_person_id": s.source_person_id,
                    "fetched_at": s.fetched_at.isoformat(),
                    "source_urls": s.source_urls,
                    "raw_snapshot_path": s.raw_snapshot_path,
                    "normalized_snapshot_path": s.normalized_snapshot_path,
                    "extra": s.extra,
                }
                for s in person.sources
            ]
        },
        "meta": {"created_at": person.created_at.isoformat(), "updated_at": person.updated_at.isoformat()},
    }

