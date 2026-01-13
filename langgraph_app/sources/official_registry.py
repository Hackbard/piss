from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass(frozen=True)
class OfficialTermEntry:
    parliament_id: str
    term_number: int
    start_date: Optional[str]
    end_date: Optional[str]
    evidence_urls: list[str]
    source_meta: dict[str, Any]


def load_official_registry(path: Path) -> list[OfficialTermEntry]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []

    parliaments = payload.get("parliaments")
    if not isinstance(parliaments, dict):
        return []

    entries: list[OfficialTermEntry] = []
    for parliament_id, cfg in parliaments.items():
        if not isinstance(cfg, dict):
            continue

        terms = cfg.get("terms")
        if not isinstance(terms, list):
            legacy = cfg.get("term_sources")
            if isinstance(legacy, list):
                terms = legacy
        if not isinstance(terms, list):
            continue

        for t in terms:
            if not isinstance(t, dict):
                continue
            term_number = t.get("term_number")
            if not isinstance(term_number, int):
                continue

            evidence_urls = t.get("evidence_urls") or []
            if not isinstance(evidence_urls, list):
                evidence_urls = []

            source_meta = t.get("source_meta") or {}
            if not isinstance(source_meta, dict):
                source_meta = {}

            entries.append(
                OfficialTermEntry(
                    parliament_id=str(parliament_id),
                    term_number=term_number,
                    start_date=t.get("start_date"),
                    end_date=t.get("end_date"),
                    evidence_urls=[str(u) for u in evidence_urls if isinstance(u, str) and u.strip()],
                    source_meta=source_meta,
                )
            )

    return entries

