from __future__ import annotations

import json
from typing import Any

import meilisearch

from pis.settings import PisSettings


def _compact_hit(hit: dict[str, Any]) -> dict[str, Any]:
    external_ids = hit.get("external_ids") or {}
    prov = hit.get("provenance") or {}
    sources = prov.get("sources") or []
    source_urls: list[str] = []
    for s in sources:
        if isinstance(s, dict):
            for u in s.get("source_urls") or []:
                if isinstance(u, str) and u and u not in source_urls:
                    source_urls.append(u)

    return {
        "pis_person_id": hit.get("pis_person_id"),
        "display_name": hit.get("display_name"),
        "aliases": hit.get("aliases") or [],
        "birth_date": hit.get("birth_date"),
        "death_date": hit.get("death_date"),
        "external_ids": {
            "wikidata_qid": external_ids.get("wikidata_qid"),
            "wikipedia_title": external_ids.get("wikipedia_title"),
            "wikipedia_pageid": external_ids.get("wikipedia_pageid"),
            "dip_person_id": external_ids.get("dip_person_id"),
        },
        "persona_summary": hit.get("persona_summary"),
        "facts": hit.get("facts") or {},
        "source_urls": source_urls,
    }


def retrieve_persons(
    *,
    query: str,
    limit: int = 5,
    filter_expr: str | None = None,
    settings: PisSettings | None = None,
) -> dict[str, Any]:
    """Keyword retrieval against Meilisearch `pis_persons` index.

    Returns a compact JSON context package for direct LLM use.
    """
    s = settings or PisSettings()
    client = meilisearch.Client(s.meili_url, s.meili_master_key)
    idx = client.index("pis_persons")

    opts: dict[str, Any] = {"limit": int(limit)}
    if filter_expr:
        opts["filter"] = filter_expr

    res = idx.search(query, opts)
    hits = res.get("hits") or []
    compact = [_compact_hit(h) for h in hits if isinstance(h, dict)]

    return {
        "query": query,
        "limit": limit,
        "filter": filter_expr,
        "count": len(compact),
        "persons": compact,
    }


def dumps_context(context: dict[str, Any]) -> str:
    return json.dumps(context, ensure_ascii=False, indent=2, default=str)

