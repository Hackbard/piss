from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from pis.settings import PisSettings
from pis.utils.http_cache import CachedHttpResponse, cached_get_json


@dataclass(frozen=True)
class DipPersonRow:
    dip_person_id: int
    vorname: str | None
    nachname: str | None
    namenszusatz: str | None
    titel: str | None
    fraktion: str | None
    wahlperiode: list[int]


@dataclass(frozen=True)
class DipPersonPage:
    num_found: int | None
    cursor: str | None
    persons: list[DipPersonRow]


def parse_person_page(data: dict[str, Any]) -> DipPersonPage:
    docs = data.get("documents", [])
    persons: list[DipPersonRow] = []
    if isinstance(docs, list):
        for d in docs:
            if not isinstance(d, dict) or "id" not in d:
                continue
            persons.append(
                DipPersonRow(
                    dip_person_id=int(d["id"]),
                    vorname=d.get("vorname"),
                    nachname=d.get("nachname"),
                    namenszusatz=d.get("namenszusatz"),
                    titel=d.get("titel"),
                    fraktion=d.get("fraktion"),
                    wahlperiode=list(d.get("wahlperiode") or []),
                )
            )
    return DipPersonPage(
        num_found=int(data.get("numFound")) if data.get("numFound") is not None else None,
        cursor=data.get("cursor"),
        persons=persons,
    )


def fetch_person_pages(
    *,
    settings: PisSettings,
    wahlperioden: list[int],
    limit: int = 100,
    force: bool = False,
    max_pages: int | None = None,
) -> list[CachedHttpResponse]:
    """Fetch DIP /person pages for the given wahlperioden (cached, paginated)."""
    settings.ensure_dirs()
    if not settings.dip_api_key:
        raise ValueError("Missing DIP API key. Set DIP_API_KEY (env) for DIP access.")

    base_url = settings.dip_base_url.rstrip("/")
    url = f"{base_url}/person"
    headers = {"authorization": f"ApiKey {settings.dip_api_key}", "accept": "application/json"}

    pages: list[CachedHttpResponse] = []
    cursor: str | None = None
    page_no = 0
    last_request_at = {"t": 0.0}

    with httpx.Client() as client:
        while True:
            params: dict[str, Any] = {"limit": int(limit), "f.wahlperiode": [int(w) for w in wahlperioden]}
            if cursor:
                params["cursor"] = cursor

            cached = cached_get_json(
                client=client,
                cache_dir=settings.pis_cache_dir / "dip" / "person",
                url=url,
                params=params,
                headers=headers,
                timeout_seconds=settings.pis_http_timeout_seconds,
                force=force,
                rate_limit_rps=settings.pis_rate_limit_rps,
                _last_request_at=last_request_at,
            )
            pages.append(cached)

            parsed = parse_person_page(cached.data)

            page_no += 1
            if max_pages is not None and page_no >= max_pages:
                break
            if not parsed.cursor or parsed.cursor == cursor:
                break
            cursor = str(parsed.cursor)

    return pages

