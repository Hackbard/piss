from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from pis.settings import PisSettings
from pis.utils.http_cache import CachedHttpResponse, cached_get_json

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"


def _politicians_de_sparql(limit: int, offset: int) -> str:
    # Germany-only, humans, occupation politician, citizenship Germany.
    # Includes optional sitelink to dewiki (title) to enable MediaWiki enrichment.
    return f"""
SELECT ?person ?personLabel ?birth ?death ?dewikiTitle WHERE {{
  ?person wdt:P31 wd:Q5 .
  ?person wdt:P106 wd:Q82955 .
  ?person wdt:P27 wd:Q183 .
  OPTIONAL {{ ?person wdt:P569 ?birth . }}
  OPTIONAL {{ ?person wdt:P570 ?death . }}
  OPTIONAL {{
    ?dewiki schema:about ?person ;
           schema:isPartOf <https://de.wikipedia.org/> ;
           schema:name ?dewikiTitle .
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en". }}
}}
LIMIT {int(limit)}
OFFSET {int(offset)}
""".strip()


@dataclass(frozen=True)
class WikidataPersonRow:
    qid: str
    label: str
    birth_date: str | None
    death_date: str | None
    dewiki_title: str | None


def _extract_qid(uri: str) -> str:
    # e.g. http://www.wikidata.org/entity/Q123 -> Q123
    return uri.rsplit("/", 1)[-1]


def _parse_bindings(data: dict[str, Any]) -> list[WikidataPersonRow]:
    bindings = data.get("results", {}).get("bindings", [])
    out: list[WikidataPersonRow] = []
    for b in bindings:
        person_uri = b.get("person", {}).get("value")
        if not person_uri:
            continue
        qid = _extract_qid(str(person_uri))
        label = str(b.get("personLabel", {}).get("value") or qid)
        birth = b.get("birth", {}).get("value")
        death = b.get("death", {}).get("value")
        title = b.get("dewikiTitle", {}).get("value")
        out.append(
            WikidataPersonRow(
                qid=qid,
                label=label,
                birth_date=str(birth)[:10] if isinstance(birth, str) else None,
                death_date=str(death)[:10] if isinstance(death, str) else None,
                dewiki_title=str(title).replace(" ", "_") if isinstance(title, str) else None,
            )
        )
    return out


def fetch_politicians_de(
    *,
    settings: PisSettings,
    limit: int = 50,
    offset: int = 0,
    force: bool = False,
) -> tuple[CachedHttpResponse, list[WikidataPersonRow]]:
    """Fetch a page of German politicians from Wikidata SPARQL (cached)."""
    settings.ensure_dirs()
    query = _politicians_de_sparql(limit=limit, offset=offset)
    params = {"format": "json", "query": query}
    headers = {
        "accept": "application/sparql-results+json",
        "user-agent": "PIS/0.1 (+https://github.com/Hackbard/piss)",
    }
    last_request_at = {"t": 0.0}
    with httpx.Client() as client:
        cached = cached_get_json(
            client=client,
            cache_dir=settings.pis_cache_dir / "wikidata_sparql",
            url=WIKIDATA_SPARQL_URL,
            params=params,
            headers=headers,
            timeout_seconds=settings.pis_http_timeout_seconds,
            force=force,
            rate_limit_rps=settings.pis_rate_limit_rps,
            _last_request_at=last_request_at,
        )
    return cached, _parse_bindings(cached.data)

