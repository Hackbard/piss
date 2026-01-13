from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx


@dataclass(frozen=True)
class WikidataTimeValue:
    value_iso: Optional[str]
    precision: int
    raw: Optional[str]


@dataclass(frozen=True)
class WikidataTerm:
    qid: str
    parliament_id: Optional[str]
    term_number: Optional[int]
    name: Optional[str]
    start: WikidataTimeValue
    end: WikidataTimeValue
    evidence_url: str
    source_meta: dict[str, Any]


WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITYDATA = "https://www.wikidata.org/wiki/Special:EntityData"


def _normalize_qid(qid: str) -> str:
    q = qid.strip()
    if not q.upper().startswith("Q"):
        raise ValueError(f"Invalid QID: {qid!r}")
    return q.upper()


def _time_to_iso_day(time_value: str) -> Optional[str]:
    v = (time_value or "").strip()
    if not v:
        return None
    if v.startswith("+"):
        v = v[1:]
    if "T" in v:
        v = v.split("T", 1)[0]
    if v == "0000-00-00":
        return None
    return v if len(v) == 10 else None


def _extract_time(claims: dict[str, Any], prop: str) -> WikidataTimeValue:
    if prop not in claims:
        return WikidataTimeValue(value_iso=None, precision=0, raw=None)

    snaks = claims.get(prop) or []
    if not isinstance(snaks, list) or not snaks:
        return WikidataTimeValue(value_iso=None, precision=0, raw=None)

    mainsnak = snaks[0].get("mainsnak") if isinstance(snaks[0], dict) else None
    datavalue = mainsnak.get("datavalue") if isinstance(mainsnak, dict) else None
    value = datavalue.get("value") if isinstance(datavalue, dict) else None
    if not isinstance(value, dict):
        return WikidataTimeValue(value_iso=None, precision=0, raw=None)

    time_value = value.get("time")
    precision = int(value.get("precision") or 0)
    raw = time_value if isinstance(time_value, str) else None

    value_iso = _time_to_iso_day(raw) if precision == 11 else None
    return WikidataTimeValue(value_iso=value_iso, precision=precision, raw=raw)


def fetch_lastrevid(qid: str, client: Optional[httpx.Client] = None) -> int:
    q = _normalize_qid(qid)
    close_client = False
    if client is None:
        client = httpx.Client(timeout=30.0)
        close_client = True
    try:
        r = client.get(WIKIDATA_API, params={"action": "wbgetentities", "ids": q, "format": "json"})
        r.raise_for_status()
        data = r.json()
        entity = (data.get("entities") or {}).get(q) or {}
        lastrevid = entity.get("lastrevid")
        if not isinstance(lastrevid, int):
            raise ValueError(f"No lastrevid for {q}")
        return lastrevid
    finally:
        if close_client:
            client.close()


def fetch_entity_pinned(qid: str, revision: int, client: Optional[httpx.Client] = None) -> dict[str, Any]:
    q = _normalize_qid(qid)
    url = f"{WIKIDATA_ENTITYDATA}/{q}.json"
    close_client = False
    if client is None:
        client = httpx.Client(timeout=30.0)
        close_client = True
    try:
        r = client.get(url, params={"revision": revision})
        r.raise_for_status()
        return r.json()
    finally:
        if close_client:
            client.close()


def parse_term_from_entitydata(qid: str, revision: int, entitydata: dict[str, Any]) -> WikidataTerm:
    q = _normalize_qid(qid)
    entity = ((entitydata.get("entities") or {}).get(q)) or {}
    claims = entity.get("claims") or {}
    if not isinstance(claims, dict):
        claims = {}

    start = _extract_time(claims, "P580")  # start time
    end = _extract_time(claims, "P582")  # end time
    if start.raw is None:
        start = _extract_time(claims, "P571")  # inception
    if end.raw is None:
        end = _extract_time(claims, "P576")  # dissolved/abolished

    labels = entity.get("labels") or {}
    name = None
    if isinstance(labels, dict):
        de = labels.get("de")
        if isinstance(de, dict) and isinstance(de.get("value"), str):
            name = de["value"]

    evidence_url = f"{WIKIDATA_ENTITYDATA}/{q}.json?revision={revision}"
    return WikidataTerm(
        qid=q,
        parliament_id=None,
        term_number=None,
        name=name,
        start=start,
        end=end,
        evidence_url=evidence_url,
        source_meta={"revision": revision},
    )

