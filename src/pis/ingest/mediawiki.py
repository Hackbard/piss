from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from pis.settings import PisSettings
from pis.utils.http_cache import CachedHttpResponse, cached_get_json

MEDIAWIKI_API_URL = "https://de.wikipedia.org/w/api.php"


@dataclass(frozen=True)
class MediaWikiIntro:
    pageid: int
    title: str
    url: str | None
    extract: str | None


def _parse_intro(data: dict[str, Any]) -> MediaWikiIntro | None:
    pages = data.get("query", {}).get("pages", {})
    if not isinstance(pages, dict) or not pages:
        return None
    # MediaWiki returns a dict keyed by pageid (as str) or "-1"
    page = next(iter(pages.values()))
    if not isinstance(page, dict) or page.get("missing") is not None:
        return None
    pageid = int(page.get("pageid"))
    title = str(page.get("title"))
    url = page.get("canonicalurl") or page.get("fullurl")
    extract = page.get("extract")
    return MediaWikiIntro(
        pageid=pageid,
        title=title.replace(" ", "_"),
        url=str(url) if url else None,
        extract=str(extract) if extract else None,
    )


def fetch_intro(
    *,
    settings: PisSettings,
    title: str,
    force: bool = False,
) -> tuple[CachedHttpResponse, MediaWikiIntro | None]:
    settings.ensure_dirs()
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts|info",
        "exintro": 1,
        "explaintext": 1,
        "inprop": "url",
        "redirects": 1,
        "titles": title.replace("_", " "),
    }
    headers = {
        "accept": "application/json",
        "user-agent": "PIS/0.1 (+https://github.com/Hackbard/piss)",
    }
    last_request_at = {"t": 0.0}
    with httpx.Client() as client:
        cached = cached_get_json(
            client=client,
            cache_dir=settings.pis_cache_dir / "mediawiki",
            url=MEDIAWIKI_API_URL,
            params=params,
            headers=headers,
            timeout_seconds=settings.pis_http_timeout_seconds,
            force=force,
            rate_limit_rps=settings.pis_rate_limit_rps,
            _last_request_at=last_request_at,
        )
    return cached, _parse_intro(cached.data)

