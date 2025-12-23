import asyncio
import time
from typing import Any, Dict, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from scraper.config import get_settings

settings = get_settings()


class MediaWikiClient:
    BASE_URL = "https://de.wikipedia.org/w/api.php"

    def __init__(self, rate_limit_rps: float = 2.0, user_agent: Optional[str] = None):
        self.rate_limit_rps = rate_limit_rps
        self.user_agent = user_agent or settings.mediawiki_user_agent
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()

    async def _rate_limit(self) -> None:
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            min_interval = 1.0 / self.rate_limit_rps
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def fetch_parse(
        self, page_title: str, include_sections: bool = False
    ) -> Dict[str, Any]:
        await self._rate_limit()

        params: Dict[str, Any] = {
            "action": "parse",
            "page": page_title,
            "prop": "text|revid|displaytitle",
            "format": "json",
        }
        if include_sections:
            params["prop"] += "|sections"

        headers = {"User-Agent": self.user_agent}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self.BASE_URL, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def fetch_query(
        self, page_title: str
    ) -> Dict[str, Any]:
        await self._rate_limit()

        params: Dict[str, Any] = {
            "action": "query",
            "prop": "info|revisions",
            "titles": page_title,
            "rvprop": "ids|timestamp",
            "format": "json",
        }

        headers = {"User-Agent": self.user_agent}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self.BASE_URL, params=params, headers=headers)
            response.raise_for_status()
            return response.json()


def get_client() -> MediaWikiClient:
    return MediaWikiClient(
        rate_limit_rps=settings.scraper_rate_limit_rps,
        user_agent=settings.mediawiki_user_agent,
    )

