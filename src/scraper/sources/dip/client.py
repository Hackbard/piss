import asyncio
import time
from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from scraper.config import get_settings

settings = get_settings()


class DipClient:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        rate_limit_rps: float = 2.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.rate_limit_rps = rate_limit_rps
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

    def _get_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"ApiKey {self.api_key}"
        return headers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def fetch_person_list(
        self,
        wahlperiode: Optional[List[int]] = None,
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        await self._rate_limit()

        params: Dict[str, Any] = {"limit": limit}
        if wahlperiode:
            params["f.wahlperiode"] = wahlperiode
        if cursor:
            params["cursor"] = cursor

        headers = self._get_headers()

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.base_url}/person", params=params, headers=headers
            )
            response.raise_for_status()
            return response.json()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def fetch_person_detail(self, person_id: int) -> Dict[str, Any]:
        await self._rate_limit()

        params: Dict[str, Any] = {}
        if self.api_key:
            params["apikey"] = self.api_key

        headers = self._get_headers()

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.base_url}/person/{person_id}", params=params, headers=headers
            )
            response.raise_for_status()
            return response.json()


def get_dip_client() -> DipClient:
    return DipClient(
        base_url=settings.dip_base_url,
        api_key=settings.dip_api_key,
        rate_limit_rps=settings.scraper_rate_limit_rps,
    )

