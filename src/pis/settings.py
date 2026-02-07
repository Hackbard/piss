from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class PisSettings(BaseSettings):
    """Runtime configuration for the PIS package."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    # Storage (repo-relative by default; override in Docker/CI as needed)
    pis_data_dir: Path = Path("data/pis")
    pis_cache_dir: Path = Path("data/pis/cache")
    pis_raw_dir: Path = Path("data/pis/raw")
    pis_normalized_dir: Path = Path("data/pis/normalized")
    pis_canonical_dir: Path = Path("data/pis/canonical")
    pis_reports_dir: Path = Path("data/pis/reports")

    # HTTP behavior
    pis_rate_limit_rps: float = 1.0
    pis_http_timeout_seconds: float = 30.0

    # Meilisearch
    meili_url: str = "http://localhost:7700"
    meili_master_key: str = "masterKey"

    # DIP (Bundestag) - official federal source
    dip_base_url: str = "https://search.dip.bundestag.de/api/v1"
    dip_api_key: str | None = None

    def ensure_dirs(self) -> None:
        for p in (
            self.pis_data_dir,
            self.pis_cache_dir,
            self.pis_raw_dir,
            self.pis_normalized_dir,
            self.pis_canonical_dir,
            self.pis_reports_dir,
        ):
            p.mkdir(parents=True, exist_ok=True)

