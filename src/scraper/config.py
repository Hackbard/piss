import os
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    neo4j_uri: str = Field(default="bolt://localhost:7687", alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(default="password", alias="NEO4J_PASSWORD")

    meili_url: str = Field(default="http://localhost:7700", alias="MEILI_URL")
    meili_master_key: Optional[str] = Field(default=None, alias="MEILI_MASTER_KEY")

    mediawiki_user_agent: str = Field(
        default="WikipediaParlamenteScraper/0.1.0 (https://github.com/example/scraper)",
        alias="MEDIAWIKI_USER_AGENT",
    )

    scraper_rate_limit_rps: float = Field(default=2.0, alias="SCRAPER_RATE_LIMIT_RPS")
    scraper_cache_dir: Path = Field(default=Path("/data/cache"), alias="SCRAPER_CACHE_DIR")
    scraper_export_dir: Path = Field(default=Path("/data/exports"), alias="SCRAPER_EXPORT_DIR")
    scraper_registry_path: Path = Field(default=Path("/app/config/landtage_registry.yaml"), alias="SCRAPER_REGISTRY_PATH")
    scraper_seeds_landtage_path: Path = Field(default=Path("/data/exports/seeds_landtage.yaml"), alias="SCRAPER_SEEDS_LANDTAGE_PATH")

    dip_api_key: Optional[str] = Field(default=None, alias="DIP_API_KEY")
    dip_base_url: str = Field(
        default="https://search.dip.bundestag.de/api/v1", alias="DIP_BASE_URL"
    )
    dip_max_wahlperiode: int = Field(default=50, alias="DIP_MAX_WAHLPERIODE")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraper_cache_dir = Path(self.scraper_cache_dir)
        self.scraper_export_dir = Path(self.scraper_export_dir)
        self.scraper_registry_path = Path(self.scraper_registry_path)
        self.scraper_seeds_landtage_path = Path(self.scraper_seeds_landtage_path)
        self.scraper_cache_dir.mkdir(parents=True, exist_ok=True)
        self.scraper_export_dir.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    return Settings()

