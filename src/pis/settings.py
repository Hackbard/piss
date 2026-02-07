from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class PisSettings(BaseSettings):
    """Runtime configuration for the PIS package."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    meili_url: str = "http://localhost:7700"
    meili_master_key: str = "masterKey"

