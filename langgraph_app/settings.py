"""Minimal env-driven settings for the LangGraph MVP runner."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_env_file() -> str | None:
    """Find .env file in project root."""
    current = Path(__file__).parent
    while current != current.parent:
        env_file = current / ".env"
        if env_file.exists():
            return str(env_file)
        current = current.parent
    return None


class MvpSettings(BaseSettings):
    """Settings for LangGraph MVP runner with .env file support."""

    model_config = SettingsConfigDict(
        env_file=_find_env_file(),
        env_file_encoding="utf-8",
        env_prefix="PISS_",
        case_sensitive=False,
        extra="ignore",
    )

    tool_base_url: str = "http://localhost:8000/api/tools"
    ollama_base_url: str = "http://192.168.178.185:11434/v1"
    ollama_model: str = "ministral-3:14b"
    strict_evidence_default: bool = True
    openai_api_key: str = "ollama"

    langsmith_tracing: bool = False
    langsmith_endpoint: Optional[str] = None
    langsmith_api_key: Optional[str] = None
    langsmith_project: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


_settings = MvpSettings()

TOOL_BASE_URL: str = _settings.tool_base_url
OLLAMA_BASE_URL: str = _settings.ollama_base_url
OLLAMA_MODEL: str = _settings.ollama_model
STRICT_EVIDENCE_DEFAULT: bool = _settings.strict_evidence_default
OPENAI_API_KEY: str = _settings.openai_api_key


