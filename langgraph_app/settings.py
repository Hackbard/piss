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
    mvp_use_llm: bool = False
    openai_api_key: str = "ollama"

    langsmith_tracing: bool = False
    langsmith_endpoint: Optional[str] = None
    langsmith_api_key: Optional[str] = None
    langsmith_project: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_langsmith_from_env()
        self._setup_langsmith_if_needed()

    def _load_langsmith_from_env(self) -> None:
        """Load LangSmith settings from .env file (without PISS_ prefix)."""
        if not self.langsmith_tracing:
            tracing = os.getenv("LANGSMITH_TRACING", "").strip().lower()
            if tracing in {"1", "true", "yes", "y", "on"}:
                self.langsmith_tracing = True
        
        if not self.langsmith_endpoint:
            endpoint = os.getenv("LANGSMITH_ENDPOINT")
            if endpoint:
                self.langsmith_endpoint = endpoint
        
        if not self.langsmith_api_key:
            api_key = os.getenv("LANGSMITH_API_KEY")
            if api_key:
                self.langsmith_api_key = api_key
        
        if not self.langsmith_project:
            project = os.getenv("LANGSMITH_PROJECT")
            if project:
                self.langsmith_project = project

    def _setup_langsmith_if_needed(self) -> None:
        """Setup LangSmith tracing if enabled and API key is provided."""
        api_key = self.langsmith_api_key
        has_valid_api_key = api_key and api_key.strip() and api_key.strip() != ""
        
        if has_valid_api_key:
            self._setup_langsmith()
        elif self.langsmith_tracing and not has_valid_api_key:
            import sys
            print(
                "[WARNING] PISS_LANGSMITH_TRACING is enabled but PISS_LANGSMITH_API_KEY is missing or empty. "
                "LangSmith tracing will not be enabled.",
                file=sys.stderr
            )

    def _setup_langsmith(self) -> None:
        """Setup LangSmith tracing if enabled."""
        api_key = self.langsmith_api_key
        if api_key and api_key.strip():
            os.environ["LANGCHAIN_TRACING_V2"] = "true"
            os.environ["LANGCHAIN_ENDPOINT"] = self.langsmith_endpoint or "https://api.smith.langchain.com"
            os.environ["LANGCHAIN_API_KEY"] = api_key.strip()
            if self.langsmith_project:
                os.environ["LANGCHAIN_PROJECT"] = self.langsmith_project


_settings = MvpSettings()

TOOL_BASE_URL: str = _settings.tool_base_url
OLLAMA_BASE_URL: str = _settings.ollama_base_url
OLLAMA_MODEL: str = _settings.ollama_model
STRICT_EVIDENCE_DEFAULT: bool = _settings.strict_evidence_default
MVP_USE_LLM: bool = _settings.mvp_use_llm
OPENAI_API_KEY: str = _settings.openai_api_key


