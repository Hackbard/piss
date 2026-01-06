"""Configuration for LangGraph Orchestrator (Env-first, prod-tauglich)."""

import os
from enum import Enum
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class PolicyMode(str, Enum):
    """Policy mode enum values."""

    NEUTRAL_STRICT = "NEUTRAL_STRICT"
    NEUTRAL_LENIENT = "NEUTRAL_LENIENT"
    OFF = "OFF"


class OrchestratorConfig(BaseSettings):
    """Configuration for LangGraph Orchestrator."""

    model_config = SettingsConfigDict(
        env_prefix="PISS_",
        case_sensitive=False,
        extra="ignore",
    )

    tool_base_url: str = "http://localhost:8000/api/tools"
    tool_timeout_seconds: float = 20.0
    tool_strict_evidence: bool = True

    ollama_base_url: str = "http://192.168.178.185:11434/v1"
    ollama_model: str = "ministral-3:14b"

    openai_api_key: str = "ollama"

    langsmith_tracing: bool = False
    langsmith_endpoint: Optional[str] = None
    langsmith_api_key: Optional[str] = None
    langsmith_project: Optional[str] = None

    policy_mode: str = PolicyMode.NEUTRAL_STRICT
    debug_explain_queries: bool = False
    debug_include_raw_tool_payloads: bool = False
    response_sections: bool = True
    max_sources: int = 20
    disallowed_phrases_strict: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.langsmith_tracing:
            self._setup_langsmith()

    def _setup_langsmith(self) -> None:
        """Setup LangSmith tracing if enabled."""
        if self.langsmith_api_key:
            os.environ["LANGCHAIN_TRACING_V2"] = "true"
            os.environ["LANGCHAIN_ENDPOINT"] = self.langsmith_endpoint or "https://api.smith.langchain.com"
            os.environ["LANGCHAIN_API_KEY"] = self.langsmith_api_key
            if self.langsmith_project:
                os.environ["LANGCHAIN_PROJECT"] = self.langsmith_project


def get_config() -> OrchestratorConfig:
    """Get orchestrator configuration."""
    return OrchestratorConfig()



