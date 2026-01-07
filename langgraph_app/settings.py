"""Minimal env-driven settings for the LangGraph MVP runner."""

from __future__ import annotations

import os


def env(key: str, default: str) -> str:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    return value


def env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


TOOL_BASE_URL: str = env("PISS_TOOL_BASE_URL", "http://localhost:8000/api/tools")
OLLAMA_BASE_URL: str = env("OLLAMA_BASE_URL", "http://192.168.178.185:11434/v1")
OLLAMA_MODEL: str = env("OLLAMA_MODEL", "ministral-3:14b")
STRICT_EVIDENCE_DEFAULT: bool = env_bool("PISS_STRICT_EVIDENCE_DEFAULT", True)


