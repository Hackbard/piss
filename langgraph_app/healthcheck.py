"""Preflight healthcheck for Ollama before starting the graph."""

from __future__ import annotations

import json
import sys
import time
from typing import Any

import httpx


def _is_debug() -> bool:
    import os
    value = os.getenv("PISS_DEBUG", "0")
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def check_ollama_or_die(
    base_url: str,
    model: str,
    timeout_s: float = 5.0,
    warmup: bool = True,
) -> None:
    """Check Ollama availability and raise RuntimeError if not reachable.
    
    Args:
        base_url: Ollama OpenAI-compat base URL (e.g., "http://192.168.178.185:11434/v1")
        model: Model name (e.g., "ministral-3:14b")
        timeout_s: Timeout in seconds for the healthcheck request (default: 5.0)
        warmup: If True, retry with longer timeout (30s) on first timeout to allow model loading
        
    Raises:
        RuntimeError: If base_url or model is empty, or if Ollama is not reachable/responding correctly
    """
    if not base_url or not base_url.strip():
        raise RuntimeError(
            "Ollama base URL ist nicht gesetzt. "
            "Bitte setzen Sie PISS_OLLAMA_BASE_URL (z.B. 'http://192.168.178.185:11434/v1')."
        )
    
    if not model or not model.strip():
        raise RuntimeError(
            "Ollama model ist nicht gesetzt. "
            "Bitte setzen Sie PISS_OLLAMA_MODEL (z.B. 'ministral-3:14b')."
        )
    
    base_url = base_url.rstrip("/")
    url = f"{base_url}/chat/completions"
    
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "healthcheck"}],
        "max_tokens": 1,
        "temperature": 0,
    }
    
    start_time = time.time()
    last_error: Exception | None = None
    
    for attempt in range(2 if warmup else 1):
        current_timeout = timeout_s if attempt == 0 else 30.0
        
        try:
            with httpx.Client(timeout=current_timeout) as client:
                response = client.post(url, json=payload)
                
                if response.status_code != 200:
                    body_snippet = response.text[:400] if response.text else "(empty)"
                    raise RuntimeError(
                        f"Ollama healthcheck fehlgeschlagen:\n"
                        f"  Base URL: {base_url}\n"
                        f"  Model: {model}\n"
                        f"  HTTP Status: {response.status_code}\n"
                        f"  Response: {body_snippet}\n"
                        f"\n"
                        f"Diagnose:\n"
                        f"  1. Prüfen Sie, ob Ollama läuft: curl -sS {base_url.replace('/v1', '')}/api/tags\n"
                        f"  2. Prüfen Sie OpenAI-compat Endpoint: curl -sS -X POST {url} -H 'Content-Type: application/json' -d '{json.dumps(payload)}'\n"
                        f"  3. Prüfen Sie ENV-Variablen: PISS_OLLAMA_BASE_URL und PISS_OLLAMA_MODEL"
                    )
                
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    body_snippet = response.text[:400] if response.text else "(empty)"
                    raise RuntimeError(
                        f"Ollama healthcheck fehlgeschlagen: Ungültige JSON-Antwort\n"
                        f"  Base URL: {base_url}\n"
                        f"  Model: {model}\n"
                        f"  HTTP Status: {response.status_code}\n"
                        f"  Response (snippet): {body_snippet}\n"
                        f"  JSON Error: {e}\n"
                        f"\n"
                        f"Diagnose:\n"
                        f"  1. Prüfen Sie, ob Ollama läuft: curl -sS {base_url.replace('/v1', '')}/api/tags\n"
                        f"  2. Prüfen Sie OpenAI-compat Endpoint: curl -sS -X POST {url} -H 'Content-Type: application/json' -d '{json.dumps(payload)}'\n"
                        f"  3. Prüfen Sie ENV-Variablen: PISS_OLLAMA_BASE_URL und PISS_OLLAMA_MODEL"
                    )
                
                latency_ms = int((time.time() - start_time) * 1000)
                
                if _is_debug():
                    print(
                        f"[DEBUG] Ollama OK: {base_url} model={model} latency_ms={latency_ms}",
                        file=sys.stderr,
                    )
                
                return
        
        except httpx.TimeoutException as e:
            last_error = e
            if attempt == 0 and warmup:
                continue
            raise RuntimeError(
                f"Ollama healthcheck fehlgeschlagen: Timeout nach {current_timeout}s\n"
                f"  Base URL: {base_url}\n"
                f"  Model: {model}\n"
                f"  Timeout: {current_timeout}s\n"
                f"\n"
                f"Mögliche Ursachen:\n"
                f"  - Ollama ist nicht erreichbar (Netzwerk/Port)\n"
                f"  - Modell wird noch geladen (erster Start nach Idle)\n"
                f"\n"
                f"Diagnose:\n"
                f"  1. Prüfen Sie, ob Ollama läuft: curl -sS {base_url.replace('/v1', '')}/api/tags\n"
                f"  2. Prüfen Sie OpenAI-compat Endpoint: curl -sS -X POST {url} -H 'Content-Type: application/json' -d '{json.dumps(payload)}'\n"
                f"  3. Prüfen Sie ENV-Variablen: PISS_OLLAMA_BASE_URL und PISS_OLLAMA_MODEL\n"
                f"  4. Versuchen Sie --health-warmup (default: an) für längeres Timeout beim Modell-Load"
            ) from e
        
        except httpx.RequestError as e:
            raise RuntimeError(
                f"Ollama healthcheck fehlgeschlagen: Netzwerkfehler\n"
                f"  Base URL: {base_url}\n"
                f"  Model: {model}\n"
                f"  Fehler: {e}\n"
                f"\n"
                f"Diagnose:\n"
                f"  1. Prüfen Sie, ob Ollama läuft: curl -sS {base_url.replace('/v1', '')}/api/tags\n"
                f"  2. Prüfen Sie Netzwerkverbindung und Port\n"
                f"  3. Prüfen Sie ENV-Variablen: PISS_OLLAMA_BASE_URL und PISS_OLLAMA_MODEL"
            ) from e
    
    if last_error:
        raise RuntimeError(
            f"Ollama healthcheck fehlgeschlagen nach {2 if warmup else 1} Versuch(en)\n"
            f"  Base URL: {base_url}\n"
            f"  Model: {model}\n"
            f"  Letzter Fehler: {last_error}\n"
            f"\n"
            f"Diagnose:\n"
            f"  1. Prüfen Sie, ob Ollama läuft: curl -sS {base_url.replace('/v1', '')}/api/tags\n"
            f"  2. Prüfen Sie OpenAI-compat Endpoint: curl -sS -X POST {url} -H 'Content-Type: application/json' -d '{json.dumps(payload)}'\n"
            f"  3. Prüfen Sie ENV-Variablen: PISS_OLLAMA_BASE_URL und PISS_OLLAMA_MODEL"
        ) from last_error




