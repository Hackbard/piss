"""CLI for the minimal members.list MVP runner."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

from langgraph_app.healthcheck import check_ollama_or_die
from langgraph_app.settings import OLLAMA_BASE_URL, OLLAMA_MODEL, _settings

DEFAULT_QUESTION = "Gib mir alle SPD-Abgeordneten im Landtag Niedersachsen zwischen 2014 und 2020."


def _trace_enabled() -> bool:
    value = os.getenv("PISS_MVP_TRACE", "true")
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _trace_dir() -> Path:
    return Path(os.getenv("PISS_MVP_TRACE_DIR", "data/exports/langgraph_mvp_traces"))


def _write_trace(payload: dict[str, Any]) -> Path | None:
    if not _trace_enabled():
        return None

    trace_dir = _trace_dir()
    trace_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pid = os.getpid()
    path = trace_dir / f"mvp-trace-{ts}-{pid}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


async def run_once(
    question: str,
    output_format: str = "text",
    sources_mode: str = "top",
    max_sources: int = 20,
) -> str:
    from langgraph_app.graph import MembersListMvpState, create_members_list_mvp_graph

    app = create_members_list_mvp_graph()

    initial_state: MembersListMvpState = {
        "question": question,
        "tool_input": None,
        "tool_result": None,
        "answer": None,
        "output_format": output_format,
        "sources_mode": sources_mode,
        "max_sources": max_sources,
        "parliament_ids": [],
        "active_only": False,
        "resolved_from_date": None,
        "resolved_to_date": None,
        "tool_base_input": None,
    }

    try:
        result: dict[str, Any] = await app.ainvoke(initial_state)
        trace_path = _write_trace({"question": question, "result": result})
        if trace_path is not None:
            print(f"[trace] {trace_path}", file=sys.stderr)

        answer = result.get("answer")
        if isinstance(answer, str) and answer.strip():
            return answer
        return "Keine Antwort erzeugt."
    except Exception as e:
        trace_path = _write_trace({"question": question, "error": str(e)})
        if trace_path is not None:
            print(f"[trace] {trace_path}", file=sys.stderr)
        raise


def _read_stdin_interactive() -> str | None:
    if not sys.stdin.isatty():
        return None
    try:
        return input("Frage (leer = Ende): ").strip()
    except EOFError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI for members.list MVP runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "question",
        nargs="*",
        help="Question to ask (e.g., 'Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014 und 2020')",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "md"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--sources",
        choices=["none", "top", "per-person"],
        default="top",
        help="Sources display mode: none, top (default), or per-person",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=20,
        help="Maximum number of sources to display (default: 20)",
    )
    parser.add_argument(
        "--no-healthcheck",
        action="store_true",
        help="Skip Ollama healthcheck before starting (not recommended)",
    )
    parser.add_argument(
        "--health-timeout",
        type=float,
        default=5.0,
        help="Healthcheck timeout in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--health-warmup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable warmup retry for model loading (default: True). "
        "If enabled, retries with 30s timeout on first timeout to allow model loading.",
    )

    args = parser.parse_args()

    if not args.no_healthcheck:
        try:
            check_ollama_or_die(
                base_url=OLLAMA_BASE_URL,
                model=OLLAMA_MODEL,
                timeout_s=args.health_timeout,
                warmup=args.health_warmup,
            )
        except RuntimeError as e:
            print(f"Fehler: {e}", file=sys.stderr)
            sys.exit(2)

    question = " ".join(args.question).strip() if args.question else ""

    if question:
        print(asyncio.run(run_once(question, args.format, args.sources, args.max_sources)))
        return

    if sys.stdin.isatty():
        q = DEFAULT_QUESTION
        print(asyncio.run(run_once(q, args.format, args.sources, args.max_sources)))
        while True:
            next_q = _read_stdin_interactive()
            if not next_q:
                break
            print(asyncio.run(run_once(next_q, args.format, args.sources, args.max_sources)))
        return

    print(asyncio.run(run_once(DEFAULT_QUESTION, args.format, args.sources, args.max_sources)))


if __name__ == "__main__":
    main()


