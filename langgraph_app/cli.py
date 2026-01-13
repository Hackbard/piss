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

import csv

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
    if len(sys.argv) > 1 and sys.argv[1] == "list-missing-starts":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli list-missing-starts")
        sub.add_argument("--format", choices=["json", "csv"], default="json")
        sub.add_argument("--out", type=str, default="")
        sub.add_argument("--parliament-id", type=str, default="")
        sub.add_argument("--group-by", type=str, default="parliament,term", help="Comma-separated list: parliament,term")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                where = ""
                params: dict[str, Any] = {}
                if args.parliament_id:
                    where = "WHERE m.parliament_id = $parliament_id"
                    params["parliament_id"] = args.parliament_id

                rows = session.run(
                    f"""
                    MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                    {where}
                    WHERE m.start_date IS NULL
                    WITH m.parliament_id AS parliament_id,
                         coalesce(l.term_number, -1) AS term_number,
                         coalesce(l.name, l.parliament) AS legislature_name,
                         count(m) AS mandates_missing_start,
                         collect(DISTINCT l.source_url)[0..5] AS members_list_urls,
                         collect(DISTINCT l.wikipedia_title)[0..5] AS wikipedia_titles
                    RETURN parliament_id,
                           term_number,
                           legislature_name,
                           mandates_missing_start,
                           members_list_urls,
                           wikipedia_titles
                    ORDER BY mandates_missing_start DESC, parliament_id, term_number
                    LIMIT 500
                    """,
                    **params,
                ).data()

            out_rows: list[dict[str, Any]] = []
            for r in rows:
                parliament_id = r.get("parliament_id")
                term_number = r.get("term_number")
                legislature_name = r.get("legislature_name")
                mandates_missing_start = r.get("mandates_missing_start", 0)
                members_list_urls = r.get("members_list_urls") or []
                wikipedia_titles = r.get("wikipedia_titles") or []

                source_candidates = []
                for url in members_list_urls:
                    if url:
                        source_candidates.append({"type": "members_list", "url": url})
                for title in wikipedia_titles:
                    if title:
                        source_candidates.append({"type": "wikipedia_title", "title": title})

                recommended_action = "Add official source entry for constituting session date (day)"
                if not source_candidates:
                    recommended_action = "Add Wikidata QID mapping and/or official source entry"

                out_rows.append(
                    {
                        "parliament_id": parliament_id,
                        "term_number": term_number if term_number != -1 else None,
                        "legislature_name": legislature_name,
                        "mandates_missing_start": mandates_missing_start,
                        "source_candidates": source_candidates,
                        "recommended_action": recommended_action,
                    }
                )

            output_path = Path(args.out) if args.out else None

            if args.format == "json":
                payload = {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "rows": out_rows,
                }
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                if output_path:
                    output_path.write_text(content, encoding="utf-8")
                else:
                    print(content)
                return

            fieldnames = [
                "parliament_id",
                "term_number",
                "legislature_name",
                "mandates_missing_start",
                "recommended_action",
            ]
            if output_path:
                with output_path.open("w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    w.writeheader()
                    for row in out_rows:
                        w.writerow({k: v for k, v in row.items() if k in fieldnames})
            else:
                w = csv.DictWriter(sys.stdout, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
                for row in out_rows:
                    w.writerow({k: v for k, v in row.items() if k in fieldnames})
            return
        finally:
            driver.close()

    if len(sys.argv) > 1 and sys.argv[1] == "list-missing-legislature-starts":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli list-missing-legislature-starts")
        sub.add_argument("--format", choices=["json", "csv"], default="json")
        sub.add_argument("--out", type=str, default="")
        sub.add_argument("--parliament-id", type=str, default="")
        sub.add_argument(
            "--official-registry",
            type=str,
            default="langgraph_app/sources/official_sources.yaml",
            help="Path to official sources registry YAML",
        )
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings
        from langgraph_app.sources.official_registry import load_official_registry

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))

        official_entries = load_official_registry(Path(args.official_registry))
        official_map: dict[tuple[str, int], Any] = {
            (e.parliament_id, e.term_number): e for e in official_entries
        }

        def parse_term_number(name: str | None, title: str | None) -> int | None:
            for s in (title, name):
                if not s:
                    continue
                m = __import__("re").search(r"\((\d+)\.\s*Wahlperiode\)", s)
                if m:
                    return int(m.group(1))
                m = __import__("re").search(r"^(\d+)\.", s.strip())
                if m:
                    return int(m.group(1))
            return None

        try:
            with driver.session() as session:
                where = "WHERE l.start_date IS NULL"
                params: dict[str, Any] = {}
                if args.parliament_id:
                    where += " AND l.parliament_id = $parliament_id"
                    params["parliament_id"] = args.parliament_id

                rows = session.run(
                    f"""
                    MATCH (l:Legislature)
                    {where}
                    OPTIONAL MATCH (l)-[:HAS_TERM]->(t:LegislatureTerm)
                    RETURN l.parliament_id AS parliament_id,
                           l.term_number AS term_number,
                           l.name AS legislature_name,
                           l.wikipedia_title AS wikipedia_title,
                           l.source_url AS members_list_url,
                           l.start_date_raw AS start_date_raw,
                           l.start_date_precision AS start_date_precision,
                           collect({{
                               source_primary: t.source_primary,
                               start_date: t.start_date,
                               start_date_precision: t.start_date_precision,
                               qid: t.qid,
                               evidence_urls: t.evidence_urls
                           }}) AS terms
                    ORDER BY l.parliament_id, l.term_number, l.name
                    """,
                    **params,
                ).data()

            out_rows: list[dict[str, Any]] = []
            for r in rows:
                parliament_id = r.get("parliament_id")
                legislature_name = r.get("legislature_name")
                wikipedia_title = r.get("wikipedia_title")
                term_number = r.get("term_number")
                if term_number is None:
                    term_number = parse_term_number(legislature_name, wikipedia_title)

                terms = r.get("terms") or []
                has_wikidata_term = any(
                    isinstance(t, dict) and t.get("source_primary") == "wikidata" for t in terms
                )
                has_wikidata_day_precision = any(
                    isinstance(t, dict)
                    and t.get("source_primary") == "wikidata"
                    and t.get("start_date")
                    and t.get("start_date_precision") == "day"
                    for t in terms
                )

                has_official_source = False
                if parliament_id and isinstance(term_number, int):
                    entry = official_map.get((parliament_id, term_number))
                    has_official_source = bool(entry and entry.start_date)

                out_rows.append(
                    {
                        "parliament_id": parliament_id,
                        "term_number": term_number,
                        "legislature_name": legislature_name,
                        "members_list_oldid_url": r.get("members_list_url"),
                        "wikipedia_title": wikipedia_title,
                        "start_date_raw": r.get("start_date_raw"),
                        "start_date_precision": r.get("start_date_precision") or "unknown",
                        "has_official_source": has_official_source,
                        "has_wikidata_term": has_wikidata_term,
                        "has_wikidata_day_precision": has_wikidata_day_precision,
                        "needs_registry_entry": (not has_official_source) and (not has_wikidata_day_precision),
                        "recommended_action": "Add official source URL for constituting session date",
                    }
                )

            output_path = Path(args.out) if args.out else None

            if args.format == "json":
                payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "rows": out_rows}
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                if output_path:
                    output_path.write_text(content, encoding="utf-8")
                else:
                    print(content)
                return

            fieldnames = list(out_rows[0].keys()) if out_rows else [
                "parliament_id",
                "term_number",
                "legislature_name",
                "members_list_oldid_url",
                "wikipedia_title",
                "start_date_raw",
                "start_date_precision",
                "has_official_source",
                "has_wikidata_term",
                "has_wikidata_day_precision",
                "needs_registry_entry",
                "recommended_action",
            ]
            if output_path:
                with output_path.open("w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames)
                    w.writeheader()
                    for row in out_rows:
                        w.writerow(row)
            else:
                w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                w.writeheader()
                for row in out_rows:
                    w.writerow(row)
            return
        finally:
            driver.close()

    if len(sys.argv) > 1 and sys.argv[1] == "ingest-official-terms":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli ingest-official-terms")
        sub.add_argument(
            "--official-registry",
            type=str,
            default="langgraph_app/sources/official_sources.yaml",
            help="Path to official sources registry YAML",
        )
        sub.add_argument("--force", action="store_true", help="Allow overwriting conflicting dates")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings
        from langgraph_app.sources.official_registry import load_official_registry
        from langgraph_app.governance.dates import (
            DatePrecision,
            GovernedDate,
            apply_governed_date,
        )

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))

        entries = load_official_registry(Path(args.official_registry))

        def is_iso_day(value: str | None) -> bool:
            return bool(value and __import__("re").match(r"^\d{4}-\d{2}-\d{2}$", value))

        def determine_precision(value: str | None) -> DatePrecision:
            if not value:
                return DatePrecision.NULL
            if is_iso_day(value):
                return DatePrecision.DAY
            return DatePrecision.UNKNOWN

        try:
            with driver.session() as session:
                for e in entries:
                    term_id = f"official:{e.parliament_id}:{e.term_number}"

                    session.run(
                        """
                        MERGE (t:LegislatureTerm {id: $id})
                        SET t.parliament_id = $parliament_id,
                            t.term_number = $term_number,
                            t.source_primary = "official",
                            t.source_meta_json = $source_meta_json
                        """,
                        id=term_id,
                        parliament_id=e.parliament_id,
                        term_number=e.term_number,
                        source_meta_json=json.dumps(e.source_meta, ensure_ascii=False, sort_keys=True),
                    )

                    source_url = e.evidence_urls[0] if e.evidence_urls else ""

                    if e.start_date:
                        start_precision = determine_precision(e.start_date)
                        governed_start = GovernedDate(
                            iso_day=e.start_date if start_precision == DatePrecision.DAY else None,
                            precision=start_precision,
                            raw=e.start_date,
                            source_kind="official",
                            source_url=source_url,
                            evidence_urls=e.evidence_urls,
                            method="official_registry",
                            reason="From official sources registry",
                        )

                        def apply_start(tx):
                            return apply_governed_date(
                                tx,
                                "LegislatureTerm",
                                term_id,
                                "start_date",
                                governed_start,
                                "cli:ingest-official-terms",
                                allow_force=args.force,
                            )

                        session.write_transaction(apply_start)

                    if e.end_date:
                        end_precision = determine_precision(e.end_date)
                        governed_end = GovernedDate(
                            iso_day=e.end_date if end_precision == DatePrecision.DAY else None,
                            precision=end_precision,
                            raw=e.end_date,
                            source_kind="official",
                            source_url=source_url,
                            evidence_urls=e.evidence_urls,
                            method="official_registry",
                            reason="From official sources registry",
                        )

                        def apply_end(tx):
                            return apply_governed_date(
                                tx,
                                "LegislatureTerm",
                                term_id,
                                "end_date",
                                governed_end,
                                "cli:ingest-official-terms",
                                allow_force=args.force,
                            )

                        session.write_transaction(apply_end)

                    session.run(
                        """
                        MATCH (l:Legislature {parliament_id: $parliament_id, term_number: $term_number})
                        MATCH (t:LegislatureTerm {id: $term_id})
                        MERGE (l)-[:HAS_TERM]->(t)
                        """,
                        parliament_id=e.parliament_id,
                        term_number=e.term_number,
                        term_id=term_id,
                    )
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "generate-official-terms-skeleton":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli generate-official-terms-skeleton")
        sub.add_argument(
            "--official-registry",
            type=str,
            default="langgraph_app/sources/official_sources.yaml",
            help="Path to official sources registry YAML (will be updated in-place unless --out is set)",
        )
        sub.add_argument(
            "--out",
            type=str,
            default="",
            help="Optional output path (if set, writes there instead of updating the input file)",
        )
        sub.add_argument(
            "--include-bt",
            action="store_true",
            help="Also generate Bundestag terms (1..20) skeleton",
        )
        sub.add_argument(
            "--include-br",
            action="store_true",
            help="Also generate Bundesrat placeholder terms skeleton (disabled by default)",
        )
        args = sub.parse_args(sys.argv[2:])

        import yaml
        from neo4j import GraphDatabase
        from scraper.config import get_settings

        in_path = Path(args.official_registry)
        out_path = Path(args.out) if args.out else in_path

        payload = yaml.safe_load(in_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {}

        payload.setdefault("version", 1)
        payload.setdefault("default", {})
        payload.setdefault("parliaments", {})

        parliaments = payload.get("parliaments")
        if not isinstance(parliaments, dict):
            parliaments = {}
            payload["parliaments"] = parliaments

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                rows = session.run(
                    """
                    MATCH (l:Legislature)
                    WHERE l.term_number IS NOT NULL
                    RETURN l.parliament_id AS parliament_id,
                           min(l.term_number) AS min_term,
                           max(l.term_number) AS max_term
                    ORDER BY parliament_id
                    """
                ).data()
        finally:
            driver.close()

        ranges: dict[str, tuple[int, int]] = {}
        for r in rows:
            pid = r.get("parliament_id")
            a = r.get("min_term")
            b = r.get("max_term")
            if isinstance(pid, str) and isinstance(a, int) and isinstance(b, int) and a > 0 and b >= a:
                ranges[pid] = (a, b)

        if args.include_bt:
            ranges["BT"] = (1, 20)
        if args.include_br and "BR" not in ranges:
            ranges["BR"] = (1, 1)

        for pid, (a, b) in ranges.items():
            cfg = parliaments.get(pid)
            if not isinstance(cfg, dict):
                cfg = {"name": "", "homepage": "", "terms": []}
                parliaments[pid] = cfg

            terms = cfg.get("terms")
            if not isinstance(terms, list):
                terms = []
                cfg["terms"] = terms

            existing: dict[int, dict[str, Any]] = {}
            for t in terms:
                if isinstance(t, dict) and isinstance(t.get("term_number"), int):
                    existing[int(t["term_number"])] = t

            new_terms: list[dict[str, Any]] = []
            for n in range(a, b + 1):
                if n in existing:
                    entry = existing[n]
                    entry.setdefault("evidence_urls", [])
                    new_terms.append(entry)
                    continue
                new_terms.append({"term_number": n, "start_date": None, "evidence_urls": []})

            cfg["terms"] = new_terms

        out_path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return

    if len(sys.argv) > 1 and sys.argv[1] == "ingest-wikidata-term":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli ingest-wikidata-term")
        sub.add_argument("--qid", type=str, required=True)
        sub.add_argument("--parliament-id", type=str, default="")
        sub.add_argument("--term-number", type=int, default=0)
        sub.add_argument("--force", action="store_true", help="Allow overwriting conflicting dates")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings
        from langgraph_app.sources.wikidata_terms import fetch_entity_pinned, fetch_lastrevid, parse_term_from_entitydata
        from langgraph_app.governance.dates import DatePrecision, GovernedDate, apply_governed_date

        def wikidata_precision_to_enum(p: int) -> DatePrecision:
            if p == 11:
                return DatePrecision.DAY
            if p == 10:
                return DatePrecision.MONTH
            if p == 9:
                return DatePrecision.YEAR
            return DatePrecision.UNKNOWN

        qid = args.qid.strip().upper()
        revision = fetch_lastrevid(qid)
        entitydata = fetch_entity_pinned(qid, revision)
        term = parse_term_from_entitydata(qid, revision, entitydata)

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                term_id = f"wikidata:{qid}:{revision}"
                session.run(
                    """
                    MERGE (t:LegislatureTerm {id: $id})
                    SET t.qid = $qid,
                        t.parliament_id = $parliament_id,
                        t.term_number = $term_number,
                        t.name = $name,
                        t.source_primary = "wikidata",
                        t.source_meta_json = $source_meta_json
                    """,
                    id=term_id,
                    qid=term.qid,
                    parliament_id=args.parliament_id or None,
                    term_number=args.term_number or None,
                    name=term.name,
                    source_meta_json=json.dumps(term.source_meta, ensure_ascii=False, sort_keys=True),
                )

                if term.start.raw:
                    start_precision = wikidata_precision_to_enum(term.start.precision)
                    governed_start = GovernedDate(
                        iso_day=term.start.value_iso if start_precision == DatePrecision.DAY else None,
                        precision=start_precision,
                        raw=term.start.raw,
                        source_kind="wikidata",
                        source_url=term.evidence_url,
                        evidence_urls=[term.evidence_url],
                        method="wikidata_term",
                        reason=f"From Wikidata QID {qid} (revision {revision})",
                    )

                    def apply_start(tx):
                        return apply_governed_date(
                            tx,
                            "LegislatureTerm",
                            term_id,
                            "start_date",
                            governed_start,
                            "cli:ingest-wikidata-term",
                            allow_force=args.force,
                        )

                    session.write_transaction(apply_start)

                if term.end.raw:
                    end_precision = wikidata_precision_to_enum(term.end.precision)
                    governed_end = GovernedDate(
                        iso_day=term.end.value_iso if end_precision == DatePrecision.DAY else None,
                        precision=end_precision,
                        raw=term.end.raw,
                        source_kind="wikidata",
                        source_url=term.evidence_url,
                        evidence_urls=[term.evidence_url],
                        method="wikidata_term",
                        reason=f"From Wikidata QID {qid} (revision {revision})",
                    )

                    def apply_end(tx):
                        return apply_governed_date(
                            tx,
                            "LegislatureTerm",
                            term_id,
                            "end_date",
                            governed_end,
                            "cli:ingest-wikidata-term",
                            allow_force=args.force,
                        )

                    session.write_transaction(apply_end)

                if args.parliament_id and args.term_number:
                    session.run(
                        """
                        MATCH (l:Legislature {parliament_id: $parliament_id, term_number: $term_number})
                        MATCH (t:LegislatureTerm {id: $term_id})
                        MERGE (l)-[:HAS_TERM]->(t)
                        """,
                        parliament_id=args.parliament_id,
                        term_number=args.term_number,
                        term_id=term_id,
                    )
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "ingest-wikidata-terms":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli ingest-wikidata-terms")
        sub.add_argument(
            "--mapping",
            type=str,
            default="langgraph_app/sources/wikidata_mapping.yaml",
            help="Path to YAML mapping file (parliament_id -> term_number -> qid)",
        )
        sub.add_argument("--all", action="store_true", help="Process all legislatures without start_date")
        sub.add_argument("--parliament-id", type=str, default="", help="Filter by parliament_id")
        sub.add_argument("--dry-run", action="store_true", help="Show what would be ingested without making changes")
        args = sub.parse_args(sys.argv[2:])

        import yaml
        from neo4j import GraphDatabase

        from scraper.config import get_settings
        from langgraph_app.sources.wikidata_terms import fetch_entity_pinned, fetch_lastrevid, parse_term_from_entitydata
        from langgraph_app.governance.dates import DatePrecision, GovernedDate, apply_governed_date

        def wikidata_precision_to_enum(p: int) -> DatePrecision:
            if p == 11:
                return DatePrecision.DAY
            if p == 10:
                return DatePrecision.MONTH
            if p == 9:
                return DatePrecision.YEAR
            return DatePrecision.UNKNOWN

        mapping_path = Path(args.mapping)
        mapping: dict[str, dict[int, str]] = {}
        if mapping_path.exists():
            payload = yaml.safe_load(mapping_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                for pid, terms in payload.items():
                    if isinstance(terms, dict):
                        mapping[pid] = {int(k): str(v) for k, v in terms.items() if isinstance(k, (int, str)) and isinstance(v, str)}

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                where = "WHERE l.start_date IS NULL"
                params: dict[str, Any] = {}
                if args.parliament_id:
                    where += " AND l.parliament_id = $parliament_id"
                    params["parliament_id"] = args.parliament_id

                rows = session.run(
                    f"""
                    MATCH (l:Legislature)
                    {where}
                    RETURN l.parliament_id AS parliament_id,
                           l.term_number AS term_number,
                           l.name AS legislature_name
                    ORDER BY l.parliament_id, l.term_number
                    """,
                    **params,
                ).data()

                processed = 0
                skipped_no_mapping = 0
                skipped_no_day_precision = 0
                errors = 0

                for r in rows:
                    parliament_id = r.get("parliament_id")
                    term_number = r.get("term_number")
                    if not parliament_id or not isinstance(term_number, int):
                        skipped_no_mapping += 1
                        continue

                    qid = mapping.get(parliament_id, {}).get(term_number)
                    if not qid:
                        skipped_no_mapping += 1
                        continue

                    try:
                        qid = qid.strip().upper()
                        revision = fetch_lastrevid(qid)
                        entitydata = fetch_entity_pinned(qid, revision)
                        term = parse_term_from_entitydata(qid, revision, entitydata)

                        if term.start.precision != 11:
                            skipped_no_day_precision += 1
                            if not args.dry_run:
                                print(
                                    f"Skipped {parliament_id}:{term_number} (QID {qid}): precision={term.start.precision}, not day",
                                    file=sys.stderr,
                                )
                            continue

                        if args.dry_run:
                            print(
                                f"Would ingest {parliament_id}:{term_number} -> QID {qid} (revision {revision}): start_date={term.start.value_iso}",
                                file=sys.stderr,
                            )
                            processed += 1
                            continue

                        term_id = f"wikidata:{qid}:{revision}"
                        session.run(
                            """
                            MERGE (t:LegislatureTerm {id: $id})
                            SET t.qid = $qid,
                                t.parliament_id = $parliament_id,
                                t.term_number = $term_number,
                                t.name = $name,
                                t.source_primary = "wikidata",
                                t.source_meta_json = $source_meta_json
                            """,
                            id=term_id,
                            qid=term.qid,
                            parliament_id=parliament_id,
                            term_number=term_number,
                            name=term.name,
                            source_meta_json=json.dumps(term.source_meta, ensure_ascii=False, sort_keys=True),
                        )

                        if term.start.raw:
                            start_precision = wikidata_precision_to_enum(term.start.precision)
                            governed_start = GovernedDate(
                                iso_day=term.start.value_iso if start_precision == DatePrecision.DAY else None,
                                precision=start_precision,
                                raw=term.start.raw,
                                source_kind="wikidata",
                                source_url=term.evidence_url,
                                evidence_urls=[term.evidence_url],
                                method="wikidata_term",
                                reason=f"From Wikidata QID {qid} (revision {revision})",
                            )

                            def apply_start(tx):
                                return apply_governed_date(
                                    tx,
                                    "LegislatureTerm",
                                    term_id,
                                    "start_date",
                                    governed_start,
                                    "cli:ingest-wikidata-terms",
                                    allow_force=False,
                                )

                            session.write_transaction(apply_start)

                        if term.end.raw:
                            end_precision = wikidata_precision_to_enum(term.end.precision)
                            governed_end = GovernedDate(
                                iso_day=term.end.value_iso if end_precision == DatePrecision.DAY else None,
                                precision=end_precision,
                                raw=term.end.raw,
                                source_kind="wikidata",
                                source_url=term.evidence_url,
                                evidence_urls=[term.evidence_url],
                                method="wikidata_term",
                                reason=f"From Wikidata QID {qid} (revision {revision})",
                            )

                            def apply_end(tx):
                                return apply_governed_date(
                                    tx,
                                    "LegislatureTerm",
                                    term_id,
                                    "end_date",
                                    governed_end,
                                    "cli:ingest-wikidata-terms",
                                    allow_force=False,
                                )

                            session.write_transaction(apply_end)

                        session.run(
                            """
                            MATCH (l:Legislature {parliament_id: $parliament_id, term_number: $term_number})
                            MATCH (t:LegislatureTerm {id: $term_id})
                            MERGE (l)-[:HAS_TERM]->(t)
                            """,
                            parliament_id=parliament_id,
                            term_number=term_number,
                            term_id=term_id,
                        )
                        processed += 1
                    except Exception as e:
                        errors += 1
                        print(f"Error processing {parliament_id}:{term_number} (QID {qid}): {e}", file=sys.stderr)

                result = {
                    "processed": processed,
                    "skipped_no_mapping": skipped_no_mapping,
                    "skipped_no_day_precision": skipped_no_day_precision,
                    "errors": errors,
                }
                print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "propagate-legislature-starts":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli propagate-legislature-starts")
        sub.add_argument("--parliament-id", type=str, default="")
        sub.add_argument("--force", action="store_true", help="Allow overwriting conflicting dates")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings
        from langgraph_app.governance.dates import DatePrecision, GovernedDate, apply_governed_date

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                where = "WHERE l.start_date IS NULL"
                params: dict[str, Any] = {}
                if args.parliament_id:
                    where += " AND l.parliament_id = $parliament_id"
                    params["parliament_id"] = args.parliament_id

                rows = session.run(
                    f"""
                    MATCH (l:Legislature)
                    {where}
                    MATCH (l)-[:HAS_TERM]->(t:LegislatureTerm)
                    WHERE t.start_date IS NOT NULL AND t.start_date_precision = "day"
                    WITH l, t,
                         CASE t.source_primary
                            WHEN "official" THEN 1
                            WHEN "wikidata" THEN 2
                            WHEN "wikipedia" THEN 3
                            ELSE 99
                         END AS rank
                    ORDER BY rank ASC
                    WITH l.id AS legislature_id, head(collect(t)) AS best
                    RETURN legislature_id,
                           best.start_date AS start_date,
                           best.start_date_precision AS start_date_precision,
                           best.start_date_raw AS start_date_raw,
                           best.source_primary AS source_primary,
                           best.evidence_urls AS evidence_urls,
                           best.source_meta_json AS source_meta_json
                    """,
                    **params,
                ).data()

                legislatures_updated = 0
                for row in rows:
                    legislature_id = row.get("legislature_id")
                    start_date = row.get("start_date")
                    evidence_urls = row.get("evidence_urls") or []
                    source_primary = row.get("source_primary") or "unknown"
                    source_url = evidence_urls[0] if evidence_urls else ""

                    if not legislature_id or not start_date:
                        continue

                    governed_start = GovernedDate(
                        iso_day=start_date,
                        precision=DatePrecision.DAY,
                        raw=row.get("start_date_raw"),
                        source_kind=source_primary,
                        source_url=source_url,
                        evidence_urls=evidence_urls,
                        method="propagate_from_term",
                        reason=f"Propagated from LegislatureTerm (source: {source_primary})",
                    )

                    def apply_start(tx):
                        return apply_governed_date(
                            tx,
                            "Legislature",
                            legislature_id,
                            "start_date",
                            governed_start,
                            "cli:propagate-legislature-starts",
                            allow_force=args.force,
                        )

                    result = session.write_transaction(apply_start)
                    if result.canonical_written:
                        legislatures_updated += 1

                mandate_rows = session.run(
                    """
                    MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                    WHERE m.start_date IS NULL AND l.start_date IS NOT NULL AND l.start_date_precision = "day"
                    RETURN m.id AS mandate_id,
                           l.start_date AS start_date,
                           l.start_date_source_kind AS source_kind,
                           l.start_date_source_url AS source_url,
                           l.start_date_evidence_urls AS evidence_urls
                    """,
                ).data()

                mandates_backfilled = 0
                for row in mandate_rows:
                    mandate_id = row.get("mandate_id")
                    start_date = row.get("start_date")
                    source_kind = row.get("source_kind") or "legislature"
                    source_url = row.get("source_url") or ""
                    evidence_urls = row.get("evidence_urls") or []

                    if not mandate_id or not start_date:
                        continue

                    governed_start = GovernedDate(
                        iso_day=start_date,
                        precision=DatePrecision.DAY,
                        raw=None,
                        source_kind=source_kind,
                        source_url=source_url,
                        evidence_urls=evidence_urls,
                        method="propagate_from_legislature",
                        reason="Propagated from Legislature.start_date",
                    )

                    def apply_mandate_start(tx):
                        return apply_governed_date(
                            tx,
                            "Mandate",
                            mandate_id,
                            "start_date",
                            governed_start,
                            "cli:propagate-legislature-starts",
                            allow_force=False,
                        )

                    result = session.write_transaction(apply_mandate_start)
                    if result.canonical_written:
                        mandates_backfilled += 1

                print(
                    json.dumps(
                        {
                            "legislatures_updated": legislatures_updated,
                            "mandates_start_backfilled": mandates_backfilled,
                        }
                    )
                )
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "audit-date-conflicts":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli audit-date-conflicts")
        sub.add_argument("--format", choices=["json", "csv"], default="json")
        sub.add_argument("--out", type=str, default="")
        sub.add_argument("--entity-type", type=str, choices=["Legislature", "Mandate", "LegislatureTerm"], default="")
        sub.add_argument("--field", type=str, choices=["start_date", "end_date"], default="")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                field = args.field or "start_date"
                entity_type_filter = ""
                params: dict[str, Any] = {}
                
                if args.entity_type:
                    entity_type_filter = f"labels(n)[0] = $entity_type"
                    params["entity_type"] = args.entity_type
                else:
                    entity_type_filter = "labels(n)[0] IN ['Legislature', 'Mandate', 'LegislatureTerm']"

                query = f"""
                    MATCH (n)
                    WHERE {entity_type_filter} AND n.{field}_conflict = true
                    RETURN labels(n)[0] AS entity_type,
                           n.id AS entity_id,
                           n.{field} AS canonical_date,
                           n.{field}_conflict_with AS conflict_with,
                           n.{field}_source_kind AS source_kind,
                           n.{field}_source_url AS source_url,
                           n.{field}_evidence_urls AS evidence_urls,
                           n.{field}_set_at AS set_at,
                           n.{field}_set_by AS set_by
                    ORDER BY entity_type, entity_id
                    """
                
                rows = session.run(query, **params).data()

            output_path = Path(args.out) if args.out else None

            if args.format == "json":
                payload = {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "rows": rows,
                }
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                if output_path:
                    output_path.write_text(content, encoding="utf-8")
                else:
                    print(content)
                return

            fieldnames = [
                "entity_type",
                "entity_id",
                "canonical_date",
                "conflict_with",
                "source_kind",
                "source_url",
                "set_at",
                "set_by",
            ]
            if output_path:
                with output_path.open("w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    w.writeheader()
                    for row in rows:
                        w.writerow({k: v for k, v in row.items() if k in fieldnames})
            else:
                w = csv.DictWriter(sys.stdout, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
                for row in rows:
                    w.writerow({k: v for k, v in row.items() if k in fieldnames})
            return
        finally:
            driver.close()

    if len(sys.argv) > 1 and sys.argv[1] == "audit-missing-canonical-dates":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli audit-missing-canonical-dates")
        sub.add_argument("--format", choices=["json", "csv"], default="json")
        sub.add_argument("--out", type=str, default="")
        sub.add_argument("--entity-type", type=str, choices=["Legislature", "Mandate"], default="")
        sub.add_argument("--field", type=str, choices=["start_date", "end_date"], default="start_date")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                field = args.field
                entity_type = args.entity_type or "Legislature"
                
                query = f"""
                    MATCH (n:{entity_type})
                    WHERE n.{field} IS NULL
                    RETURN n.id AS entity_id,
                           n.{field}_raw AS raw_date,
                           n.{field}_precision AS precision,
                           n.{field}_source_kind AS source_kind,
                           n.{field}_source_url AS source_url
                    ORDER BY entity_id
                    LIMIT 1000
                    """
                
                rows = session.run(query).data()

            output_path = Path(args.out) if args.out else None

            if args.format == "json":
                payload = {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "entity_type": entity_type,
                    "field": field,
                    "rows": rows,
                }
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                if output_path:
                    output_path.write_text(content, encoding="utf-8")
                else:
                    print(content)
                return

            fieldnames = ["entity_id", "raw_date", "precision", "source_kind", "source_url"]
            if output_path:
                with output_path.open("w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames)
                    w.writeheader()
                    for row in rows:
                        w.writerow(row)
            else:
                w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                w.writeheader()
                for row in rows:
                    w.writerow(row)
            return
        finally:
            driver.close()

    if len(sys.argv) > 1 and sys.argv[1] == "resolve-date-conflict":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli resolve-date-conflict")
        sub.add_argument("--entity", type=str, required=True, help="Entity type (Legislature, Mandate, LegislatureTerm)")
        sub.add_argument("--entity-id", type=str, required=True, help="Entity ID")
        sub.add_argument("--field", type=str, required=True, choices=["start_date", "end_date"], help="Field name")
        sub.add_argument("--accept", type=str, required=True, help="Date to accept (YYYY-MM-DD)")
        sub.add_argument("--evidence-url", type=str, required=True, help="Evidence URL")
        sub.add_argument("--reason", type=str, default="", help="Reason for resolution")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase

        from scraper.config import get_settings
        from langgraph_app.governance.dates import DatePrecision, GovernedDate, apply_governed_date

        if not __import__("re").match(r"^\d{4}-\d{2}-\d{2}$", args.accept):
            print(f"Error: --accept must be in YYYY-MM-DD format, got {args.accept!r}", file=sys.stderr)
            sys.exit(1)

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                result = session.run(
                    f"""
                    MATCH (n:{args.entity} {{id: $entity_id}})
                    RETURN n.{args.field} AS current_date,
                           n.{args.field}_conflict AS has_conflict,
                           n.{args.field}_conflict_with AS conflict_with,
                           n.{args.field}_source_kind AS source_kind,
                           n.{args.field}_evidence_urls AS evidence_urls
                    """,
                    entity_id=args.entity_id,
                ).single()

                if not result:
                    print(f"Error: Entity {args.entity} with id {args.entity_id} not found", file=sys.stderr)
                    sys.exit(1)

                current_date = result.get("current_date")
                has_conflict = result.get("has_conflict", False)
                conflict_with = result.get("conflict_with") or []
                source_kind = result.get("source_kind") or "manual"
                evidence_urls = result.get("evidence_urls") or []

                if not has_conflict:
                    print(f"Warning: Entity {args.entity_id} does not have a conflict flag set", file=sys.stderr)

                all_evidence_urls = list(set([args.evidence_url] + evidence_urls))

                governed_date = GovernedDate(
                    iso_day=args.accept,
                    precision=DatePrecision.DAY,
                    raw=None,
                    source_kind=source_kind,
                    source_url=args.evidence_url,
                    evidence_urls=all_evidence_urls,
                    method="manual_resolution",
                    reason=args.reason or f"Manual resolution of conflict (previous: {current_date}, conflict_with: {conflict_with})",
                )

                def apply_resolution(tx):
                    return apply_governed_date(
                        tx,
                        args.entity,
                        args.entity_id,
                        args.field,
                        governed_date,
                        "cli:resolve-date-conflict",
                        allow_force=True,
                    )

                result = session.write_transaction(apply_resolution)
                print(
                    json.dumps(
                        {
                            "resolved": True,
                            "entity_type": args.entity,
                            "entity_id": args.entity_id,
                            "field": args.field,
                            "previous_date": current_date,
                            "accepted_date": args.accept,
                            "conflict_detected": result.conflict_detected,
                            "audit_event_id": result.audit_event_id,
                        }
                    )
                )
        finally:
            driver.close()
        return

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


