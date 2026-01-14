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


def _migrate_evidence_links(
    labels: list[str],
    batch_size: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    from neo4j import GraphDatabase
    from scraper.config import get_settings

    settings = get_settings()
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            if dry_run:
                stats_result = session.run(
                    """
                    MATCH (n)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                    RETURN count(DISTINCT n) AS nodes_scanned
                    """,
                    labels=labels,
                ).single()
                nodes_scanned = stats_result.get("nodes_scanned", 0) if stats_result else 0

                urls_result = session.run(
                    """
                    MATCH (n)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                    OPTIONAL MATCH (n)-[:SUPPORTED_BY]->(er)
                    WHERE er.url IS NOT NULL OR er['url'] IS NOT NULL
                    WITH n,
                         collect(DISTINCT coalesce(er.url, er['url'])) AS evidence_ref_urls
                    WITH n, evidence_ref_urls,
                         CASE WHEN n['evidence_urls'] IS NULL THEN [] ELSE [n['evidence_urls']] END +
                         CASE WHEN n['start_date_source'] IS NULL OR trim(toString(n['start_date_source'])) = '' THEN [] ELSE [toString(n['start_date_source'])] END +
                         CASE WHEN n['end_date_source'] IS NULL OR trim(toString(n['end_date_source'])) = '' THEN [] ELSE [toString(n['end_date_source'])] END +
                         CASE WHEN n['source_url'] IS NULL OR trim(toString(n['source_url'])) = '' THEN [] ELSE [toString(n['source_url'])] END +
                         CASE WHEN n['wikipedia_url'] IS NULL OR trim(toString(n['wikipedia_url'])) = '' THEN [] ELSE [toString(n['wikipedia_url'])] END +
                         CASE WHEN n['evidence_url'] IS NULL OR trim(toString(n['evidence_url'])) = '' THEN [] ELSE [toString(n['evidence_url'])] END +
                         evidence_ref_urls
                         AS all_urls
                    UNWIND all_urls AS url
                    WHERE url IS NOT NULL AND trim(toString(url)) <> ''
                    RETURN count(DISTINCT toString(url)) AS unique_urls,
                           count(*) AS total_links
                    """,
                    labels=labels,
                ).single()
                unique_urls = urls_result.get("unique_urls", 0) if urls_result else 0
                total_links = urls_result.get("total_links", 0) if urls_result else 0

                evidence_ref_result = session.run(
                    """
                    MATCH (n)-[:SUPPORTED_BY]->(er)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                      AND (er:EvidenceRef OR (er.url IS NOT NULL AND trim(er.url) <> ''))
                      AND (er.url IS NOT NULL OR er['url'] IS NOT NULL)
                    RETURN count(DISTINCT er) AS evidence_ref_count
                    """,
                    labels=labels,
                ).single()
                evidence_ref_count = evidence_ref_result.get("evidence_ref_count", 0) if evidence_ref_result else 0

                return {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "labels": labels,
                    "batch_size": batch_size,
                    "dry_run": True,
                    "nodes_scanned": nodes_scanned,
                    "urls_discovered": unique_urls,
                    "total_links": total_links,
                    "evidence_ref_count": evidence_ref_count,
                    "done": True,
                }
            else:
                nodes_result = session.run(
                    """
                    MATCH (n)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                    RETURN count(DISTINCT n) AS nodes_scanned
                    """,
                    labels=labels,
                ).single()
                nodes_scanned = nodes_result.get("nodes_scanned", 0) if nodes_result else 0

                result = session.run(
                    """
                    MATCH (n)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                    OPTIONAL MATCH (n)-[:SUPPORTED_BY]->(er)
                    WHERE er.url IS NOT NULL OR er['url'] IS NOT NULL
                    WITH n,
                         collect(DISTINCT coalesce(er.url, er['url'])) AS evidence_ref_urls
                    WITH n, evidence_ref_urls,
                         CASE WHEN n['evidence_urls'] IS NULL THEN [] ELSE [n['evidence_urls']] END +
                         CASE WHEN n['start_date_source'] IS NULL OR trim(toString(n['start_date_source'])) = '' THEN [] ELSE [toString(n['start_date_source'])] END +
                         CASE WHEN n['end_date_source'] IS NULL OR trim(toString(n['end_date_source'])) = '' THEN [] ELSE [toString(n['end_date_source'])] END +
                         CASE WHEN n['source_url'] IS NULL OR trim(toString(n['source_url'])) = '' THEN [] ELSE [toString(n['source_url'])] END +
                         CASE WHEN n['wikipedia_url'] IS NULL OR trim(toString(n['wikipedia_url'])) = '' THEN [] ELSE [toString(n['wikipedia_url'])] END +
                         CASE WHEN n['evidence_url'] IS NULL OR trim(toString(n['evidence_url'])) = '' THEN [] ELSE [toString(n['evidence_url'])] END +
                         evidence_ref_urls
                         AS all_urls
                    UNWIND all_urls AS url
                    WITH n, url
                    WHERE url IS NOT NULL AND trim(toString(url)) <> ''
                    MERGE (e:Evidence {url: toString(url)})
                    MERGE (n)-[:SUPPORTED_BY]->(e)
                    RETURN count(DISTINCT e) AS evidence_nodes_merged,
                           count(*) AS rels_merged
                    """,
                    labels=labels,
                )
                stats = result.single()
                evidence_nodes_merged = stats.get("evidence_nodes_merged", 0) if stats else 0
                rels_merged = stats.get("rels_merged", 0) if stats else 0

                evidence_ref_result = session.run(
                    """
                    MATCH (n)-[:SUPPORTED_BY]->(er)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                      AND (er:EvidenceRef OR (er.url IS NOT NULL AND trim(er.url) <> ''))
                      AND (er.url IS NOT NULL OR er['url'] IS NOT NULL)
                    WITH n, er,
                         coalesce(er.url, er['url']) AS ref_url
                    WHERE ref_url IS NOT NULL AND trim(ref_url) <> ''
                    MERGE (e:Evidence {url: ref_url})
                    MERGE (n)-[:SUPPORTED_BY]->(e)
                    RETURN count(DISTINCT e) AS evidence_ref_nodes_merged,
                           count(*) AS evidence_ref_rels_merged
                    """,
                    labels=labels,
                )
                ref_stats = evidence_ref_result.single()
                evidence_ref_nodes_merged = ref_stats.get("evidence_ref_nodes_merged", 0) if ref_stats else 0
                evidence_ref_rels_merged = ref_stats.get("evidence_ref_rels_merged", 0) if ref_stats else 0

                urls_discovered = evidence_nodes_merged + evidence_ref_nodes_merged

                return {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "labels": labels,
                    "batch_size": batch_size,
                    "nodes_scanned": nodes_scanned,
                    "urls_discovered": urls_discovered,
                    "evidence_nodes_merged": evidence_nodes_merged + evidence_ref_nodes_merged,
                    "rels_merged": rels_merged + evidence_ref_rels_merged,
                    "done": True,
                }
    finally:
        driver.close()


def _backfill_date_metadata(
    labels: list[str],
    fields: list[str],
    batch_size: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    from neo4j import GraphDatabase
    from scraper.config import get_settings

    settings = get_settings()
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            summary = {}
            for field in fields:
                precision_key = f"{field}_precision"
                raw_key = f"{field}_raw"

                if dry_run:
                    precision_result = session.run(
                        f"""
                        MATCH (n)
                        WHERE ANY(l IN labels(n) WHERE l IN $labels)
                          AND n.{field} IS NOT NULL
                          AND n['{precision_key}'] IS NULL
                        RETURN count(*) AS updated
                        """,
                        labels=labels,
                    ).single()
                    precision_count = precision_result.get("updated", 0) if precision_result else 0

                    raw_result = session.run(
                        f"""
                        MATCH (n)
                        WHERE ANY(l IN labels(n) WHERE l IN $labels)
                          AND n.{field} IS NOT NULL
                          AND n['{raw_key}'] IS NULL
                        RETURN count(*) AS updated
                        """,
                        labels=labels,
                    ).single()
                    raw_count = raw_result.get("updated", 0) if raw_result else 0

                    summary[field] = {
                        "precision_backfill_count": precision_count,
                        "raw_backfill_count": raw_count,
                    }
                else:
                    precision_result = session.run(
                        f"""
                        MATCH (n)
                        WHERE ANY(l IN labels(n) WHERE l IN $labels)
                          AND n.{field} IS NOT NULL
                          AND n['{precision_key}'] IS NULL
                        SET n['{precision_key}'] = 'day'
                        RETURN count(*) AS updated
                        """,
                        labels=labels,
                    )
                    precision_count = precision_result.single().get("updated", 0) if precision_result else 0

                    raw_result = session.run(
                        f"""
                        MATCH (n)
                        WHERE ANY(l IN labels(n) WHERE l IN $labels)
                          AND n.{field} IS NOT NULL
                          AND n['{raw_key}'] IS NULL
                        SET n['{raw_key}'] = toString(n.{field})
                        RETURN count(*) AS updated
                        """,
                        labels=labels,
                    )
                    raw_count = raw_result.single().get("updated", 0) if raw_result else 0

                    summary[field] = {
                        "precision_backfill_count": precision_count,
                        "raw_backfill_count": raw_count,
                    }

            return {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "labels": labels,
                "fields": fields,
                "batch_size": batch_size,
                "dry_run": dry_run,
                "summary": summary,
                "done": True,
            }
    finally:
        driver.close()


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

        from datetime import datetime, timezone
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
                    entity_type_filter = "labels(n)[0] = $entity_type"
                    params["entity_type"] = args.entity_type
                else:
                    entity_type_filter = "labels(n)[0] IN ['Legislature', 'Mandate', 'LegislatureTerm']"

                conflict_key = f"{field}_conflict"
                source_key = f"{field}_source"
                precision_key = f"{field}_precision"
                raw_key = f"{field}_raw"
                conflict_with_key = f"{field}_conflict_with"
                
                params.update({
                    "conflict_key": conflict_key,
                    "field_key": field,
                    "source_key": source_key,
                    "precision_key": precision_key,
                    "raw_key": raw_key,
                    "conflict_with_key": conflict_with_key,
                })
                
                query = """
                    MATCH (n)
                    WHERE """ + entity_type_filter + """
                      AND $conflict_key IN keys(n)
                      AND coalesce(n[$conflict_key], false) = true
                    RETURN
                      labels(n)[0] AS entity_type,
                      n.id AS entity_id,
                      n[$field_key] AS canonical_date,
                      n[$source_key] AS source,
                      n[$precision_key] AS precision,
                      n[$raw_key] AS raw,
                      n[$conflict_with_key] AS conflict_with,
                      properties(n) AS props
                    ORDER BY entity_type, entity_id
                    """
                
                rows_raw = session.run(query, **params).data()
                
                rows = []
                for row in rows_raw:
                    compact_row = {
                        "entity_type": row.get("entity_type"),
                        "entity_id": row.get("entity_id"),
                        "canonical_date": row.get("canonical_date"),
                        "source": row.get("source"),
                        "precision": row.get("precision"),
                        "raw": row.get("raw"),
                        "conflict_with": row.get("conflict_with"),
                    }
                    props = row.get("props", {})
                    if conflict_key in props:
                        compact_row["conflict_flag"] = props[conflict_key]
                    rows.append(compact_row)

            output_path = Path(args.out) if args.out else None

            if args.format == "json":
                payload = {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "field": field,
                    "conflict_key": conflict_key,
                    "count": len(rows),
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
                "source",
                "precision",
                "raw",
                "conflict_with",
                "conflict_flag",
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

    if len(sys.argv) > 1 and sys.argv[1] == "audit-mandate-overlaps":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli audit-mandate-overlaps")
        sub.add_argument("--format", choices=["json"], default="json")
        sub.add_argument("--parliament-id", type=str, default="")
        sub.add_argument("--person-id", type=str, default="")
        sub.add_argument("--same-party-only", action="store_true", default=True, help="Only show overlaps with same party (default: True)")
        sub.add_argument("--no-same-party-only", dest="same_party_only", action="store_false", help="Show all overlaps, including different parties")
        sub.add_argument("--limit", type=int, default=50)
        sub.add_argument("--offset", type=int, default=0)
        args = sub.parse_args(sys.argv[2:])

        from datetime import datetime, timezone
        from neo4j import GraphDatabase

        from scraper.config import get_settings

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                where_clauses = []
                params: dict[str, Any] = {
                    "same_party_only": args.same_party_only,
                    "offset": args.offset,
                    "limit": args.limit,
                }

                if args.parliament_id:
                    where_clauses.append("(l1.parliament_id = $parliament_id OR l2.parliament_id = $parliament_id)")
                    params["parliament_id"] = args.parliament_id

                if args.person_id:
                    where_clauses.append("p.id = $person_id")
                    params["person_id"] = args.person_id

                where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""

                query = f"""
                    MATCH (p:Person)-[:HELD]->(m1:Mandate)
                    MATCH (p)-[:HELD]->(m2:Mandate)
                    WHERE m1.id < m2.id
                      AND m1.start_date IS NOT NULL
                      AND m2.start_date IS NOT NULL
                      AND coalesce(m1.start_date, date('0001-01-01')) <= coalesce(m2.end_date, date('9999-12-31'))
                      AND coalesce(m2.start_date, date('0001-01-01')) <= coalesce(m1.end_date, date('9999-12-31'))
                      AND ($same_party_only = false OR m1.party_code = m2.party_code)
                      {where_clause}

                    OPTIONAL MATCH (m1)-[:IN_LEGISLATURE]->(l1:Legislature)
                    OPTIONAL MATCH (m2)-[:IN_LEGISLATURE]->(l2:Legislature)
                    OPTIONAL MATCH (m1)-[:SUPPORTED_BY]->(e1:Evidence)
                    OPTIONAL MATCH (m2)-[:SUPPORTED_BY]->(e2:Evidence)

                    WITH p, m1, m2, l1, l2,
                         collect(DISTINCT coalesce(e1.url, e1.source_url)) AS e1_urls,
                         collect(DISTINCT coalesce(e2.url, e2.source_url)) AS e2_urls

                    RETURN
                      p.id AS person_id,
                      p.name AS person_name,
                      m1.id AS mandate1_id,
                      m1.party_code AS mandate1_party_code,
                      toString(m1.start_date) AS mandate1_start_date,
                      toString(m1.end_date) AS mandate1_end_date,
                      coalesce(l1.name, l1.parliament) AS mandate1_legislature,
                      e1_urls AS mandate1_evidence_urls,

                      m2.id AS mandate2_id,
                      m2.party_code AS mandate2_party_code,
                      toString(m2.start_date) AS mandate2_start_date,
                      toString(m2.end_date) AS mandate2_end_date,
                      coalesce(l2.name, l2.parliament) AS mandate2_legislature,
                      e2_urls AS mandate2_evidence_urls

                    ORDER BY person_name
                    SKIP $offset LIMIT $limit
                """

                rows = session.run(query, **params).data()

                out_rows = []
                for row in rows:
                    e1_urls = [url for url in (row.get("mandate1_evidence_urls") or []) if url]
                    e2_urls = [url for url in (row.get("mandate2_evidence_urls") or []) if url]
                    
                    out_rows.append({
                        "person_id": row.get("person_id"),
                        "person_name": row.get("person_name"),
                        "mandate1_id": row.get("mandate1_id"),
                        "mandate1_party_code": row.get("mandate1_party_code"),
                        "mandate1_start_date": row.get("mandate1_start_date"),
                        "mandate1_end_date": row.get("mandate1_end_date"),
                        "mandate1_legislature": row.get("mandate1_legislature"),
                        "mandate1_evidence_urls": e1_urls,
                        "mandate2_id": row.get("mandate2_id"),
                        "mandate2_party_code": row.get("mandate2_party_code"),
                        "mandate2_start_date": row.get("mandate2_start_date"),
                        "mandate2_end_date": row.get("mandate2_end_date"),
                        "mandate2_legislature": row.get("mandate2_legislature"),
                        "mandate2_evidence_urls": e2_urls,
                    })

                payload = {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "meta": {
                        "limit": args.limit,
                        "offset": args.offset,
                        "returned": len(out_rows),
                        "same_party_only": args.same_party_only,
                        "parliament_id": args.parliament_id or None,
                        "person_id": args.person_id or None,
                    },
                    "rows": out_rows,
                }
                content = json.dumps(payload, ensure_ascii=False, indent=2)
                print(content)
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
                           n.{field}_source AS source_url,
                           properties(n) AS props
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

    if len(sys.argv) > 1 and sys.argv[1] == "ingest-manual-legislature-starts":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli ingest-manual-legislature-starts")
        sub.add_argument("--force", action="store_true", help="Allow overwriting existing start_date")
        sub.add_argument("--file", type=str, default="data/manual_legislature_start_dates.yaml", help="Path to YAML file")
        args = sub.parse_args(sys.argv[2:])

        import yaml
        import re
        from uuid import uuid4
        from datetime import datetime, timezone

        from neo4j import GraphDatabase
        from scraper.config import get_settings

        yaml_path = Path(args.file)
        if not yaml_path.exists():
            print(f"Error: YAML file not found: {yaml_path}", file=sys.stderr)
            sys.exit(1)

        with yaml_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        legislatures = data.get("legislatures", [])
        if not legislatures:
            print("No legislatures found in YAML file", file=sys.stderr)
            sys.exit(0)

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        
        ingested_count = 0
        skipped_count = 0
        error_count = 0
        errors = []

        try:
            with driver.session() as session:
                for entry in legislatures:
                    parliament_id = entry.get("parliament_id")
                    term_number = entry.get("term_number")
                    start_date = entry.get("start_date")
                    evidence_url = entry.get("evidence_url")
                    note = entry.get("note", "")

                    if not parliament_id or term_number is None:
                        errors.append(f"Missing parliament_id or term_number: {entry}")
                        error_count += 1
                        continue

                    if not start_date:
                        errors.append(f"Missing start_date for {parliament_id} term {term_number}")
                        error_count += 1
                        continue

                    if not re.match(r"^\d{4}-\d{2}-\d{2}$", start_date):
                        errors.append(f"Invalid start_date format (must be YYYY-MM-DD): {start_date} for {parliament_id} term {term_number}")
                        error_count += 1
                        continue

                    if not evidence_url or not evidence_url.strip():
                        errors.append(f"Missing or empty evidence_url for {parliament_id} term {term_number}")
                        error_count += 1
                        continue

                    legislature_id = f"{parliament_id}_lt_{term_number}"

                    result = session.run(
                        """
                        MATCH (l:Legislature {id: $legislature_id})
                        RETURN l.start_date AS current_start_date,
                               l.start_date_precision AS current_precision,
                               l.start_date_source AS current_source
                        """,
                        legislature_id=legislature_id,
                    ).single()

                    if not result:
                        errors.append(f"Legislature not found: {legislature_id}")
                        error_count += 1
                        continue

                    current_start_date = result.get("current_start_date")
                    if current_start_date and not args.force:
                        skipped_count += 1
                        continue

                    evidence_id = str(uuid4())

                    def ingest_tx(tx):
                        tx.run(
                            """
                            MATCH (l:Legislature {id: $legislature_id})
                            SET l.start_date = date($start_date),
                                l.start_date_precision = 'day',
                                l.start_date_raw = $start_date,
                                l.start_date_source = $evidence_url
                            """,
                            legislature_id=legislature_id,
                            start_date=start_date,
                            evidence_url=evidence_url,
                        )

                        tx.run(
                            """
                            MERGE (e:Evidence {url: $evidence_url})
                            ON CREATE SET e.id = $evidence_id,
                                          e.retrieved_at = $retrieved_at,
                                          e.source_type = 'manual_registry'
                            """,
                            evidence_url=evidence_url,
                            evidence_id=evidence_id,
                            retrieved_at=datetime.now(timezone.utc).isoformat(),
                        )

                        tx.run(
                            """
                            MATCH (l:Legislature {id: $legislature_id})
                            MATCH (e:Evidence {url: $evidence_url})
                            MERGE (l)-[:SUPPORTED_BY {purpose: 'manual_start_date'}]->(e)
                            """,
                            legislature_id=legislature_id,
                            evidence_url=evidence_url,
                        )

                        if current_start_date and args.force:
                            audit_event_id = str(uuid4())
                            tx.run(
                                """
                                MATCH (l:Legislature {id: $legislature_id})
                                CREATE (e:AuditEvent {
                                    id: $audit_event_id,
                                    at: $at,
                                    actor: $actor,
                                    action: 'set_date',
                                    entity_type: 'Legislature',
                                    entity_id: $legislature_id,
                                    field: 'start_date',
                                    previous: $previous,
                                    next: $next,
                                    source_url: $source_url,
                                    reason: $reason
                                })
                                CREATE (e)-[:AFFECTS]->(l)
                                """,
                                legislature_id=legislature_id,
                                audit_event_id=audit_event_id,
                                at=datetime.now(timezone.utc).isoformat(),
                                actor="cli:ingest-manual-legislature-starts",
                                previous=str(current_start_date),
                                next=start_date,
                                source_url=evidence_url,
                                reason=f"Manual registry entry (force overwrite). Note: {note}" if note else "Manual registry entry (force overwrite)",
                            )

                    session.write_transaction(ingest_tx)
                    ingested_count += 1

        finally:
            driver.close()

        result = {
            "ingested": ingested_count,
            "skipped": skipped_count,
            "errors": error_count,
            "error_details": errors,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if error_count > 0:
            sys.exit(1)
        return

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

    if len(sys.argv) > 1 and sys.argv[1] == "migrate-evidence-links":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli migrate-evidence-links")
        sub.add_argument(
            "--labels",
            type=str,
            default="Legislature,Mandate,LegislatureTerm",
            help="Comma-separated list of node labels to process",
        )
        sub.add_argument(
            "--batch-size",
            type=int,
            default=2000,
            help="Batch size for transactions",
        )
        sub.add_argument("--dry-run", action="store_true", help="Show counts only, no writes")
        args = sub.parse_args(sys.argv[2:])

        labels = [l.strip() for l in args.labels.split(",") if l.strip()]
        payload = _migrate_evidence_links(labels, args.batch_size, args.dry_run)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if len(sys.argv) > 1 and sys.argv[1] == "backfill-date-metadata":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli backfill-date-metadata")
        sub.add_argument(
            "--labels",
            type=str,
            default="Legislature,Mandate,LegislatureTerm",
            help="Comma-separated list of node labels to process",
        )
        sub.add_argument(
            "--fields",
            type=str,
            default="start_date,end_date",
            help="Comma-separated list of date fields to backfill",
        )
        sub.add_argument(
            "--batch-size",
            type=int,
            default=10000,
            help="Batch size for transactions",
        )
        sub.add_argument("--dry-run", action="store_true", help="Show counts only, no writes")
        args = sub.parse_args(sys.argv[2:])

        labels = [l.strip() for l in args.labels.split(",") if l.strip()]
        fields = [f.strip() for f in args.fields.split(",") if f.strip()]
        payload = _backfill_date_metadata(labels, fields, args.batch_size, args.dry_run)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if len(sys.argv) > 1 and sys.argv[1] == "migrate-governance-legacy":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli migrate-governance-legacy")
        sub.add_argument(
            "--labels",
            type=str,
            default="Legislature,Mandate,LegislatureTerm",
            help="Comma-separated list of node labels to process",
        )
        sub.add_argument(
            "--evidence-batch-size",
            type=int,
            default=2000,
            help="Batch size for evidence migration transactions",
        )
        sub.add_argument(
            "--metadata-batch-size",
            type=int,
            default=10000,
            help="Batch size for date metadata backfill transactions",
        )
        sub.add_argument("--dry-run", action="store_true", help="Show counts only, no writes")
        args = sub.parse_args(sys.argv[2:])

        from datetime import datetime, timezone

        labels = [l.strip() for l in args.labels.split(",") if l.strip()]
        evidence_data = _migrate_evidence_links(labels, args.evidence_batch_size, args.dry_run)
        metadata_data = _backfill_date_metadata(labels, ["start_date", "end_date"], args.metadata_batch_size, args.dry_run)

        evidence_summary = {
            "nodes_scanned": evidence_data.get("nodes_scanned", 0),
            "urls_discovered": evidence_data.get("urls_discovered", 0),
            "evidence_nodes_merged": evidence_data.get("evidence_nodes_merged", 0),
            "rels_merged": evidence_data.get("rels_merged", 0),
        }

        metadata_summary = {}
        for field in ["start_date", "end_date"]:
            field_data = metadata_data.get("summary", {}).get(field, {})
            metadata_summary[field] = {
                "precision": field_data.get("precision_backfill_count", 0),
                "raw": field_data.get("raw_backfill_count", 0),
            }

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "labels": labels,
            "evidence_migration": evidence_summary,
            "metadata_backfill": metadata_summary,
            "done": True,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if len(sys.argv) > 1 and sys.argv[1] == "audit-evidence-keys":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli audit-evidence-keys")
        sub.add_argument(
            "--labels",
            type=str,
            default="Legislature,Mandate,LegislatureTerm",
            help="Comma-separated list of node labels to process",
        )
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase
        from scraper.config import get_settings
        from datetime import datetime, timezone

        labels = [l.strip() for l in args.labels.split(",") if l.strip()]

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                result = session.run(
                    """
                    MATCH (n)
                    WHERE ANY(l IN labels(n) WHERE l IN $labels)
                    UNWIND keys(n) AS key
                    WITH key,
                         CASE
                           WHEN toLower(key) CONTAINS 'evidence' OR toLower(key) CONTAINS 'source' THEN 1
                           ELSE 0
                         END AS is_evidence_key
                    WHERE is_evidence_key = 1
                    WITH key, count(*) AS node_count
                    RETURN key, node_count
                    ORDER BY node_count DESC
                    """,
                    labels=labels,
                ).data()

                keys_data = [{"key": row.get("key"), "node_count": row.get("node_count", 0)} for row in result]

                payload = {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "labels": labels,
                    "evidence_keys": keys_data,
                }
                print(json.dumps(payload, ensure_ascii=False, indent=2))
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "audit-warnings":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli audit-warnings")
        sub.add_argument(
            "--mode",
            type=str,
            default="integrity",
            choices=["integrity", "completeness", "all", "governance"],
            help="Validation mode",
        )
        sub.add_argument(
            "--limit",
            type=int,
            default=5,
            help="Sample limit per code",
        )
        sub.add_argument(
            "--top",
            type=int,
            default=15,
            help="Top codes to show",
        )
        sub.add_argument(
            "--input",
            type=str,
            default="",
            help="Path to existing validate JSON (if not provided, run validator)",
        )
        sub.add_argument(
            "--output",
            type=str,
            default="",
            help="Optional artifact path to write full JSON",
        )
        args = sub.parse_args(sys.argv[2:])

        from collections import Counter
        from datetime import datetime, timezone
        from pathlib import Path

        if args.input:
            input_path = Path(args.input)
            if not input_path.exists():
                print(f"Error: Input file not found: {input_path}", file=sys.stderr)
                sys.exit(1)
            with input_path.open("r", encoding="utf-8") as f:
                validation_data = json.load(f)
        else:
            import subprocess as sp

            cmd = ["scraper", "validate", "--json", "--quiet", "--mode", args.mode]
            result = sp.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
            if result.returncode != 0 and result.returncode != 2:
                print(f"Error running validator: {result.stderr}", file=sys.stderr)
                sys.exit(1)
            try:
                validation_data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                print(f"Error parsing validator JSON: {e}", file=sys.stderr)
                print(f"Validator output: {result.stdout[:500]}", file=sys.stderr)
                sys.exit(1)

        warnings = validation_data.get("warnings", [])
        warning_count = len(warnings)

        code_counter = Counter(w.get("code", "UNKNOWN") for w in warnings)
        by_code = [{"code": code, "count": count} for code, count in code_counter.most_common(args.top)]

        samples: dict[str, list[dict[str, Any]]] = {}
        for code, _ in code_counter.most_common(args.top):
            code_warnings = [w for w in warnings if w.get("code") == code]
            samples[code] = code_warnings[: args.limit]

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "warning_count": warning_count,
            "by_code": by_code,
            "samples": samples,
        }

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if len(sys.argv) > 1 and sys.argv[1] == "check-warning-budget":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli check-warning-budget")
        sub.add_argument(
            "--baseline",
            type=str,
            required=True,
            help="Path to baseline validation JSON",
        )
        sub.add_argument(
            "--current",
            type=str,
            required=True,
            help="Path to current validation JSON",
        )
        sub.add_argument(
            "--policy",
            type=str,
            default="config/validation_warning_policy.yaml",
            help="Path to warning policy YAML",
        )
        sub.add_argument(
            "--max-increase",
            type=int,
            default=0,
            help="Maximum allowed warning count increase",
        )
        sub.add_argument(
            "--fail-on-actionable",
            action="store_true",
            default=True,
            help="Fail if actionable warnings present (default: True)",
        )
        sub.add_argument(
            "--no-fail-on-actionable",
            dest="fail_on_actionable",
            action="store_false",
            help="Do not fail on actionable warnings",
        )
        sub.add_argument(
            "--output",
            type=str,
            default="",
            help="Optional JSON report artifact path",
        )
        sub.add_argument(
            "--print-summary",
            action="store_true",
            help="Print summary to stderr",
        )
        args = sub.parse_args(sys.argv[2:])

        from collections import Counter
        from datetime import datetime, timezone
        from pathlib import Path

        from langgraph_app.validation.warning_policy import WarningPolicy

        baseline_path = Path(args.baseline)
        current_path = Path(args.current)

        if not baseline_path.exists():
            print(f"Error: Baseline file not found: {baseline_path}", file=sys.stderr)
            sys.exit(2)
        if not current_path.exists():
            print(f"Error: Current file not found: {current_path}", file=sys.stderr)
            sys.exit(2)

        with baseline_path.open("r", encoding="utf-8") as f:
            baseline_data = json.load(f)
        with current_path.open("r", encoding="utf-8") as f:
            current_data = json.load(f)

        baseline_warnings = baseline_data.get("warnings", [])
        current_warnings = current_data.get("warnings", [])

        baseline_count = len(baseline_warnings)
        current_count = len(current_warnings)
        delta_count = current_count - baseline_count

        baseline_codes = Counter(w.get("code", "UNKNOWN") for w in baseline_warnings)
        current_codes = Counter(w.get("code", "UNKNOWN") for w in current_warnings)

        code_deltas: list[dict[str, Any]] = []
        all_codes = set(baseline_codes.keys()) | set(current_codes.keys())
        for code in all_codes:
            baseline_val = baseline_codes.get(code, 0)
            current_val = current_codes.get(code, 0)
            delta = current_val - baseline_val
            if delta != 0:
                code_deltas.append({"code": code, "baseline": baseline_val, "current": current_val, "delta": delta})

        code_deltas.sort(key=lambda x: abs(x["delta"]), reverse=True)

        policy_path = Path(args.policy)
        policy = WarningPolicy.load(policy_path) if policy_path.exists() else WarningPolicy([], [], [])

        actionable_warnings = [w for w in current_warnings if policy.is_actionable(w.get("code", ""))]
        actionable_count = len(actionable_warnings)
        actionable_codes = list(set(w.get("code", "UNKNOWN") for w in actionable_warnings))

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "baseline": {"warning_count": baseline_count},
            "current": {"warning_count": current_count},
            "delta": {"warning_count": delta_count},
            "code_deltas": code_deltas[:10],
            "actionable": {"count": actionable_count, "codes": actionable_codes},
        }

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        if args.print_summary:
            print(f"Baseline warnings: {baseline_count}", file=sys.stderr)
            print(f"Current warnings: {current_count}", file=sys.stderr)
            print(f"Delta: {delta_count:+d}", file=sys.stderr)
            if actionable_count > 0:
                print(f"Actionable warnings: {actionable_count} ({', '.join(actionable_codes)})", file=sys.stderr)
            if code_deltas:
                print("\nTop 10 code deltas:", file=sys.stderr)
                for cd in code_deltas[:10]:
                    print(f"  {cd['code']}: {cd['baseline']} -> {cd['current']} ({cd['delta']:+d})", file=sys.stderr)

        print(json.dumps(report, ensure_ascii=False, indent=2))

        exit_code = 0
        if args.fail_on_actionable and actionable_count > 0:
            exit_code = 2
        elif delta_count > args.max_increase:
            exit_code = 2

        sys.exit(exit_code)

    if len(sys.argv) > 1 and sys.argv[1] == "audit-mandate-missing-starts":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli audit-mandate-missing-starts")
        sub.add_argument("--parliament-id", type=str, default="", help="Filter by parliament_id")
        sub.add_argument("--limit", type=int, default=1000, help="Limit for query results")
        sub.add_argument("--top", type=int, default=20, help="Top N samples per category")
        sub.add_argument("--output", type=str, default="", help="Output JSON file path")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase
        from scraper.config import get_settings
        from collections import defaultdict
        import datetime as dt_module
        from pathlib import Path

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                where_clause = "WHERE m.start_date IS NULL"
                params: dict[str, Any] = {"limit": args.limit}
                if args.parliament_id:
                    where_clause += " AND m.parliament_id = $parliament_id"
                    params["parliament_id"] = args.parliament_id

                query = f"""
                MATCH (m:Mandate)
                {where_clause}
                OPTIONAL MATCH (m)-[:IN_LEGISLATURE]->(l)
                OPTIONAL MATCH (m)-[:HELD]->(p:Person)
                OPTIONAL MATCH (l)-[:SUPPORTED_BY]->(e:Evidence)
                WITH m, l, p,
                     collect(DISTINCT coalesce(e.url, e.source_url)) AS evidence_node_urls
                WITH m, l, p, evidence_node_urls,
                     coalesce(l.start_date, '') AS legislature_start_date,
                     coalesce(l['start_date_precision'], '') AS legislature_start_precision,
                     coalesce(l['start_date_source'], '') AS legislature_start_source,
                     coalesce(l['start_date_evidence_urls'], []) AS legislature_evidence_urls,
                     CASE 
                       WHEN l IS NOT NULL AND EXISTS {{ (l)-[:SUPPORTED_BY]->(:Evidence) }} THEN true
                       WHEN l IS NOT NULL AND EXISTS {{ (l)-[:SUPPORTED_BY]->(x) WHERE x.url IS NOT NULL }} THEN true
                       WHEN l IS NOT NULL AND coalesce(trim(l['start_date_source']), '') <> '' THEN true
                       WHEN l IS NOT NULL AND size(coalesce(l['start_date_evidence_urls'], [])) > 0 THEN true
                       ELSE false
                     END AS has_legislature_evidence
                RETURN
                    m.id AS mandate_id,
                    coalesce(p.name, '') AS person_name,
                    m.parliament_id AS parliament_id,
                    coalesce(l.id, '') AS legislature_id,
                    coalesce(l.name, l.parliament, '') AS legislature_name,
                    coalesce(l.term_number, -1) AS term_number,
                    legislature_start_date,
                    legislature_start_precision,
                    legislature_start_source,
                    legislature_evidence_urls,
                    evidence_node_urls,
                    has_legislature_evidence
                LIMIT $limit
                """
                rows = session.run(query, **params).data()

                def classify_root_cause(row: dict) -> str:
                    legislature_id = row.get("legislature_id", "")
                    legislature_start_date = row.get("legislature_start_date", "")
                    legislature_start_precision = row.get("legislature_start_precision", "")
                    has_legislature_evidence = row.get("has_legislature_evidence", False)

                    if not legislature_id or legislature_id == "":
                        return "missing_legislature_link"
                    
                    if not legislature_start_date or legislature_start_date == "":
                        return "legislature_missing_start_date"
                    
                    if legislature_start_precision != "day":
                        return "legislature_start_not_day_precision"
                    
                    if not has_legislature_evidence:
                        return "legislature_start_missing_evidence"
                    
                    return "backfillable_from_legislature"

                classified = defaultdict(list)
                by_parliament_term = defaultdict(int)
                by_root_cause = defaultdict(int)

                for row in rows:
                    root_cause = classify_root_cause(row)
                    classified[root_cause].append(row)
                    by_root_cause[root_cause] += 1
                    
                    parliament_id = row.get("parliament_id", "")
                    term_number = row.get("term_number", -1)
                    if parliament_id and term_number >= 0:
                        by_parliament_term[f"{parliament_id}:{term_number}"] += 1

                samples: dict[str, list[dict[str, Any]]] = {}
                for root_cause, items in classified.items():
                    samples[root_cause] = items[:args.top]

                by_parliament_term_list = [
                    {"parliament_id": k.split(":")[0], "term_number": int(k.split(":")[1]), "count": v}
                    for k, v in sorted(by_parliament_term.items(), key=lambda x: x[1], reverse=True)
                ]

                payload = {
                    "generated_at": dt_module.datetime.now(dt_module.timezone.utc).isoformat(),
                    "total_missing": len(rows),
                    "by_root_cause": dict(by_root_cause),
                    "by_parliament_term": by_parliament_term_list[:50],
                    "samples": {k: v[:args.top] for k, v in samples.items()},
                }

                if args.output:
                    output_path = Path(args.output)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

                print(json.dumps(payload, ensure_ascii=False, indent=2))
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "fix-mandate-starts-from-legislature":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli fix-mandate-starts-from-legislature")
        sub.add_argument("--parliament-id", type=str, default="", help="Filter by parliament_id")
        sub.add_argument("--dry-run", action="store_true", help="Show what would be changed without making changes")
        args = sub.parse_args(sys.argv[2:])

        from neo4j import GraphDatabase
        from scraper.config import get_settings
        from langgraph_app.governance.dates import GovernedDate, DatePrecision, apply_governed_date
        from collections import defaultdict

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                where_clause = """WHERE m.start_date IS NULL 
                                  AND l.start_date IS NOT NULL 
                                  AND l.start_date_precision = 'day'"""
                params: dict[str, Any] = {}
                if args.parliament_id:
                    where_clause += " AND m.parliament_id = $parliament_id"
                    params["parliament_id"] = args.parliament_id

                query = f"""
                MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l)
                {where_clause}
                OPTIONAL MATCH (l)-[:SUPPORTED_BY]->(e:Evidence)
                WITH m, l,
                     collect(DISTINCT coalesce(e.url, e.source_url)) AS evidence_node_urls
                WITH m, l, evidence_node_urls,
                     l.start_date AS legislature_start_date,
                     coalesce(l['start_date_source'], '') AS legislature_start_source,
                     coalesce(l['start_date_evidence_urls'], []) AS legislature_evidence_urls
                WHERE legislature_start_date IS NOT NULL
                  AND (
                    legislature_start_source <> '' 
                    OR size(legislature_evidence_urls) > 0 
                    OR size(evidence_node_urls) > 0
                    OR EXISTS {{ (l)-[:SUPPORTED_BY]->(:Evidence) }}
                    OR EXISTS {{ (l)-[:SUPPORTED_BY]->(x) WHERE x.url IS NOT NULL }}
                  )
                RETURN
                    m.id AS mandate_id,
                    legislature_start_date,
                    legislature_start_source,
                    legislature_evidence_urls,
                    evidence_node_urls
                """
                rows = session.run(query, **params).data()

                scanned = len(rows)
                updated = 0
                skipped_by_reason: dict[str, int] = defaultdict(int)

                for row in rows:
                    mandate_id = row.get("mandate_id")
                    start_date = row.get("legislature_start_date")
                    source_url = row.get("legislature_start_source", "").strip()
                    evidence_urls = (row.get("legislature_evidence_urls") or []) + (row.get("evidence_node_urls") or [])
                    evidence_urls = [url for url in evidence_urls if url and url.strip()]

                    if not mandate_id or not start_date:
                        skipped_by_reason["missing_data"] += 1
                        continue

                    if not source_url and not evidence_urls:
                        skipped_by_reason["no_evidence"] += 1
                        continue

                    if not source_url:
                        source_url = evidence_urls[0] if evidence_urls else ""

                    if source_url not in evidence_urls:
                        evidence_urls.insert(0, source_url)

                    if args.dry_run:
                        updated += 1
                        continue

                    governed_start = GovernedDate(
                        iso_day=start_date,
                        precision=DatePrecision.DAY,
                        raw=None,
                        source_kind="propagate_legislature_start",
                        source_url=source_url,
                        evidence_urls=evidence_urls,
                        method="propagate_legislature_start",
                        reason="Propagated from Legislature.start_date",
                    )

                    def apply_mandate_start(tx):
                        return apply_governed_date(
                            tx,
                            "Mandate",
                            mandate_id,
                            "start_date",
                            governed_start,
                            "cli:fix-mandate-starts-from-legislature",
                            allow_force=False,
                        )

                    result = session.write_transaction(apply_mandate_start)
                    if result.canonical_written:
                        updated += 1
                    else:
                        skipped_by_reason["not_applied"] += 1

                payload = {
                    "scanned": scanned,
                    "updated": updated,
                    "skipped_by_reason": dict(skipped_by_reason),
                    "dry_run": args.dry_run,
                }
                print(json.dumps(payload, ensure_ascii=False, indent=2))
        finally:
            driver.close()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "export-curation-queue":
        sub = argparse.ArgumentParser(prog="langgraph_app.cli export-curation-queue")
        sub.add_argument("--type", type=str, default="mandate_missing_start", help="Queue type")
        sub.add_argument("--output", type=str, default="artifacts/curation_queue.mandate_missing_start.json", help="Output file path")
        args = sub.parse_args(sys.argv[2:])

        if args.type != "mandate_missing_start":
            print(f"Error: Unsupported queue type: {args.type}", file=sys.stderr)
            sys.exit(1)

        from neo4j import GraphDatabase
        from scraper.config import get_settings
        from collections import defaultdict
        import datetime as dt_module
        from pathlib import Path

        settings = get_settings()
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        try:
            with driver.session() as session:
                query = """
                MATCH (m:Mandate)
                WHERE m.start_date IS NULL
                OPTIONAL MATCH (m)-[:IN_LEGISLATURE]->(l)
                OPTIONAL MATCH (m)-[:HELD]->(p:Person)
                OPTIONAL MATCH (l)-[:SUPPORTED_BY]->(e:Evidence)
                WITH m, l, p,
                     collect(DISTINCT coalesce(e.url, e.source_url)) AS evidence_node_urls
                WITH m, l, p, evidence_node_urls,
                     coalesce(l.id, '') AS legislature_id,
                     coalesce(l.name, l.parliament, '') AS legislature_name,
                     coalesce(l.term_number, -1) AS term_number,
                     coalesce(l.start_date, '') AS legislature_start_date,
                     coalesce(l['start_date_precision'], '') AS legislature_start_precision,
                     coalesce(l['start_date_source'], '') AS legislature_start_source,
                     coalesce(l['start_date_evidence_urls'], []) AS legislature_evidence_urls,
                     CASE 
                       WHEN l IS NOT NULL AND EXISTS { (l)-[:SUPPORTED_BY]->(:Evidence) } THEN true
                       WHEN l IS NOT NULL AND EXISTS { (l)-[:SUPPORTED_BY]->(x) WHERE x.url IS NOT NULL } THEN true
                       WHEN l IS NOT NULL AND coalesce(trim(l['start_date_source']), '') <> '' THEN true
                       WHEN l IS NOT NULL AND size(coalesce(l['start_date_evidence_urls'], [])) > 0 THEN true
                       ELSE false
                     END AS has_legislature_evidence
                WHERE l.id IS NOT NULL AND l.id <> ''
                  AND (legislature_start_date = '' 
                       OR legislature_start_precision <> 'day'
                       OR NOT has_legislature_evidence)
                RETURN
                    m.id AS mandate_id,
                    coalesce(p.name, '') AS person_name,
                    m.parliament_id AS parliament_id,
                    legislature_id,
                    legislature_name,
                    term_number,
                    legislature_start_date,
                    legislature_start_precision,
                    legislature_start_source,
                    legislature_evidence_urls,
                    evidence_node_urls
                """
                rows = session.run(query).data()

                grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {
                    "parliament_id": "",
                    "term_number": -1,
                    "legislature_id": "",
                    "legislature_name": "",
                    "mandates_missing_start_count": 0,
                    "sample_mandate_ids": [],
                    "source_candidates": [],
                    "recommended_action": "Add official source entry for constituting session date (day)",
                })

                for row in rows:
                    parliament_id = row.get("parliament_id", "")
                    term_number = row.get("term_number", -1)
                    legislature_id = row.get("legislature_id", "")
                    key = f"{parliament_id}:{term_number}:{legislature_id}"

                    if key not in grouped:
                        grouped[key]["parliament_id"] = parliament_id
                        grouped[key]["term_number"] = term_number
                        grouped[key]["legislature_id"] = legislature_id
                        grouped[key]["legislature_name"] = row.get("legislature_name", "")

                    grouped[key]["mandates_missing_start_count"] += 1
                    if len(grouped[key]["sample_mandate_ids"]) < 10:
                        grouped[key]["sample_mandate_ids"].append(row.get("mandate_id", ""))

                    evidence_urls = (row.get("legislature_evidence_urls") or []) + (row.get("evidence_node_urls") or [])
                    for url in evidence_urls:
                        if url and url.strip() and url not in grouped[key]["source_candidates"]:
                            grouped[key]["source_candidates"].append(url)

                queue_items = sorted(grouped.values(), key=lambda x: (x["parliament_id"], x["term_number"]))

                payload = {
                    "generated_at": dt_module.datetime.now(dt_module.timezone.utc).isoformat(),
                    "type": args.type,
                    "total_items": len(queue_items),
                    "items": queue_items,
                }

                output_path = Path(args.output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

                print(json.dumps(payload, ensure_ascii=False, indent=2))
        finally:
            driver.close()
        return

    known_commands = {
        "list-missing-starts",
        "list-missing-legislature-starts",
        "ingest-official-terms",
        "generate-official-terms-skeleton",
        "ingest-wikidata-term",
        "ingest-wikidata-terms",
        "propagate-legislature-starts",
        "audit-date-conflicts",
        "audit-mandate-overlaps",
        "audit-missing-canonical-dates",
        "ingest-manual-legislature-starts",
        "resolve-date-conflict",
        "migrate-evidence-links",
        "backfill-date-metadata",
        "migrate-governance-legacy",
        "audit-evidence-keys",
        "audit-warnings",
        "check-warning-budget",
        "audit-mandate-missing-starts",
        "fix-mandate-starts-from-legislature",
        "export-curation-queue",
    }

    if len(sys.argv) > 1:
        first_arg = sys.argv[1]
        if first_arg not in known_commands and not first_arg.startswith("-"):
            print(f"Unknown command: {first_arg}", file=sys.stderr)
            print("\nAvailable commands:", file=sys.stderr)
            for cmd in sorted(known_commands):
                print(f"  {cmd}", file=sys.stderr)
            print("\nFor query/chat mode, provide a question as positional argument.", file=sys.stderr)
            print("Use --help for more information.", file=sys.stderr)
            sys.exit(2)

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

    question = " ".join(args.question).strip() if args.question else ""

    def ensure_llm_ready() -> None:
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

    if question:
        ensure_llm_ready()
        print(asyncio.run(run_once(question, args.format, args.sources, args.max_sources)))
        return

    if sys.stdin.isatty():
        ensure_llm_ready()
        q = DEFAULT_QUESTION
        print(asyncio.run(run_once(q, args.format, args.sources, args.max_sources)))
        while True:
            next_q = _read_stdin_interactive()
            if not next_q:
                break
            print(asyncio.run(run_once(next_q, args.format, args.sources, args.max_sources)))
        return

    ensure_llm_ready()
    print(asyncio.run(run_once(DEFAULT_QUESTION, args.format, args.sources, args.max_sources)))


if __name__ == "__main__":
    main()


