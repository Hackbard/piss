"""Term start date curation queue export and apply."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from neo4j import GraphDatabase

from langgraph_app.governance.dates import ApplyResult, DatePrecision, GovernedDate, apply_governed_date
from scraper.config import get_settings


def export_term_start_curation_queue(
    output_path: Path,
    min_mandates: int = 1,
    top: int = 200,
    parliament_ids: list[str] | None = None,
) -> dict[str, Any]:
    """
    Export curation queue for Legislature terms missing canonical day start dates.
    
    Returns dict with generated_at, policy, terms.
    """
    settings = get_settings()
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    
    try:
        with driver.session() as session:
            where_parts: list[str] = [
                "(l.start_date IS NULL OR coalesce(l.start_date_precision, '') <> 'day')",
            ]
            params: dict[str, Any] = {
                "min_mandates": min_mandates,
                "top": top,
            }
            
            if parliament_ids:
                where_parts.append("l.parliament_id IN $parliament_ids")
                params["parliament_ids"] = parliament_ids
            
            where_clause = " AND ".join(where_parts)
            
            query = f"""
            MATCH (l:Legislature)
            WHERE {where_clause}
            OPTIONAL MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l)
            WHERE m.start_date IS NULL
            WITH l,
                 count(DISTINCT m) AS mandates_missing_start_count
            WHERE mandates_missing_start_count >= $min_mandates
            OPTIONAL MATCH (l)-[:SUPPORTED_BY]->(e:Evidence)
            WITH l,
                 mandates_missing_start_count,
                 collect(DISTINCT e.url) AS evidence_urls
            RETURN
              l.id AS legislature_id,
              l.parliament_id AS parliament_id,
              coalesce(l.term_number, -1) AS term_number,
              coalesce(l.name, l.parliament, '') AS legislature_name,
              coalesce(l.wikipedia_title, '') AS wikipedia_title,
              mandates_missing_start_count,
              evidence_urls
            ORDER BY mandates_missing_start_count DESC, l.parliament_id, l.term_number
            LIMIT $top
            """
            
            rows = session.run(query, **params).data()
            
            terms: list[dict[str, Any]] = []
            for row in rows:
                legislature_id = row.get("legislature_id", "")
                parliament_id = row.get("parliament_id", "")
                term_number = row.get("term_number", -1)
                legislature_name = row.get("legislature_name", "")
                wikipedia_title = row.get("wikipedia_title", "")
                mandates_count = row.get("mandates_missing_start_count", 0)
                evidence_urls = row.get("evidence_urls", [])
                
                source_candidates: list[dict[str, Any]] = []
                
                for url in evidence_urls:
                    if url and url.strip():
                        if "wikipedia.org" in url or "oldid=" in url:
                            source_candidates.append({
                                "type": "members_list",
                                "url": url,
                            })
                
                if wikipedia_title:
                    source_candidates.append({
                        "type": "wikipedia_title",
                        "title": wikipedia_title,
                    })
                
                source_candidates.append({
                    "type": "official_source",
                    "url": "",
                })
                
                terms.append({
                    "parliament_id": parliament_id,
                    "term_number": term_number,
                    "legislature_id": legislature_id,
                    "legislature_name": legislature_name,
                    "mandates_missing_start_count": mandates_count,
                    "source_candidates": source_candidates,
                    "start_date_day": None,
                    "source_url": None,
                    "evidence_urls": [],
                    "notes": "",
                })
            
            result = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "policy": "Know or NULL (day + evidence only)",
                "terms": terms,
            }
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(result, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            
            return result
    finally:
        driver.close()


def apply_term_start_overrides(
    input_path: Path,
    dry_run: bool = False,
    only_parliament_ids: list[str] | None = None,
    only_term: str | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """
    Apply term start date overrides from YAML file.
    
    Returns dict with processed_terms, applied_terms, skipped_terms, errors, applied_term_identifiers.
    """
    with open(input_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    if not isinstance(data, dict) or "terms" not in data:
        raise ValueError(f"Invalid YAML format: expected dict with 'terms' key")
    
    terms = data.get("terms", [])
    if not isinstance(terms, list):
        raise ValueError("'terms' must be a list")
    
    settings = get_settings()
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    
    processed_terms = 0
    applied_terms = 0
    skipped_terms = 0
    errors: list[dict[str, Any]] = []
    applied_term_identifiers: list[str] = []
    
    try:
        with driver.session() as session:
            for term_data in terms:
                processed_terms += 1
                
                parliament_id = term_data.get("parliament_id", "")
                term_number = term_data.get("term_number")
                legislature_id = term_data.get("legislature_id", "")
                start_date_day = term_data.get("start_date_day")
                source_url = term_data.get("source_url", "")
                evidence_urls = term_data.get("evidence_urls", [])
                
                if only_parliament_ids and parliament_id not in only_parliament_ids:
                    skipped_terms += 1
                    continue
                
                if only_term:
                    parts = only_term.split(":")
                    if len(parts) != 2:
                        errors.append({
                            "term": f"{parliament_id}:{term_number}",
                            "error": f"Invalid --only-term format: {only_term}",
                        })
                        continue
                    if parliament_id != parts[0] or str(term_number) != parts[1]:
                        skipped_terms += 1
                        continue
                
                if start_date_day is None:
                    skipped_terms += 1
                    continue
                
                if not isinstance(start_date_day, str):
                    errors.append({
                        "term": f"{parliament_id}:{term_number}",
                        "error": f"start_date_day must be a string (YYYY-MM-DD), got {type(start_date_day).__name__}",
                    })
                    continue
                
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", start_date_day):
                    errors.append({
                        "term": f"{parliament_id}:{term_number}",
                        "error": f"start_date_day must be ISO format YYYY-MM-DD, got {start_date_day!r}",
                    })
                    continue
                
                if not source_url or not source_url.strip():
                    errors.append({
                        "term": f"{parliament_id}:{term_number}",
                        "error": "source_url is required when start_date_day is set",
                    })
                    continue
                
                if not evidence_urls:
                    evidence_urls = [source_url]
                elif source_url not in evidence_urls:
                    evidence_urls = [source_url] + evidence_urls
                
                if not legislature_id:
                    lookup_query = """
                    MATCH (l:Legislature)
                    WHERE l.parliament_id = $parliament_id AND l.term_number = $term_number
                    RETURN l.id AS legislature_id
                    LIMIT 1
                    """
                    lookup_result = session.run(
                        lookup_query,
                        parliament_id=parliament_id,
                        term_number=term_number,
                    ).single()
                    
                    if not lookup_result:
                        errors.append({
                            "term": f"{parliament_id}:{term_number}",
                            "error": f"Legislature not found for parliament_id={parliament_id}, term_number={term_number}",
                        })
                        continue
                    
                    legislature_id = lookup_result.get("legislature_id", "")
                
                if not legislature_id:
                    errors.append({
                        "term": f"{parliament_id}:{term_number}",
                        "error": "legislature_id is required",
                    })
                    continue
                
                try:
                    governed_start = GovernedDate(
                        iso_day=start_date_day,
                        precision=DatePrecision.DAY,
                        raw=start_date_day,
                        source_kind="manual_curation",
                        source_url=source_url,
                        evidence_urls=evidence_urls,
                        method="manual_term_start_override",
                        reason="Manual curation from term start queue",
                    )
                except ValueError as e:
                    errors.append({
                        "term": f"{parliament_id}:{term_number}",
                        "error": f"Invalid GovernedDate: {e}",
                    })
                    continue
                
                if dry_run:
                    applied_terms += 1
                    applied_term_identifiers.append(f"{parliament_id}:{term_number}")
                    continue
                
                def apply_start(tx):
                    return apply_governed_date(
                        tx,
                        "Legislature",
                        legislature_id,
                        "start_date",
                        governed_start,
                        "cli:apply-term-start-overrides",
                        allow_force=False,
                    )
                
                try:
                    result = session.execute_write(apply_start)
                    if result.canonical_written:
                        for url in evidence_urls:
                            if url and url.strip():
                                session.run(
                                    """
                                    MATCH (l:Legislature {id: $legislature_id})
                                    MERGE (e:Evidence {url: $url})
                                    MERGE (l)-[:SUPPORTED_BY]->(e)
                                    """,
                                    legislature_id=legislature_id,
                                    url=url,
                                )
                        
                        applied_terms += 1
                        applied_term_identifiers.append(f"{parliament_id}:{term_number}")
                    else:
                        errors.append({
                            "term": f"{parliament_id}:{term_number}",
                            "error": f"Failed to write canonical date: {result}",
                        })
                except Exception as e:
                    errors.append({
                        "term": f"{parliament_id}:{term_number}",
                        "error": f"Exception during apply: {e}",
                    })
                    if strict:
                        raise
    
    finally:
        driver.close()
    
    return {
        "processed_terms": processed_terms,
        "applied_terms": applied_terms,
        "skipped_terms": skipped_terms,
        "errors": errors,
        "applied_term_identifiers": applied_term_identifiers,
    }
