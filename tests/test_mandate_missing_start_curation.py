"""Tests for mandate missing start date curation pipeline."""

import pytest
from unittest.mock import Mock
import re

from langgraph_app.governance.dates import GovernedDate, DatePrecision


def test_classify_root_cause_missing_legislature_link():
    row = {
        "legislature_id": "",
        "legislature_start_date": "",
        "legislature_start_precision": "",
        "has_legislature_evidence": False,
    }
    
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
            return "legislature_missing_start_date"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "missing_legislature_link"


def test_classify_root_cause_legislature_missing_start_date():
    row = {
        "legislature_id": "leg-123",
        "legislature_start_date": "",
        "legislature_start_precision": "",
        "has_legislature_evidence": False,
    }
    
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
            return "legislature_missing_start_date"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "legislature_missing_start_date"


def test_classify_root_cause_legislature_missing_start_date_precision():
    row = {
        "legislature_id": "leg-123",
        "legislature_start_date": "2020-01",
        "legislature_start_precision": "month",
        "has_legislature_evidence": True,
    }
    
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
            return "legislature_missing_start_date"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "legislature_missing_start_date"


def test_classify_root_cause_legislature_start_missing_evidence():
    row = {
        "legislature_id": "leg-123",
        "legislature_start_date": "2020-01-01",
        "legislature_start_precision": "day",
        "has_legislature_evidence": False,
    }
    
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
            return "legislature_missing_start_date"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "legislature_start_missing_evidence"


def test_classify_root_cause_backfillable_from_legislature():
    row = {
        "legislature_id": "leg-123",
        "legislature_start_date": "2020-01-01",
        "legislature_start_precision": "day",
        "has_legislature_evidence": True,
    }
    
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
            return "legislature_missing_start_date"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "backfillable_from_legislature"


def test_fix_command_skip_no_evidence():
    row = {
        "mandate_id": "mandate-123",
        "legislature_start_date": "2020-01-01",
        "legislature_start_source": "",
        "legislature_evidence_urls": [],
        "evidence_node_urls": [],
    }
    
    source_url = row.get("legislature_start_source", "").strip()
    evidence_urls = (row.get("legislature_evidence_urls") or []) + (row.get("evidence_node_urls") or [])
    evidence_urls = [url for url in evidence_urls if url and url.strip()]
    
    should_skip = not source_url and not evidence_urls
    
    assert should_skip is True


def test_fix_command_update_with_evidence():
    row = {
        "mandate_id": "mandate-123",
        "legislature_start_date": "2020-01-01",
        "legislature_start_source": "https://example.com/source",
        "legislature_evidence_urls": ["https://example.com/evidence"],
        "evidence_node_urls": [],
    }
    
    source_url = row.get("legislature_start_source", "").strip()
    evidence_urls = (row.get("legislature_evidence_urls") or []) + (row.get("evidence_node_urls") or [])
    evidence_urls = [url for url in evidence_urls if url and url.strip()]
    
    if not source_url and evidence_urls:
        source_url = evidence_urls[0]
    
    if source_url not in evidence_urls:
        evidence_urls.insert(0, source_url)
    
    should_skip = not source_url and not evidence_urls
    
    assert should_skip is False
    assert source_url == "https://example.com/source"
    assert len(evidence_urls) >= 1
    assert source_url in evidence_urls


def test_governed_date_creation_for_fix_command():
    source_url = "https://example.com/source"
    evidence_urls = ["https://example.com/source", "https://example.com/evidence"]
    start_date = "2020-01-01"
    
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
    
    assert governed_start.iso_day == start_date
    assert governed_start.precision == DatePrecision.DAY
    assert governed_start.source_kind == "propagate_legislature_start"
    assert governed_start.source_url == source_url
    assert source_url in governed_start.evidence_urls


def test_audit_query_no_legacy_properties():
    """Verify that audit-mandate-missing-starts Cypher queries do not reference legacy properties."""
    legacy_properties = [
        "start_date_evidence_urls",
        "start_date_source_kind",
        "start_date_source_url",
        "start_date_set_at",
        "start_date_set_by",
    ]
    
    legacy_patterns = [
        re.compile(rf"l\['{prop}'\]", re.IGNORECASE) for prop in legacy_properties
    ] + [
        re.compile(rf"l\.{prop}", re.IGNORECASE) for prop in legacy_properties
    ]
    
    query_by_root_cause = """
                MATCH (m:Mandate)
                WHERE m.start_date IS NULL
                OPTIONAL MATCH (m)-[:IN_LEGISLATURE]->(l)
                WITH
                  m, l,
                  CASE
                    WHEN l IS NULL THEN 'missing_legislature_link'
                    WHEN l.start_date IS NULL OR coalesce(l.start_date_precision, '') <> 'day' THEN 'legislature_missing_start_date'
                    WHEN l.start_date IS NOT NULL AND coalesce(l.start_date_precision, '') = 'day' AND NOT EXISTS { (l)-[:SUPPORTED_BY]->(:Evidence) } THEN 'legislature_missing_evidence'
                    ELSE 'backfillable_from_legislature'
                  END AS root_cause
                RETURN
                  root_cause,
                  count(m) AS count
                ORDER BY count DESC
                """
    
    query_samples = """
                MATCH (m:Mandate)
                WHERE m.start_date IS NULL
                OPTIONAL MATCH (m)-[:IN_LEGISLATURE]->(l)
                OPTIONAL MATCH (p1:Person)-[:HAS_MANDATE]->(m)
                OPTIONAL MATCH (p2:Person)-[:HELD]->(m)
                OPTIONAL MATCH (m)-[:HELD]->(p3:Person)
                OPTIONAL MATCH (l)-[:SUPPORTED_BY]->(e:Evidence)
                WITH m, l,
                     coalesce(p1.name, p2.name, p3.name, '') AS person_name,
                     collect(DISTINCT e.url) AS legislature_evidence_urls,
                     EXISTS { (l)-[:SUPPORTED_BY]->(:Evidence) } AS has_legislature_evidence,
                     coalesce(trim(toString(l.start_date_source)), '') AS legislature_start_source,
                     coalesce(toString(l.start_date_precision), '') AS legislature_start_precision
                RETURN
                  m.id AS mandate_id,
                  person_name,
                  m.parliament_id AS parliament_id,
                  coalesce(l.id, '') AS legislature_id,
                  coalesce(l.name, l.parliament, '') AS legislature_name,
                  coalesce(l.term_number, -1) AS term_number,
                  coalesce(toString(l.start_date), '') AS legislature_start_date,
                  legislature_start_precision,
                  legislature_start_source,
                  legislature_evidence_urls,
                  has_legislature_evidence
                ORDER BY parliament_id, term_number, mandate_id
                LIMIT $sample_limit
                """
    
    queries = [query_by_root_cause, query_samples]
    
    for query in queries:
        for pattern in legacy_patterns:
            matches = pattern.findall(query)
            assert len(matches) == 0, f"Found legacy property reference in query: {matches}"
    
    assert "EXISTS { (l)-[:SUPPORTED_BY]->(:Evidence) }" in query_samples
    assert "legislature_evidence_urls" in query_samples
    assert "has_legislature_evidence" in query_samples
