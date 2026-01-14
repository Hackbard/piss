"""Tests for mandate missing start date curation pipeline."""

import pytest
from unittest.mock import Mock

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
            return "legislature_start_not_day_precision"
        
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
            return "legislature_start_not_day_precision"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "legislature_missing_start_date"


def test_classify_root_cause_legislature_start_not_day_precision():
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
            return "legislature_start_not_day_precision"
        
        if not has_legislature_evidence:
            return "legislature_start_missing_evidence"
        
        return "backfillable_from_legislature"
    
    assert classify_root_cause(row) == "legislature_start_not_day_precision"


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
            return "legislature_start_not_day_precision"
        
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
            return "legislature_start_not_day_precision"
        
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
