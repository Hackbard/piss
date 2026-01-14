"""Tests for governance normalizer."""

import pytest

from langgraph_app.governance.normalize import (
    normalize_date_field,
    normalize_legislature_record,
    normalize_mandate_record,
)


class MockLegislature:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class MockMandate:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestNormalizeDateField:
    def test_non_day_sets_canonical_null_and_sets_raw_precision(self):
        props = normalize_date_field(
            field_name="start_date",
            date_iso=None,
            date_raw="2024",
            date_precision="year",
            source_url="https://example.com",
        )
        
        assert props["start_date"] is None
        assert props["start_date_precision"] == "year"
        assert props["start_date_raw"] == "2024"
        assert props["start_date_source"] == "https://example.com"
    
    def test_day_requires_source_and_evidence_contains_source(self):
        props = normalize_date_field(
            field_name="start_date",
            date_iso="2024-01-15",
            date_raw="15. Januar 2024",
            date_precision="day",
            source_url="https://example.com",
            evidence_urls=["https://example.com", "https://other.com"],
        )
        
        assert props["start_date"] == "2024-01-15"
        assert props["start_date_precision"] == "day"
        assert props["start_date_raw"] == "15. Januar 2024"
        assert props["start_date_source"] == "https://example.com"
    
    def test_day_without_source_sets_canonical_null(self):
        props = normalize_date_field(
            field_name="start_date",
            date_iso="2024-01-15",
            date_raw="15. Januar 2024",
            date_precision="day",
            source_url=None,
        )
        
        assert props["start_date"] is None
        assert props["start_date_precision"] == "unknown"
        assert props["start_date_raw"] == "15. Januar 2024"
        assert props["start_date_source"] is None
    
    def test_day_without_iso_sets_canonical_null(self):
        props = normalize_date_field(
            field_name="start_date",
            date_iso=None,
            date_raw="15. Januar 2024",
            date_precision="day",
            source_url="https://example.com",
        )
        
        assert props["start_date"] is None
        assert props["start_date_precision"] == "unknown"
        assert props["start_date_raw"] == "15. Januar 2024"
        assert props["start_date_source"] == "https://example.com"
    
    def test_unknown_precision_sets_canonical_null(self):
        props = normalize_date_field(
            field_name="start_date",
            date_iso=None,
            date_raw="sometime in 2024",
            date_precision="unknown",
            source_url="https://example.com",
        )
        
        assert props["start_date"] is None
        assert props["start_date_precision"] == "unknown"
        assert props["start_date_raw"] == "sometime in 2024"
        assert props["start_date_source"] == "https://example.com"


class TestNormalizeLegislatureRecord:
    def test_legislature_with_day_precision(self):
        legislature = MockLegislature(
            id="test_legislature",
            parliament_id="NI",
            term_number=17,
            name="17. Landtag Niedersachsen",
            start_date="2013-02-20",
            start_date_raw="20. Februar 2013",
            start_date_precision="day",
            source_url="https://de.wikipedia.org/wiki/17._Landtag_Niedersachsen?oldid=123456",
            evidence_ids=["evidence-123"],
        )
        
        props = normalize_legislature_record(legislature)
        
        assert props["start_date"] == "2013-02-20"
        assert props["start_date_precision"] == "day"
        assert props["start_date_source"] == "https://de.wikipedia.org/wiki/17._Landtag_Niedersachsen?oldid=123456"
        assert props["evidence_ids"] == ["evidence-123"]
    
    def test_legislature_with_year_precision(self):
        legislature = MockLegislature(
            id="test_legislature",
            parliament_id="NI",
            term_number=17,
            name="17. Landtag Niedersachsen",
            start_date=None,
            start_date_raw="2013",
            start_date_precision="year",
            source_url="https://example.com",
            evidence_ids=[],
        )
        
        props = normalize_legislature_record(legislature)
        
        assert props["start_date"] is None
        assert props["start_date_precision"] == "year"
        assert props["start_date_raw"] == "2013"
        assert props["start_date_source"] == "https://example.com"


class TestNormalizeMandateRecord:
    def test_mandate_with_day_precision(self):
        mandate = MockMandate(
            id="test_mandate",
            person_id="person-123",
            parliament_id="NI",
            legislature_id="legislature-123",
            party_code="SPD",
            start_date="2013-02-20",
            start_date_raw="20. Februar 2013",
            start_date_precision="day",
            start_date_source="https://example.com",
            evidence_ids=["evidence-123"],
        )
        
        props = normalize_mandate_record(mandate)
        
        assert props["start_date"] == "2013-02-20"
        assert props["start_date_precision"] == "day"
        assert props["start_date_source"] == "https://example.com"
        assert props["evidence_ids"] == ["evidence-123"]
    
    def test_mandate_without_precision(self):
        mandate = MockMandate(
            id="test_mandate",
            person_id="person-123",
            parliament_id="NI",
            legislature_id="legislature-123",
            party_code="SPD",
            start_date=None,
            start_date_raw="2013",
            evidence_ids=[],
        )
        
        props = normalize_mandate_record(mandate)
        
        assert props["start_date"] is None
        assert props["start_date_precision"] == "unknown"
        assert props["start_date_raw"] == "2013"
