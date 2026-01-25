"""Tests for term start curation queue export and apply."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml

from langgraph_app.curation.term_starts import (
    apply_term_start_overrides,
    export_term_start_curation_queue,
)


class TestExportTermStartCurationQueue:
    """Test export-term-start-curation-queue functionality."""
    
    def test_export_yaml_structure(self, tmp_path):
        """Test that export creates valid YAML with expected structure."""
        output_path = tmp_path / "test_queue.yaml"
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_result = Mock()
            mock_result.data.return_value = [
                {
                    "legislature_id": "leg-123",
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_name": "10. Landtag Hamburg",
                    "wikipedia_title": "Liste_der_Mitglieder_des_Hamburgischen_Landtages_(10._Wahlperiode)",
                    "mandates_missing_start_count": 256,
                    "evidence_urls": [
                        "https://de.wikipedia.org/w/index.php?title=...&oldid=123456",
                    ],
                }
            ]
            mock_session.run.return_value = mock_result
            
            result = export_term_start_curation_queue(
                output_path=output_path,
                min_mandates=1,
                top=200,
                parliament_ids=None,
            )
            
            assert "generated_at" in result
            assert result["policy"] == "Know or NULL (day + evidence only)"
            assert len(result["terms"]) == 1
            
            term = result["terms"][0]
            assert term["parliament_id"] == "HH"
            assert term["term_number"] == 10
            assert term["legislature_id"] == "leg-123"
            assert term["mandates_missing_start_count"] == 256
            assert term["start_date_day"] is None
            assert term["source_url"] is None
            assert term["evidence_urls"] == []
            assert len(term["source_candidates"]) > 0
            
            assert output_path.exists()
            with open(output_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                assert loaded["policy"] == "Know or NULL (day + evidence only)"
    
    def test_export_filters_by_min_mandates(self, tmp_path):
        """Test that export respects min_mandates filter."""
        output_path = tmp_path / "test_queue.yaml"
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_result2 = Mock()
            mock_result2.data.return_value = [
                {
                    "legislature_id": "leg-1",
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_name": "10. Landtag Hamburg",
                    "wikipedia_title": "",
                    "mandates_missing_start_count": 5,
                    "evidence_urls": [],
                },
            ]
            mock_session.run.return_value = mock_result2
            
            result = export_term_start_curation_queue(
                output_path=output_path,
                min_mandates=5,
                top=200,
                parliament_ids=None,
            )
            
            assert len(result["terms"]) == 1
            assert result["terms"][0]["mandates_missing_start_count"] == 5


class TestApplyTermStartOverrides:
    """Test apply-term-start-overrides functionality."""
    
    def test_apply_valid_yaml(self, tmp_path):
        """Test applying valid YAML with day dates and evidence."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source", "https://example.com/evidence"],
                    "notes": "Test entry",
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_apply_result = Mock()
            mock_apply_result.canonical_written = True
            mock_session.execute_write.return_value = mock_apply_result
            mock_session.run.return_value = Mock()
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 1
            assert result["skipped_terms"] == 0
            assert len(result["errors"]) == 0
            assert "HH:10" in result["applied_term_identifiers"]
    
    def test_apply_rejects_non_day_dates(self, tmp_path):
        """Test that non-day dates are rejected."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": "2020-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "",
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "YYYY-MM-DD" in result["errors"][0]["error"]
    
    def test_apply_rejects_missing_source_url(self, tmp_path):
        """Test that missing source_url is rejected."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "",
                    "evidence_urls": [],
                    "notes": "",
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "source_url is required" in result["errors"][0]["error"]
    
    def test_apply_skips_null_start_date_day(self, tmp_path):
        """Test that terms with null start_date_day are skipped."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": None,
                    "source_url": None,
                    "evidence_urls": [],
                    "notes": "",
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert result["skipped_terms"] == 1
            assert len(result["errors"]) == 0
    
    def test_apply_dry_run_no_writes(self, tmp_path):
        """Test that dry-run produces no writes."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "",
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=True,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 1
            assert result["skipped_terms"] == 0
            
            mock_session.execute_write.assert_not_called()
            mock_session.run.assert_not_called()
    
    def test_apply_filters_by_parliament_ids(self, tmp_path):
        """Test that only-parliament-ids filter works."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "",
                },
                {
                    "parliament_id": "NI",
                    "term_number": 17,
                    "legislature_id": "leg-456",
                    "legislature_name": "17. Landtag Niedersachsen",
                    "mandates_missing_start_count": 128,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "",
                },
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_apply_result = Mock()
            mock_apply_result.canonical_written = True
            mock_session.execute_write.return_value = mock_apply_result
            mock_session.run.return_value = Mock()
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=["HH"],
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 2
            assert result["applied_terms"] == 1
            assert result["skipped_terms"] == 1
            assert "HH:10" in result["applied_term_identifiers"]
            assert "NI:17" not in result["applied_term_identifiers"]
    
    def test_apply_filters_by_only_term(self, tmp_path):
        """Test that only-term filter works."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "",
                },
                {
                    "parliament_id": "HH",
                    "term_number": 11,
                    "legislature_id": "leg-456",
                    "legislature_name": "11. Landtag Hamburg",
                    "mandates_missing_start_count": 128,
                    "source_candidates": [],
                    "start_date_day": "2020-01-01",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "",
                },
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_apply_result = Mock()
            mock_apply_result.canonical_written = True
            mock_session.execute_write.return_value = mock_apply_result
            mock_session.run.return_value = Mock()
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term="HH:10",
                strict=True,
            )
            
            assert result["processed_terms"] == 2
            assert result["applied_terms"] == 1
            assert result["skipped_terms"] == 1
            assert "HH:10" in result["applied_term_identifiers"]
            assert "HH:11" not in result["applied_term_identifiers"]
    
    def test_apply_month_precision_override(self, tmp_path):
        """Test applying month precision override (no canonical date set)."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "legislature_name": "10. Landtag Hamburg",
                    "mandates_missing_start_count": 256,
                    "source_candidates": [],
                    "start_date_precision": "month",
                    "start_date_raw": "1982-06",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source", "https://example.com/evidence"],
                    "notes": "Konstituierung: Ende Juni 1982 (kein Tag belegbar).",
                    "day_unresolvable": True,
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_apply_result = Mock()
            mock_apply_result.applied = True
            mock_apply_result.canonical_written = False
            mock_session.execute_write.return_value = mock_apply_result
            mock_session.run.return_value = Mock()
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 1
            assert result["applied_day_terms"] == 0
            assert result["applied_nonday_terms"] == 1
            assert result["skipped_terms"] == 0
            assert len(result["errors"]) == 0
            assert "HH:10" in result["applied_term_identifiers"]
            assert len(result["term_results"]) == 1
            assert result["term_results"][0]["precision"] == "month"
            assert result["term_results"][0]["raw"] == "1982-06"
            assert result["term_results"][0]["day_unresolvable"] is True
    
    def test_apply_year_precision_override(self, tmp_path):
        """Test applying year precision override (no canonical date set)."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 12,
                    "legislature_id": "leg-456",
                    "legislature_name": "12. Landtag Hamburg",
                    "mandates_missing_start_count": 128,
                    "source_candidates": [],
                    "start_date_precision": "year",
                    "start_date_raw": "1982",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "notes": "Only year known",
                    "day_unresolvable": False,
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_apply_result = Mock()
            mock_apply_result.applied = True
            mock_apply_result.canonical_written = False
            mock_session.execute_write.return_value = mock_apply_result
            mock_session.run.return_value = Mock()
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 1
            assert result["applied_day_terms"] == 0
            assert result["applied_nonday_terms"] == 1
            assert result["term_results"][0]["precision"] == "year"
            assert result["term_results"][0]["raw"] == "1982"
            assert result["term_results"][0]["day_unresolvable"] is False
    
    def test_apply_rejects_both_day_and_precision(self, tmp_path):
        """Test that specifying both start_date_day and start_date_precision is rejected."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "start_date_day": "1982-06-29",
                    "start_date_precision": "month",
                    "start_date_raw": "1982-06",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "both start_date_day and start_date_precision" in result["errors"][0]["error"]
    
    def test_apply_rejects_day_unresolvable_with_day_precision(self, tmp_path):
        """Test that day_unresolvable=true is rejected when start_date_precision=day."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "start_date_day": "1982-06-29",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                    "day_unresolvable": True,
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "day_unresolvable=true is only allowed" in result["errors"][0]["error"]
    
    def test_apply_rejects_invalid_month_format(self, tmp_path):
        """Test that invalid month format is rejected."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "start_date_precision": "month",
                    "start_date_raw": "1982-6",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "YYYY-MM format" in result["errors"][0]["error"]
    
    def test_apply_rejects_invalid_year_format(self, tmp_path):
        """Test that invalid year format is rejected."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "start_date_precision": "year",
                    "start_date_raw": "82",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "YYYY format" in result["errors"][0]["error"]
    
    def test_apply_rejects_invalid_precision_value(self, tmp_path):
        """Test that invalid precision value is rejected."""
        input_path = tmp_path / "test_input.yaml"
        
        yaml_data = {
            "generated_at": "2026-01-22T00:00:00Z",
            "policy": "Know or NULL (day + evidence only)",
            "terms": [
                {
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_id": "leg-123",
                    "start_date_precision": "week",
                    "start_date_raw": "1982-06",
                    "source_url": "https://example.com/source",
                    "evidence_urls": ["https://example.com/source"],
                }
            ],
        }
        
        with open(input_path, "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            result = apply_term_start_overrides(
                input_path=input_path,
                dry_run=False,
                only_parliament_ids=None,
                only_term=None,
                strict=True,
            )
            
            assert result["processed_terms"] == 1
            assert result["applied_terms"] == 0
            assert len(result["errors"]) == 1
            assert "must be 'month' or 'year'" in result["errors"][0]["error"]
    
    def test_export_includes_precision_fields(self, tmp_path):
        """Test that export includes new precision fields."""
        output_path = tmp_path / "test_queue.yaml"
        
        with patch("langgraph_app.curation.term_starts.GraphDatabase") as mock_db:
            mock_driver = Mock()
            mock_session = Mock()
            mock_db.driver.return_value = mock_driver
            session_context = MagicMock()
            session_context.__enter__.return_value = mock_session
            session_context.__exit__.return_value = None
            mock_driver.session = Mock(return_value=session_context)
            
            mock_result = Mock()
            mock_result.data.return_value = [
                {
                    "legislature_id": "leg-123",
                    "parliament_id": "HH",
                    "term_number": 10,
                    "legislature_name": "10. Landtag Hamburg",
                    "wikipedia_title": "",
                    "mandates_missing_start_count": 256,
                    "evidence_urls": [],
                }
            ]
            mock_session.run.return_value = mock_result
            
            result = export_term_start_curation_queue(
                output_path=output_path,
                min_mandates=1,
                top=200,
                parliament_ids=None,
            )
            
            term = result["terms"][0]
            assert "start_date_precision" in term
            assert term["start_date_precision"] is None
            assert "start_date_raw" in term
            assert term["start_date_raw"] is None
            assert "day_unresolvable" in term
            assert term["day_unresolvable"] is False
