from datetime import date
import json
import sys
from io import StringIO

import pytest

from scraper.models.domain import Mandate, Party
from scraper.validation.validator import (
    DataValidator,
    ValidationResult,
    is_integrity_issue,
    is_completeness_issue,
)


class TestDataValidator:
    def test_missing_start_date(self):
        validator = DataValidator(strict_completeness=True)
        result = ValidationResult(mode="all")
        
        # Create mandate with empty start_date (which should be caught by validator)
        # Note: Mandate model requires start_date as string, so we use empty string
        # The validator checks `if not mandate.start_date:` which catches empty strings
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="",
            end_date="2020-12-31",
        )
        
        validator.validate_mandate(mandate, result)
        result.classify_by_mode()
        
        assert result.has_errors()
        assert any(e["code"] == "MANDATE_MISSING_START_DATE" for e in result.errors)
    
    def test_end_before_start(self):
        validator = DataValidator()
        result = ValidationResult()
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="2020-12-31",
            end_date="2020-01-01",
        )
        
        validator.validate_mandate(mandate, result)
        
        assert result.has_errors()
        assert any(e["code"] == "MANDATE_END_BEFORE_START" for e in result.errors)
    
    def test_unknown_party_code(self):
        validator = DataValidator()
        validator.known_party_codes = {"SPD", "CDU"}
        result = ValidationResult()
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="2020-01-01",
            end_date="2020-12-31",
            party_code="UNKNOWN",
        )
        
        validator.validate_mandate(mandate, result)
        
        assert any(w["code"] == "MANDATE_UNKNOWN_PARTY_CODE" for w in result.warnings)
    
    def test_missing_evidence_warning(self):
        validator = DataValidator(strict_mode=False)
        result = ValidationResult()
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="2020-01-01",
            end_date="2020-12-31",
        )
        
        validator.validate_mandate(mandate, result)
        
        assert any(w["code"] == "MANDATE_MISSING_EVIDENCE" for w in result.warnings)
    
    def test_missing_evidence_error_strict(self):
        validator = DataValidator(strict_mode=True)
        result = ValidationResult()
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="2020-01-01",
            end_date="2020-12-31",
        )
        
        validator.validate_mandate(mandate, result)
        
        assert any(e["code"] == "MANDATE_MISSING_EVIDENCE" for e in result.errors)
    
    def test_duplicate_mandates(self):
        validator = DataValidator()
        result = ValidationResult()
        
        mandates = [
            Mandate(
                id="test-1",
                person_id="person-1",
                parliament_id="parliament-1",
                legislature_id="legislature-1",
                start_date="2020-01-01",
                end_date="2020-12-31",
                party_code="SPD",
            ),
            Mandate(
                id="test-2",
                person_id="person-1",
                parliament_id="parliament-1",
                legislature_id="legislature-1",
                start_date="2020-01-01",
                end_date="2020-12-31",
                party_code="SPD",
            ),
        ]
        
        validator.validate_duplicate_mandates(mandates, result)
        
        assert result.has_errors()
        assert any(e["code"] == "MANDATE_DUPLICATE" for e in result.errors)
    
    def test_overlapping_mandates_same_party(self):
        validator = DataValidator()
        result = ValidationResult()
        
        mandates = [
            Mandate(
                id="test-1",
                person_id="person-1",
                parliament_id="parliament-1",
                legislature_id="legislature-1",
                start_date="2020-01-01",
                end_date="2020-06-30",
                party_code="SPD",
            ),
            Mandate(
                id="test-2",
                person_id="person-1",
                parliament_id="parliament-1",
                legislature_id="legislature-1",
                start_date="2020-06-01",
                end_date="2020-12-31",
                party_code="SPD",
            ),
        ]
        
        validator.validate_mandate_overlaps(mandates, person_id="person-1", result=result)
        
        assert result.has_errors()
        assert any(e["code"] == "MANDATE_OVERLAP_SAME_PARTY" for e in result.errors)
    
    def test_overlapping_mandates_different_party(self):
        validator = DataValidator()
        result = ValidationResult()
        
        mandates = [
            Mandate(
                id="test-1",
                person_id="person-1",
                parliament_id="parliament-1",
                legislature_id="legislature-1",
                start_date="2020-01-01",
                end_date="2020-06-30",
                party_code="SPD",
            ),
            Mandate(
                id="test-2",
                person_id="person-1",
                parliament_id="parliament-1",
                legislature_id="legislature-1",
                start_date="2020-06-01",
                end_date="2020-12-31",
                party_code="CDU",
            ),
        ]
        
        validator.validate_mandate_overlaps(mandates, person_id="person-1", result=result)
        
        assert any(w["code"] == "MANDATE_OVERLAP_DIFFERENT_PARTY" for w in result.warnings)
    
    def test_missing_start_date_warning_in_integrity_mode(self):
        validator = DataValidator(mode="integrity")
        result = ValidationResult(mode="integrity")
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="",
            end_date="2020-12-31",
        )
        
        validator.validate_mandate(mandate, result)
        result.classify_by_mode()
        
        assert not result.has_errors()
        assert any(w["code"] == "MANDATE_MISSING_START_DATE" for w in result.warnings)
    
    def test_missing_start_date_error_in_all_mode(self):
        validator = DataValidator(mode="all", strict_completeness=True)
        result = ValidationResult(mode="all")
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="",
            end_date="2020-12-31",
        )
        
        validator.validate_mandate(mandate, result)
        result.classify_by_mode()
        
        assert result.has_errors()
        assert any(e["code"] == "MANDATE_MISSING_START_DATE" for e in result.errors)
    
    def test_integrity_error_stays_error_in_integrity_mode(self):
        validator = DataValidator(mode="integrity")
        result = ValidationResult(mode="integrity")
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date="2020-12-31",
            end_date="2020-01-01",
        )
        
        validator.validate_mandate(mandate, result)
        result.classify_by_mode()
        
        assert result.has_errors()
        assert any(e["code"] == "MANDATE_END_BEFORE_START" for e in result.errors)
    
    def test_result_to_dict_includes_meta(self):
        result = ValidationResult(mode="integrity")
        result.add_error("MANDATE_END_BEFORE_START", "Test error", "test-1", "Mandate")
        result.add_warning("MANDATE_MISSING_START_DATE", "Test warning", "test-2", "Mandate")
        
        output = result.to_dict(version="1.0.0")
        
        assert output["error_count"] == 1
        assert output["warning_count"] == 1
        assert "meta" in output
        assert output["meta"]["mode"] == "integrity"
        assert output["meta"]["version"] == "1.0.0"
        assert "executed_at" in output["meta"]
    
    def test_is_integrity_issue(self):
        assert is_integrity_issue("MANDATE_END_BEFORE_START") is True
        assert is_integrity_issue("MANDATE_DUPLICATE") is True
        assert is_integrity_issue("DATE_CANONICAL_WITHOUT_EVIDENCE") is True
        assert is_integrity_issue("MANDATE_MISSING_START_DATE") is False
    
    def test_is_completeness_issue(self):
        assert is_completeness_issue("MANDATE_MISSING_START_DATE") is True
        assert is_completeness_issue("LEGISLATURE_MISSING_START_DATE") is True
        assert is_completeness_issue("MANDATE_END_BEFORE_START") is False

