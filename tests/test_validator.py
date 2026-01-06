from datetime import date

import pytest

from scraper.models.domain import Mandate, Party
from scraper.validation.validator import DataValidator, ValidationResult


class TestDataValidator:
    def test_missing_start_date(self):
        validator = DataValidator()
        result = ValidationResult()
        
        mandate = Mandate(
            id="test-1",
            person_id="person-1",
            parliament_id="parliament-1",
            legislature_id="legislature-1",
            start_date=None,
            end_date="2020-12-31",
        )
        
        validator.validate_mandate(mandate, result)
        
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

