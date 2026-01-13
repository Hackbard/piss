from datetime import date
from typing import Any, Dict, List, Optional

from scraper.models.domain import Mandate, Party, Person
from scraper.utils.intervals import interval_overlaps, parse_date_iso


class ValidationError(Exception):
    """Base exception for validation errors."""
    pass


class ValidationResult:
    """Result of a validation run."""
    
    def __init__(self):
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
    
    def add_error(self, code: str, message: str, entity_id: Optional[str] = None, entity_type: Optional[str] = None) -> None:
        """Add an error to the result."""
        self.errors.append({
            "code": code,
            "message": message,
            "entity_id": entity_id,
            "entity_type": entity_type,
        })
    
    def add_warning(self, code: str, message: str, entity_id: Optional[str] = None, entity_type: Optional[str] = None) -> None:
        """Add a warning to the result."""
        self.warnings.append({
            "code": code,
            "message": message,
            "entity_id": entity_id,
            "entity_type": entity_type,
        })
    
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
        }


class DataValidator:
    """Validator for domain entities."""
    
    def __init__(self, strict_mode: bool = False, strict_completeness: bool = False):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, missing evidence is treated as ERROR instead of WARN
            strict_completeness: If True, missing start_date is treated as ERROR (completeness gap).
                                If False (default), missing start_date is treated as WARNING.
        """
        self.strict_mode = strict_mode
        self.strict_completeness = strict_completeness
        self.known_party_codes: set[str] = set()
    
    def validate_mandate(self, mandate: Mandate, result: ValidationResult) -> None:
        """Validate a single mandate."""
        if not mandate.start_date:
            if self.strict_completeness:
                result.add_error(
                    "MANDATE_MISSING_START_DATE",
                    f"Mandate {mandate.id} is missing required start_date",
                    entity_id=mandate.id,
                    entity_type="Mandate",
                )
            else:
                result.add_warning(
                    "MANDATE_MISSING_START_DATE",
                    f"Mandate {mandate.id} is missing start_date (completeness gap)",
                    entity_id=mandate.id,
                    entity_type="Mandate",
                )
            return
        
        start_date = parse_date_iso(mandate.start_date)
        end_date = parse_date_iso(mandate.end_date) if mandate.end_date else None
        
        if start_date is None:
            result.add_error(
                "MANDATE_INVALID_START_DATE",
                f"Mandate {mandate.id} has invalid start_date format: {mandate.start_date}",
                entity_id=mandate.id,
                entity_type="Mandate",
            )
            return
        
        if end_date is not None and end_date < start_date:
            result.add_error(
                "MANDATE_END_BEFORE_START",
                f"Mandate {mandate.id} has end_date ({mandate.end_date}) before start_date ({mandate.start_date})",
                entity_id=mandate.id,
                entity_type="Mandate",
            )
        
        if mandate.party_code and mandate.party_code not in self.known_party_codes:
            result.add_warning(
                "MANDATE_UNKNOWN_PARTY_CODE",
                f"Mandate {mandate.id} has unknown party_code: {mandate.party_code}",
                entity_id=mandate.id,
                entity_type="Mandate",
            )
        
        if not mandate.evidence_refs and not mandate.evidence_ids:
            if self.strict_mode:
                result.add_error(
                    "MANDATE_MISSING_EVIDENCE",
                    f"Mandate {mandate.id} has no evidence references",
                    entity_id=mandate.id,
                    entity_type="Mandate",
                )
            else:
                result.add_warning(
                    "MANDATE_MISSING_EVIDENCE",
                    f"Mandate {mandate.id} has no evidence references",
                    entity_id=mandate.id,
                    entity_type="Mandate",
                )
    
    def validate_mandate_overlaps(
        self,
        mandates: List[Mandate],
        result: ValidationResult,
        person_id: Optional[str] = None,
        legislature_id: Optional[str] = None,
    ) -> None:
        """
        Check for overlapping mandates for the same person/legislature.
        
        Args:
            mandates: List of mandates to check
            person_id: If provided, only check mandates for this person
            legislature_id: If provided, only check mandates for this legislature
            result: Validation result to add errors/warnings to
        """
        filtered = mandates
        if person_id:
            filtered = [m for m in filtered if m.person_id == person_id]
        if legislature_id:
            filtered = [m for m in filtered if m.legislature_id == legislature_id]
        
        for i, mandate_a in enumerate(filtered):
            if not mandate_a.start_date:
                continue
            
            start_a = parse_date_iso(mandate_a.start_date)
            end_a = parse_date_iso(mandate_a.end_date) if mandate_a.end_date else None
            
            if start_a is None:
                continue
            
            for mandate_b in filtered[i + 1:]:
                if not mandate_b.start_date:
                    continue
                
                if mandate_a.id == mandate_b.id:
                    continue
                
                if mandate_a.person_id != mandate_b.person_id:
                    continue
                
                if mandate_a.legislature_id != mandate_b.legislature_id:
                    continue
                
                start_b = parse_date_iso(mandate_b.start_date)
                end_b = parse_date_iso(mandate_b.end_date) if mandate_b.end_date else None
                
                if start_b is None:
                    continue
                
                if interval_overlaps(start_a, end_a, start_b, end_b):
                    if mandate_a.party_code == mandate_b.party_code:
                        result.add_error(
                            "MANDATE_OVERLAP_SAME_PARTY",
                            f"Mandates {mandate_a.id} and {mandate_b.id} overlap with same party_code ({mandate_a.party_code})",
                            entity_id=mandate_a.id,
                            entity_type="Mandate",
                        )
                    else:
                        result.add_warning(
                            "MANDATE_OVERLAP_DIFFERENT_PARTY",
                            f"Mandates {mandate_a.id} and {mandate_b.id} overlap with different party_codes ({mandate_a.party_code} vs {mandate_b.party_code}) - possible party switch",
                            entity_id=mandate_a.id,
                            entity_type="Mandate",
                        )
    
    def validate_duplicate_mandates(self, mandates: List[Mandate], result: ValidationResult) -> None:
        """
        Check for duplicate mandates based on dedupe key.
        
        Dedupe key: (person_id, legislature_id, start_date, end_date, party_code)
        """
        seen = {}
        for mandate in mandates:
            if not mandate.start_date:
                continue
            
            key = (
                mandate.person_id,
                mandate.legislature_id,
                mandate.start_date,
                mandate.end_date or "",
                mandate.party_code or "",
            )
            
            if key in seen:
                result.add_error(
                    "MANDATE_DUPLICATE",
                    f"Duplicate mandate: {mandate.id} duplicates {seen[key].id}",
                    entity_id=mandate.id,
                    entity_type="Mandate",
                )
            else:
                seen[key] = mandate
    
    def validate_all(
        self,
        mandates: List[Mandate],
        parties: Optional[List[Party]] = None,
        persons: Optional[List[Person]] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        parliament_id: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate all entities.
        
        Args:
            mandates: List of mandates to validate
            parties: List of known parties (for party_code validation)
            persons: List of persons (optional, for future validation)
            from_date: Filter mandates by date range start
            to_date: Filter mandates by date range end
            parliament_id: Filter mandates by parliament_id
        
        Returns:
            ValidationResult with errors and warnings
        """
        result = ValidationResult()
        
        if parties:
            self.known_party_codes = {p.code for p in parties if p.code}
        
        filtered_mandates = mandates
        if from_date or to_date:
            from scraper.utils.intervals import filter_mandates_by_overlap
            filtered_mandates = filter_mandates_by_overlap(mandates, from_date, to_date)
        
        if parliament_id:
            filtered_mandates = [m for m in filtered_mandates if m.parliament_id == parliament_id]
        
        for mandate in filtered_mandates:
            self.validate_mandate(mandate, result)
        
        self.validate_duplicate_mandates(filtered_mandates, result)
        
        person_ids = {m.person_id for m in filtered_mandates}
        for person_id in person_ids:
            person_mandates = [m for m in filtered_mandates if m.person_id == person_id]
            self.validate_mandate_overlaps(person_mandates, result, person_id=person_id)
        
        return result

