from datetime import date, datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from scraper.models.domain import Mandate, Party, Person
from scraper.utils.intervals import interval_overlaps, parse_date_iso


class ValidationError(Exception):
    """Base exception for validation errors."""
    pass


ValidationMode = Literal["integrity", "completeness", "all"]


def is_integrity_issue(code: str) -> bool:
    """Check if a validation code represents an integrity issue."""
    integrity_codes = {
        "MANDATE_END_BEFORE_START",
        "MANDATE_INVALID_START_DATE",
        "MANDATE_DUPLICATE",
        "DATE_CANONICAL_WITHOUT_EVIDENCE",
        "DATE_RAW_WITHOUT_PRECISION",
        "DATE_PRECISION_INVALID",
        "DATE_CONFLICT",
    }
    return code in integrity_codes


def is_completeness_issue(code: str) -> bool:
    """Check if a validation code represents a completeness issue."""
    completeness_codes = {
        "MANDATE_MISSING_START_DATE",
        "LEGISLATURE_MISSING_START_DATE",
    }
    return code in completeness_codes


class ValidationResult:
    """Result of a validation run."""
    
    def __init__(self, mode: ValidationMode = "integrity"):
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.mode: ValidationMode = mode
    
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
    
    def classify_by_mode(self) -> None:
        """Reclassify errors and warnings based on validation mode."""
        if self.mode == "all":
            return
        
        integrity_errors = []
        completeness_errors = []
        integrity_warnings = []
        completeness_warnings = []
        other_errors = []
        other_warnings = []
        
        for error in self.errors:
            if is_integrity_issue(error["code"]):
                integrity_errors.append(error)
            elif is_completeness_issue(error["code"]):
                completeness_errors.append(error)
            else:
                other_errors.append(error)
        
        for warning in self.warnings:
            if is_integrity_issue(warning["code"]):
                integrity_warnings.append(warning)
            elif is_completeness_issue(warning["code"]):
                completeness_warnings.append(warning)
            else:
                other_warnings.append(warning)
        
        if self.mode == "integrity":
            self.errors = integrity_errors + other_errors
            self.warnings = completeness_errors + completeness_warnings + integrity_warnings + other_warnings
        elif self.mode == "completeness":
            self.errors = []
            self.warnings = completeness_errors + completeness_warnings + integrity_errors + integrity_warnings + other_errors + other_warnings
    
    def to_dict(self, version: Optional[str] = None) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "meta": {
                "mode": self.mode,
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "version": version,
            },
        }


class DataValidator:
    """Validator for domain entities."""
    
    def __init__(self, strict_mode: bool = False, strict_completeness: bool = False, strict_overlaps: bool = False, mode: ValidationMode = "integrity"):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, missing evidence is treated as ERROR instead of WARN
            strict_completeness: If True, missing start_date is treated as ERROR (completeness gap).
                                If False (default), missing start_date is treated as WARNING.
            strict_overlaps: If True, mandate overlaps with same party are treated as ERROR.
                            If False (default), they are treated as WARNING (requires curation).
            mode: Validation mode - "integrity" (default), "completeness", or "all"
        """
        self.strict_mode = strict_mode
        self.strict_completeness = strict_completeness
        self.strict_overlaps = strict_overlaps
        self.mode = mode
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
                    f"Mandate {mandate.id} is missing start_date (completeness gap). Missing start_date; fixable if Legislature has day start date; otherwise needs official term start source.",
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
                        if self.strict_overlaps:
                            result.add_error(
                                "MANDATE_OVERLAP_SAME_PARTY",
                                f"Mandates {mandate_a.id} and {mandate_b.id} overlap with same party_code ({mandate_a.party_code})",
                                entity_id=mandate_a.id,
                                entity_type="Mandate",
                            )
                        else:
                            result.add_warning(
                                "MANDATE_OVERLAP_SAME_PARTY",
                                f"Mandates {mandate_a.id} and {mandate_b.id} overlap with same party_code ({mandate_a.party_code}) - requires curation",
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
        result = ValidationResult(mode=self.mode)
        
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
        
        result.classify_by_mode()
        return result


    def validate_date_governance_neo4j(
        self,
        session,
        entity_type: str = "Legislature",
        field: str = "start_date",
        result: Optional[ValidationResult] = None,
    ) -> ValidationResult:
        """
        Validate date governance rules in Neo4j.

        Checks for:
        - DATE_CANONICAL_WITHOUT_EVIDENCE: canonical date without evidence_urls
        - DATE_CONFLICT: date conflicts (flagged)
        - DATE_RAW_WITHOUT_PRECISION: raw date without precision
        - DATE_PRECISION_INVALID: invalid precision values

        Args:
            session: Neo4j session
            entity_type: Entity type to validate (Legislature, Mandate, LegislatureTerm)
            field: Field name (start_date, end_date)
            result: Optional existing ValidationResult to add to

        Returns:
            ValidationResult with errors and warnings
        """
        if result is None:
            result = ValidationResult(mode=self.mode)

        rows = session.run(
            f"""
            MATCH (n)
            WHERE ANY(l IN labels(n) WHERE l = $entity_type)
              AND n.{field} IS NOT NULL
            OPTIONAL MATCH (n)-[:SUPPORTED_BY]->(e:Evidence)
            OPTIONAL MATCH (n)-[:SUPPORTED_BY]->(x)
            WHERE x.url IS NOT NULL AND trim(x.url) <> ''
            WITH n,
                 n.id AS entity_id,
                 n.{field} AS canonical_date,
                 n.{field}_precision AS precision,
                 n.{field}_evidence_urls AS evidence_urls,
                 n.{field}_conflict AS conflict,
                 n['evidence_urls'] AS legacy_evidence_urls,
                 n['{field}_source'] AS provenance_source,
                 collect(DISTINCT e.url) AS evidence_node_urls,
                 collect(DISTINCT x.url) AS legacy_node_urls
            RETURN entity_id,
                   canonical_date,
                   precision,
                   evidence_urls,
                   conflict,
                   legacy_evidence_urls,
                   provenance_source,
                   evidence_node_urls,
                   legacy_node_urls
            """,
            entity_type=entity_type,
        ).data()

        for row in rows:
            entity_id = row.get("entity_id")
            evidence_urls = row.get("evidence_urls") or []
            legacy_evidence_urls = row.get("legacy_evidence_urls")
            provenance_source = row.get("provenance_source")
            evidence_node_urls = [url for url in (row.get("evidence_node_urls") or []) if url]
            legacy_node_urls = [url for url in (row.get("legacy_node_urls") or []) if url]

            has_legacy_evidence_urls = False
            if legacy_evidence_urls:
                if isinstance(legacy_evidence_urls, list):
                    has_legacy_evidence_urls = len([u for u in legacy_evidence_urls if u]) > 0
                else:
                    has_legacy_evidence_urls = str(legacy_evidence_urls).strip() != ""

            has_evidence = (
                len(evidence_urls) > 0
                or has_legacy_evidence_urls
                or (provenance_source and str(provenance_source).strip() != "")
                or len(evidence_node_urls) > 0
                or len(legacy_node_urls) > 0
            )

            if not has_evidence:
                result.add_error(
                    "DATE_CANONICAL_WITHOUT_EVIDENCE",
                    f"{entity_type} {entity_id} has canonical {field} but no evidence (checked: evidence_urls, legacy evidence_urls, {field}_source, SUPPORTED_BY relationships)",
                    entity_id=entity_id,
                    entity_type=entity_type,
                )

            if row.get("conflict") is True:
                result.add_error(
                    "DATE_CONFLICT",
                    f"{entity_type} {entity_id} has a date conflict on {field}",
                    entity_id=entity_id,
                    entity_type=entity_type,
                )

        rows_raw = session.run(
            f"""
            MATCH (n)
            WHERE ANY(l IN labels(n) WHERE l = $entity_type)
              AND n.{field}_raw IS NOT NULL
            RETURN n.id AS entity_id,
                   n.{field}_raw AS raw_date,
                   n.{field}_precision AS precision
            """,
            entity_type=entity_type,
        ).data()

        for row in rows_raw:
            entity_id = row.get("entity_id")
            precision = row.get("precision")

            if not precision or precision not in ["day", "month", "year", "unknown", "null"]:
                result.add_error(
                    "DATE_RAW_WITHOUT_PRECISION",
                    f"{entity_type} {entity_id} has {field}_raw but invalid or missing precision: {precision}",
                    entity_id=entity_id,
                    entity_type=entity_type,
                )

        result.classify_by_mode()
        return result
