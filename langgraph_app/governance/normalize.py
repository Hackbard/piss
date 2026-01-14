"""Normalize import records to governance-compatible properties.

This module transforms importer payloads into governance-consistent properties
without requiring DB reads. It enforces the "Know or NULL" policy:
- If precision != day: canonical date field MUST be None/NULL
- If precision == day: canonical date MUST be set with source and evidence
"""

from typing import Any, Dict, List, Optional


def normalize_date_field(
    field_name: str,
    date_iso: Optional[str] = None,
    date_raw: Optional[str] = None,
    date_precision: Optional[str] = None,
    source_url: Optional[str] = None,
    evidence_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Normalize a date field to governance-compatible properties.
    
    Rules:
    - If precision != day:
      - canonical date field MUST be None/NULL
      - write <field>_raw and <field>_precision
      - write <field>_source if available (do not require)
    - If precision == day:
      - canonical date MUST be set
      - <field>_precision='day'
      - <field>_raw can be the same ISO string (ok)
      - <field>_source MUST be set
      - evidence must include the source URL
    
    Args:
        field_name: Field name (e.g., "start_date", "end_date")
        date_iso: ISO date string (YYYY-MM-DD) or None
        date_raw: Raw date string or None
        date_precision: Precision ("day", "month", "year", "unknown", "null") or None
        source_url: Source URL (required if precision == day)
        evidence_urls: List of evidence URLs (should include source_url if precision == day)
    
    Returns:
        Dictionary with normalized properties:
        - <field_name>: canonical date (date object or None)
        - <field_name>_precision: precision string
        - <field_name>_raw: raw date string or None
        - <field_name>_source: source URL or None
    """
    props: Dict[str, Any] = {}
    
    precision = date_precision or "unknown"
    if precision not in ["day", "month", "year", "unknown", "null"]:
        precision = "unknown"
    
    if precision != "day":
        props[f"{field_name}"] = None
        props[f"{field_name}_precision"] = precision
        props[f"{field_name}_raw"] = date_raw if date_raw else None
        if source_url:
            props[f"{field_name}_source"] = source_url
        else:
            props[f"{field_name}_source"] = None
        return props
    
    if not date_iso:
        props[f"{field_name}"] = None
        props[f"{field_name}_precision"] = "unknown"
        props[f"{field_name}_raw"] = date_raw if date_raw else None
        props[f"{field_name}_source"] = source_url if source_url else None
        return props
    
    if not source_url:
        props[f"{field_name}"] = None
        props[f"{field_name}_precision"] = "unknown"
        props[f"{field_name}_raw"] = date_raw if date_raw else date_iso
        props[f"{field_name}_source"] = None
        return props
    
    props[f"{field_name}"] = date_iso
    props[f"{field_name}_precision"] = "day"
    props[f"{field_name}_raw"] = date_raw if date_raw else date_iso
    props[f"{field_name}_source"] = source_url
    
    return props


def normalize_legislature_record(legislature: Any) -> Dict[str, Any]:
    """
    Normalize a Legislature record to governance-compatible properties.
    
    Args:
        legislature: Legislature object with date fields
    
    Returns:
        Dictionary with normalized properties for Neo4j SET clause
    """
    props: Dict[str, Any] = {
        "id": legislature.id,
        "parliament_id": legislature.parliament_id,
        "term_number": getattr(legislature, "term_number", None),
        "name": legislature.name,
        "wikipedia_title": getattr(legislature, "wikipedia_title", None),
    }
    
    source_url = getattr(legislature, "source_url", None)
    evidence_urls = getattr(legislature, "evidence_ids", [])
    
    start_props = normalize_date_field(
        field_name="start_date",
        date_iso=legislature.start_date,
        date_raw=getattr(legislature, "start_date_raw", None),
        date_precision=getattr(legislature, "start_date_precision", None),
        source_url=source_url,
        evidence_urls=evidence_urls,
    )
    props.update(start_props)
    
    end_props = normalize_date_field(
        field_name="end_date",
        date_iso=legislature.end_date,
        date_raw=getattr(legislature, "end_date_raw", None),
        date_precision=getattr(legislature, "end_date_precision", None),
        source_url=source_url,
        evidence_urls=evidence_urls,
    )
    props.update(end_props)
    
    props["evidence_ids"] = evidence_urls
    
    return props


def normalize_mandate_record(mandate: Any) -> Dict[str, Any]:
    """
    Normalize a Mandate record to governance-compatible properties.
    
    Args:
        mandate: Mandate object with date fields
    
    Returns:
        Dictionary with normalized properties for Neo4j SET clause
    """
    props: Dict[str, Any] = {
        "id": mandate.id,
        "person_id": mandate.person_id,
        "parliament_id": mandate.parliament_id,
        "legislature_id": mandate.legislature_id,
        "party_code": mandate.party_code,
        "wahlkreis": getattr(mandate, "wahlkreis", None),
        "role": getattr(mandate, "role", None),
        "notes": getattr(mandate, "notes", None),
    }
    
    source_url = getattr(mandate, "start_date_source", None)
    if not source_url:
        source_url = getattr(mandate, "source_url", None)
    
    evidence_urls = getattr(mandate, "evidence_ids", [])
    if not evidence_urls and hasattr(mandate, "evidence_refs"):
        evidence_urls = [ref.evidence_id for ref in mandate.evidence_refs if hasattr(ref, "evidence_id")]
    
    start_props = normalize_date_field(
        field_name="start_date",
        date_iso=mandate.start_date,
        date_raw=getattr(mandate, "start_date_raw", None),
        date_precision=getattr(mandate, "start_date_precision", None),
        source_url=source_url,
        evidence_urls=evidence_urls,
    )
    props.update(start_props)
    
    end_source_url = getattr(mandate, "end_date_source", None)
    if not end_source_url:
        end_source_url = source_url
    
    end_props = normalize_date_field(
        field_name="end_date",
        date_iso=mandate.end_date,
        date_raw=getattr(mandate, "end_date_raw", None),
        date_precision=getattr(mandate, "end_date_precision", None),
        source_url=end_source_url,
        evidence_urls=evidence_urls,
    )
    props.update(end_props)
    
    props["evidence_ids"] = evidence_urls
    
    return props
