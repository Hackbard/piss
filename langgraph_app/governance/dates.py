"""Governance layer for date fields - Know or NULL policy."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from neo4j import Transaction


class DatePrecision(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"
    UNKNOWN = "unknown"
    NULL = "null"


@dataclass
class GovernedDate:
    iso_day: str | None
    precision: DatePrecision
    raw: str | None
    source_kind: str
    source_url: str
    evidence_urls: list[str]
    method: str
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.precision == DatePrecision.DAY:
            if not self.iso_day:
                raise ValueError("iso_day must be set when precision is DAY")
            if not _is_iso_day(self.iso_day):
                raise ValueError(f"iso_day must be ISO format YYYY-MM-DD, got {self.iso_day!r}")
            if not self.evidence_urls:
                raise ValueError("evidence_urls must not be empty when precision is DAY")
            if not self.source_url:
                raise ValueError("source_url must be set when precision is DAY")
            if self.source_url not in self.evidence_urls:
                raise ValueError("source_url must be included in evidence_urls")


@dataclass
class ApplyResult:
    applied: bool
    canonical_written: bool
    conflict_detected: bool
    previous_canonical: str | None = None
    audit_event_id: str | None = None


_ISO_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_iso_day(value: str | None) -> bool:
    return bool(value and _ISO_DAY_RE.match(value))


def _dedup_evidence_urls(urls: list[str]) -> list[str]:
    seen = set()
    result = []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            result.append(url)
    return result


def _warn_pinned_url(source_url: str, source_kind: str) -> None:
    if source_kind == "wikipedia" and "oldid=" not in source_url:
        import warnings
        warnings.warn(
            f"Wikipedia source_url should include oldid= for reproducibility: {source_url}",
            UserWarning,
            stacklevel=3,
        )
    elif source_kind == "wikidata" and "revision=" not in source_url:
        import warnings
        warnings.warn(
            f"Wikidata source_url should include revision= for reproducibility: {source_url}",
            UserWarning,
            stacklevel=3,
        )


def apply_governed_date(
    tx: Transaction,
    node_label: str,
    node_id: str,
    field: str,
    governed_date: GovernedDate,
    actor: str,
    allow_force: bool = False,
) -> ApplyResult:
    """
    Apply a governed date to a Neo4j node with full provenance and conflict handling.

    Rules:
    - precision != DAY: do not write canonical <field>, write raw/precision/provenance
    - precision == DAY: require evidence_urls not empty, require source_url present
    - conflict handling: if existing canonical day differs:
      - allow_force false: set conflict flag, write AuditEvent, do not overwrite
      - allow_force true: overwrite canonical, write AuditEvent with previous value
    - Always: dedup evidence_urls, set *_set_at, *_set_by, create AuditEvent

    Args:
        tx: Neo4j transaction
        node_label: Label of the node (e.g., "Legislature", "Mandate")
        node_id: ID property value of the node
        field: Field name (e.g., "start_date", "end_date")
        governed_date: GovernedDate instance
        actor: Actor string (e.g., "cli:ingest-official-terms")
        allow_force: If True, allow overwriting conflicting canonical dates

    Returns:
        ApplyResult with details about what was applied
    """
    if governed_date.precision == DatePrecision.DAY:
        _warn_pinned_url(governed_date.source_url, governed_date.source_kind)

    evidence_urls = _dedup_evidence_urls(governed_date.evidence_urls)

    result = tx.run(
        f"""
        MATCH (n:{node_label} {{id: $node_id}})
        RETURN n.{field} AS current_canonical,
               n.{field}_precision AS current_precision,
               n.{field}_conflict AS current_conflict
        """,
        node_id=node_id,
    ).single()

    current_canonical = result.get("current_canonical") if result else None
    current_precision = result.get("current_precision") if result else None
    current_conflict = result.get("current_conflict", False) if result else False

    conflict_detected = False
    previous_canonical = None

    if governed_date.precision == DatePrecision.DAY:
        if current_canonical and current_canonical != governed_date.iso_day:
            conflict_detected = True
            previous_canonical = current_canonical
            if not allow_force:
                tx.run(
                    f"""
                    MATCH (n:{node_label} {{id: $node_id}})
                    SET n.{field}_conflict = true,
                        n.{field}_conflict_with = coalesce(n.{field}_conflict_with, []) + [$conflicting_value]
                    """,
                    node_id=node_id,
                    conflicting_value=governed_date.iso_day,
                )
                canonical_written = False
            else:
                canonical_written = True
        else:
            canonical_written = True
    else:
        canonical_written = False

    set_at = datetime.now(timezone.utc).isoformat()

    if governed_date.precision == DatePrecision.DAY and canonical_written:
        tx.run(
            f"""
            MATCH (n:{node_label} {{id: $node_id}})
            SET n.{field} = $iso_day,
                n.{field}_precision = $precision,
                n.{field}_raw = $raw,
                n.{field}_source_kind = $source_kind,
                n.{field}_source_url = $source_url,
                n.{field}_evidence_urls = $evidence_urls,
                n.{field}_method = $method,
                n.{field}_set_at = $set_at,
                n.{field}_set_by = $actor,
                n.{field}_conflict = false,
                n.{field}_conflict_with = []
            """,
            node_id=node_id,
            iso_day=governed_date.iso_day,
            precision=governed_date.precision.value,
            raw=governed_date.raw,
            source_kind=governed_date.source_kind,
            source_url=governed_date.source_url,
            evidence_urls=evidence_urls,
            method=governed_date.method,
            set_at=set_at,
            actor=actor,
        )
    else:
        tx.run(
            f"""
            MATCH (n:{node_label} {{id: $node_id}})
            SET n.{field}_precision = $precision,
                n.{field}_raw = $raw,
                n.{field}_source_kind = $source_kind,
                n.{field}_source_url = $source_url,
                n.{field}_evidence_urls = $evidence_urls,
                n.{field}_method = $method,
                n.{field}_set_at = $set_at,
                n.{field}_set_by = $actor
            """,
            node_id=node_id,
            precision=governed_date.precision.value,
            raw=governed_date.raw,
            source_kind=governed_date.source_kind,
            source_url=governed_date.source_url,
            evidence_urls=evidence_urls,
            method=governed_date.method,
            set_at=set_at,
            actor=actor,
        )

    audit_event_id = str(uuid4())
    tx.run(
        """
        MATCH (n)
        WHERE n.id = $node_id AND labels(n)[0] = $node_label
        CREATE (e:AuditEvent {
            id: $audit_event_id,
            at: $at,
            actor: $actor,
            action: $action,
            entity_type: $node_label,
            entity_id: $node_id,
            field: $field,
            previous: $previous,
            next: $next,
            source_url: $source_url,
            evidence_urls: $evidence_urls,
            reason: $reason
        })
        CREATE (e)-[:AFFECTS]->(n)
        """,
        node_id=node_id,
        node_label=node_label,
        audit_event_id=audit_event_id,
        at=set_at,
        actor=actor,
        action="set_date" if canonical_written else "set_date_provenance",
        entity_type=node_label,
        entity_id=node_id,
        field=field,
        previous=previous_canonical if conflict_detected else current_canonical,
        next=governed_date.iso_day if governed_date.precision == DatePrecision.DAY else None,
        source_url=governed_date.source_url,
        evidence_urls=evidence_urls,
        reason=governed_date.reason,
    )

    return ApplyResult(
        applied=True,
        canonical_written=canonical_written,
        conflict_detected=conflict_detected,
        previous_canonical=previous_canonical,
        audit_event_id=audit_event_id,
    )
