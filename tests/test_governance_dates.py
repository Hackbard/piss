"""Tests for date governance layer."""

import pytest
from datetime import datetime, timezone

from langgraph_app.governance.dates import (
    DatePrecision,
    GovernedDate,
    apply_governed_date,
    ApplyResult,
)


class TestGovernedDate:
    def test_day_precision_requires_iso_day(self):
        with pytest.raises(ValueError, match="iso_day must be set"):
            GovernedDate(
                iso_day=None,
                precision=DatePrecision.DAY,
                raw="2020-01-01",
                source_kind="official",
                source_url="https://example.com",
                evidence_urls=["https://example.com"],
                method="test",
            )

    def test_day_precision_requires_evidence_urls(self):
        with pytest.raises(ValueError, match="evidence_urls must not be empty"):
            GovernedDate(
                iso_day="2020-01-01",
                precision=DatePrecision.DAY,
                raw="2020-01-01",
                source_kind="official",
                source_url="https://example.com",
                evidence_urls=[],
                method="test",
            )

    def test_day_precision_requires_source_url(self):
        with pytest.raises(ValueError, match="source_url must be set"):
            GovernedDate(
                iso_day="2020-01-01",
                precision=DatePrecision.DAY,
                raw="2020-01-01",
                source_kind="official",
                source_url="",
                evidence_urls=["https://example.com"],
                method="test",
            )

    def test_day_precision_requires_source_url_in_evidence_urls(self):
        with pytest.raises(ValueError, match="source_url must be included in evidence_urls"):
            GovernedDate(
                iso_day="2020-01-01",
                precision=DatePrecision.DAY,
                raw="2020-01-01",
                source_kind="official",
                source_url="https://example.com",
                evidence_urls=["https://other.com"],
                method="test",
            )

    def test_non_day_precision_allows_null_iso_day(self):
        date = GovernedDate(
            iso_day=None,
            precision=DatePrecision.MONTH,
            raw="January 2020",
            source_kind="official",
            source_url="https://example.com",
            evidence_urls=["https://example.com"],
            method="test",
        )
        assert date.iso_day is None
        assert date.precision == DatePrecision.MONTH

    def test_invalid_iso_format_raises_error(self):
        with pytest.raises(ValueError, match="iso_day must be ISO format"):
            GovernedDate(
                iso_day="2020/01/01",
                precision=DatePrecision.DAY,
                raw="2020/01/01",
                source_kind="official",
                source_url="https://example.com",
                evidence_urls=["https://example.com"],
                method="test",
            )


class TestApplyGovernedDate:
    def test_apply_day_precision_writes_canonical(self, neo4j_session):
        governed_date = GovernedDate(
            iso_day="2020-01-01",
            precision=DatePrecision.DAY,
            raw="2020-01-01",
            source_kind="official",
            source_url="https://example.com",
            evidence_urls=["https://example.com"],
            method="test",
        )

        neo4j_session.run(
            """
            CREATE (l:Legislature {id: 'test-legislature-1', parliament_id: 'TEST', term_number: 1})
            """
        )

        def apply(tx):
            return apply_governed_date(
                tx,
                "Legislature",
                "test-legislature-1",
                "start_date",
                governed_date,
                "test:apply_day_precision",
                allow_force=False,
            )

        result = neo4j_session.write_transaction(apply)

        assert result.applied is True
        assert result.canonical_written is True
        assert result.conflict_detected is False

        check = neo4j_session.run(
            """
            MATCH (l:Legislature {id: 'test-legislature-1'})
            RETURN l.start_date AS start_date,
                   l.start_date_precision AS precision,
                   l.start_date_source_kind AS source_kind,
                   l.start_date_evidence_urls AS evidence_urls
            """
        ).single()

        assert check["start_date"] == "2020-01-01"
        assert check["precision"] == "day"
        assert check["source_kind"] == "official"
        assert check["evidence_urls"] == ["https://example.com"]

    def test_apply_non_day_precision_does_not_write_canonical(self, neo4j_session):
        governed_date = GovernedDate(
            iso_day=None,
            precision=DatePrecision.MONTH,
            raw="January 2020",
            source_kind="official",
            source_url="https://example.com",
            evidence_urls=["https://example.com"],
            method="test",
        )

        neo4j_session.run(
            """
            CREATE (l:Legislature {id: 'test-legislature-2', parliament_id: 'TEST', term_number: 2})
            """
        )

        def apply(tx):
            return apply_governed_date(
                tx,
                "Legislature",
                "test-legislature-2",
                "start_date",
                governed_date,
                "test:apply_non_day",
                allow_force=False,
            )

        result = neo4j_session.write_transaction(apply)

        assert result.applied is True
        assert result.canonical_written is False

        check = neo4j_session.run(
            """
            MATCH (l:Legislature {id: 'test-legislature-2'})
            RETURN l.start_date AS start_date,
                   l.start_date_precision AS precision,
                   l.start_date_raw AS raw
            """
        ).single()

        assert check["start_date"] is None
        assert check["precision"] == "month"
        assert check["raw"] == "January 2020"

    def test_apply_conflict_without_force_sets_flag(self, neo4j_session):
        neo4j_session.run(
            """
            CREATE (l:Legislature {
                id: 'test-legislature-3',
                parliament_id: 'TEST',
                term_number: 3,
                start_date: '2020-01-01',
                start_date_precision: 'day'
            })
            """
        )

        governed_date = GovernedDate(
            iso_day="2020-01-02",
            precision=DatePrecision.DAY,
            raw="2020-01-02",
            source_kind="official",
            source_url="https://example.com",
            evidence_urls=["https://example.com"],
            method="test",
        )

        def apply(tx):
            return apply_governed_date(
                tx,
                "Legislature",
                "test-legislature-3",
                "start_date",
                governed_date,
                "test:apply_conflict",
                allow_force=False,
            )

        result = neo4j_session.write_transaction(apply)

        assert result.applied is True
        assert result.canonical_written is False
        assert result.conflict_detected is True
        assert result.previous_canonical == "2020-01-01"

        check = neo4j_session.run(
            """
            MATCH (l:Legislature {id: 'test-legislature-3'})
            RETURN l.start_date AS start_date,
                   l.start_date_conflict AS conflict,
                   l.start_date_conflict_with AS conflict_with
            """
        ).single()

        assert check["start_date"] == "2020-01-01"
        assert check["conflict"] is True
        assert "2020-01-02" in check["conflict_with"]

    def test_apply_conflict_with_force_overwrites(self, neo4j_session):
        neo4j_session.run(
            """
            CREATE (l:Legislature {
                id: 'test-legislature-4',
                parliament_id: 'TEST',
                term_number: 4,
                start_date: '2020-01-01',
                start_date_precision: 'day'
            })
            """
        )

        governed_date = GovernedDate(
            iso_day="2020-01-02",
            precision=DatePrecision.DAY,
            raw="2020-01-02",
            source_kind="official",
            source_url="https://example.com",
            evidence_urls=["https://example.com"],
            method="test",
        )

        def apply(tx):
            return apply_governed_date(
                tx,
                "Legislature",
                "test-legislature-4",
                "start_date",
                governed_date,
                "test:apply_conflict_force",
                allow_force=True,
            )

        result = neo4j_session.write_transaction(apply)

        assert result.applied is True
        assert result.canonical_written is True
        assert result.conflict_detected is True
        assert result.previous_canonical == "2020-01-01"

        check = neo4j_session.run(
            """
            MATCH (l:Legislature {id: 'test-legislature-4'})
            RETURN l.start_date AS start_date,
                   l.start_date_conflict AS conflict
            """
        ).single()

        assert check["start_date"] == "2020-01-02"
        assert check["conflict"] is False

    def test_apply_creates_audit_event(self, neo4j_session):
        neo4j_session.run(
            """
            CREATE (l:Legislature {id: 'test-legislature-5', parliament_id: 'TEST', term_number: 5})
            """
        )

        governed_date = GovernedDate(
            iso_day="2020-01-01",
            precision=DatePrecision.DAY,
            raw="2020-01-01",
            source_kind="official",
            source_url="https://example.com",
            evidence_urls=["https://example.com"],
            method="test",
        )

        def apply(tx):
            return apply_governed_date(
                tx,
                "Legislature",
                "test-legislature-5",
                "start_date",
                governed_date,
                "test:apply_audit",
                allow_force=False,
            )

        result = neo4j_session.write_transaction(apply)

        assert result.audit_event_id is not None

        audit = neo4j_session.run(
            """
            MATCH (e:AuditEvent {id: $event_id})-[r:AFFECTS]->(l:Legislature {id: 'test-legislature-5'})
            RETURN e.action AS action,
                   e.actor AS actor,
                   e.field AS field,
                   e.next AS next_date
            """,
            event_id=result.audit_event_id,
        ).single()

        assert audit["action"] == "set_date"
        assert audit["actor"] == "test:apply_audit"
        assert audit["field"] == "start_date"
        assert audit["next_date"] == "2020-01-01"


@pytest.fixture
def neo4j_session():
    from neo4j import GraphDatabase
    from scraper.config import get_settings

    settings = get_settings()
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    session = driver.session()

    session.run("MATCH (n) DETACH DELETE n")

    yield session

    session.run("MATCH (n) DETACH DELETE n")
    session.close()
    driver.close()
