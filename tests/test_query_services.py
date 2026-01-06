from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from scraper.config import Settings
from scraper.models.query import MandateQueryFilter, SortDirection, SortField
from scraper.services.neo4j_query import (
    Neo4jLegislatureStatsService,
    Neo4jMandateQueryService,
    Neo4jPersonLookupService,
    QueryExecutionException,
)


class TestNeo4jMandateQueryService:
    def test_normalize_filter_with_only_from_date(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        filter_obj = MandateQueryFilter(from_date=date(2020, 1, 1))
        normalized = service._normalize_filter(filter_obj)
        
        assert normalized.from_date == date(2020, 1, 1)
        assert normalized.to_date == date.today()
        service.close()

    def test_normalize_filter_with_only_to_date(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        filter_obj = MandateQueryFilter(to_date=date(2020, 12, 31))
        normalized = service._normalize_filter(filter_obj)
        
        assert normalized.from_date == date(1900, 1, 1)
        assert normalized.to_date == date(2020, 12, 31)
        service.close()

    def test_normalize_filter_clamps_limit(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        filter_obj = MandateQueryFilter(limit=2000)
        normalized = service._normalize_filter(filter_obj)
        
        assert normalized.limit == 1000
        service.close()

    def test_build_query_with_all_filters(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        filter_obj = MandateQueryFilter(
            parliament_id="par1",
            legislature_id="leg1",
            party_code="SPD",
            person_id="p1",
            person_name_contains="Weil",
            from_date=date(2020, 1, 1),
            to_date=date(2020, 12, 31),
            limit=50,
            offset=10,
            sort=SortField.START_DATE,
            sort_direction=SortDirection.DESC,
        )
        
        query, params = service._build_query(filter_obj)
        
        assert "parliament_id" in params
        assert "legislature_id" in params
        assert "party_code" in params
        assert "person_id" in params
        assert "person_name_contains" in params
        assert "from_date" in params
        assert "to_date" in params
        assert params["limit"] == 50
        assert params["offset"] == 10
        assert "ORDER BY" in query
        assert "DESC" in query
        service.close()

    def test_build_query_overlap_logic(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        filter_obj = MandateQueryFilter(
            from_date=date(2020, 1, 1),
            to_date=date(2020, 12, 31),
        )
        
        query, params = service._build_query(filter_obj)
        
        assert "m.start_date <= $to_date" in query
        assert "(m.end_date IS NULL OR m.end_date >= $from_date)" in query
        assert params["from_date"] == "2020-01-01"
        assert params["to_date"] == "2020-12-31"
        service.close()

    def test_map_record_to_row(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        mock_record = MagicMock()
        mock_record.__getitem__ = lambda self, key: {
            "person_id": "p1",
            "person_name": "Test Person",
            "wikipedia_title": "Test_Person",
            "mandate_id": "m1",
            "legislature_id": "l1",
            "legislature_name": "Test Legislature",
            "parliament_id": "par1",
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "party_code": "SPD",
            "evidence_urls": ["url1", "url2"],
        }.get(key)
        mock_record.get = lambda key, default=None: {
            "person_id": "p1",
            "person_name": "Test Person",
            "wikipedia_title": "Test_Person",
            "mandate_id": "m1",
            "legislature_id": "l1",
            "legislature_name": "Test Legislature",
            "parliament_id": "par1",
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "party_code": "SPD",
            "evidence_urls": ["url1", "url2"],
        }.get(key, default)
        
        row = service._map_record_to_row(mock_record)
        
        assert row.person_id == "p1"
        assert row.person_name == "Test Person"
        assert row.start_date == date(2020, 1, 1)
        assert row.end_date == date(2020, 12, 31)
        assert row.evidence_urls == ["url1", "url2"]
        service.close()

    def test_map_record_to_row_with_null_end_date(self):
        settings = Settings()
        service = Neo4jMandateQueryService(settings)
        
        mock_record = MagicMock()
        mock_record.__getitem__ = lambda self, key: {
            "person_id": "p1",
            "person_name": "Test Person",
            "wikipedia_title": None,
            "mandate_id": "m1",
            "legislature_id": "l1",
            "legislature_name": None,
            "parliament_id": "par1",
            "start_date": "2020-01-01",
            "end_date": None,
            "party_code": None,
            "evidence_urls": [],
        }.get(key)
        mock_record.get = lambda key, default=None: {
            "person_id": "p1",
            "person_name": "Test Person",
            "wikipedia_title": None,
            "mandate_id": "m1",
            "legislature_id": "l1",
            "legislature_name": None,
            "parliament_id": "par1",
            "start_date": "2020-01-01",
            "end_date": None,
            "party_code": None,
            "evidence_urls": [],
        }.get(key, default)
        
        row = service._map_record_to_row(mock_record)
        
        assert row.end_date is None
        assert row.evidence_urls == []
        service.close()


class TestNeo4jPersonLookupService:
    def test_search_by_name_clamps_limit(self):
        settings = Settings()
        service = Neo4jPersonLookupService(settings)
        
        result = service.search_by_name("Test", limit=200)
        
        assert result == []
        service.close()

