from datetime import date

import pytest

from scraper.models.query import (
    MandateQueryFilter,
    MandateRow,
    PersonDTO,
    SortDirection,
    SortField,
)


class TestMandateQueryFilter:
    def test_default_values(self):
        filter_obj = MandateQueryFilter()
        assert filter_obj.limit == 200
        assert filter_obj.offset == 0
        assert filter_obj.sort == SortField.PERSON_NAME
        assert filter_obj.sort_direction == SortDirection.ASC

    def test_limit_clamping(self):
        filter_obj = MandateQueryFilter(limit=2000)
        assert filter_obj.limit == 1000

    def test_date_parsing(self):
        filter_obj = MandateQueryFilter(from_date="2020-01-01", to_date="2020-12-31")
        assert filter_obj.from_date == date(2020, 1, 1)
        assert filter_obj.to_date == date(2020, 12, 31)

    def test_date_validation_fails_when_to_before_from(self):
        with pytest.raises(ValueError, match="to_date must be >= from_date"):
            MandateQueryFilter(from_date="2020-12-31", to_date="2020-01-01")

    def test_date_validation_passes_when_equal(self):
        filter_obj = MandateQueryFilter(from_date="2020-01-01", to_date="2020-01-01")
        assert filter_obj.from_date == filter_obj.to_date

    def test_date_parsing_from_date_object(self):
        filter_obj = MandateQueryFilter(from_date=date(2020, 1, 1))
        assert filter_obj.from_date == date(2020, 1, 1)


class TestMandateRow:
    def test_evidence_urls_normalization(self):
        row = MandateRow(
            person_id="p1",
            person_name="Test Person",
            mandate_id="m1",
            legislature_id="l1",
            parliament_id="par1",
            start_date=date(2020, 1, 1),
            evidence_urls=["url1", "url2", "url1", None, ""],
        )
        assert row.evidence_urls == ["url1", "url2"]

    def test_evidence_urls_empty_list(self):
        row = MandateRow(
            person_id="p1",
            person_name="Test Person",
            mandate_id="m1",
            legislature_id="l1",
            parliament_id="par1",
            start_date=date(2020, 1, 1),
            evidence_urls=None,
        )
        assert row.evidence_urls == []


class TestPersonDTO:
    def test_evidence_urls_normalization(self):
        person = PersonDTO(
            person_id="p1",
            name="Test Person",
            evidence_urls=["url1", "url2", "url1"],
        )
        assert person.evidence_urls == ["url1", "url2"]

