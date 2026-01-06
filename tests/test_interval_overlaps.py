from datetime import date

import pytest

from scraper.utils.intervals import filter_mandates_by_overlap, interval_overlaps, parse_date_iso


class TestIntervalOverlaps:
    def test_overlapping_intervals(self):
        assert interval_overlaps(
            date(2020, 1, 1), date(2020, 12, 31),
            date(2020, 6, 1), date(2020, 6, 30)
        ) is True
    
    def test_non_overlapping_intervals(self):
        assert interval_overlaps(
            date(2020, 1, 1), date(2020, 6, 1),
            date(2020, 6, 2), date(2020, 12, 31)
        ) is False
    
    def test_open_ended_a(self):
        assert interval_overlaps(
            date(2020, 1, 1), None,
            date(2020, 6, 1), date(2020, 12, 31)
        ) is True
    
    def test_open_ended_b(self):
        assert interval_overlaps(
            date(2020, 1, 1), date(2020, 6, 1),
            date(2020, 6, 1), None
        ) is True
    
    def test_both_open_ended(self):
        assert interval_overlaps(
            date(2020, 1, 1), None,
            date(2020, 6, 1), None
        ) is True
    
    def test_touching_intervals(self):
        assert interval_overlaps(
            date(2020, 1, 1), date(2020, 6, 1),
            date(2020, 6, 1), date(2020, 12, 31)
        ) is True
    
    def test_contained_interval(self):
        assert interval_overlaps(
            date(2020, 1, 1), date(2020, 12, 31),
            date(2020, 6, 1), date(2020, 6, 30)
        ) is True


class TestParseDateIso:
    def test_valid_date(self):
        assert parse_date_iso("2020-01-01") == date(2020, 1, 1)
    
    def test_none(self):
        assert parse_date_iso(None) is None
    
    def test_invalid_format(self):
        assert parse_date_iso("invalid") is None


class TestFilterMandatesByOverlap:
    def test_filter_by_date_range(self):
        class MockMandate:
            def __init__(self, start_date, end_date):
                self.start_date = start_date
                self.end_date = end_date
        
        mandates = [
            MockMandate("2020-01-01", "2020-06-30"),
            MockMandate("2020-07-01", "2020-12-31"),
            MockMandate("2020-03-01", "2020-04-30"),
        ]
        
        filtered = filter_mandates_by_overlap(
            mandates,
            from_date=date(2020, 3, 1),
            to_date=date(2020, 4, 30),
        )
        
        assert len(filtered) == 2
        assert filtered[0].start_date == "2020-01-01"
        assert filtered[1].start_date == "2020-03-01"
    
    def test_filter_open_ended(self):
        class MockMandate:
            def __init__(self, start_date, end_date):
                self.start_date = start_date
                self.end_date = end_date
        
        mandates = [
            MockMandate("2020-01-01", None),
            MockMandate("2020-07-01", "2020-12-31"),
        ]
        
        filtered = filter_mandates_by_overlap(
            mandates,
            from_date=date(2020, 6, 1),
            to_date=date(2020, 12, 31),
        )
        
        assert len(filtered) == 2

