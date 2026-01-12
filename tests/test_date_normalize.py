"""Tests for date normalization utilities."""

import pytest
from datetime import date, datetime

from scraper.utils.date_normalize import normalize_date, should_store_raw_value, INVALID_DATE_STRINGS


def test_normalize_date_none():
    assert normalize_date(None) is None


def test_normalize_date_empty_string():
    assert normalize_date("") is None
    assert normalize_date("   ") is None


def test_normalize_date_invalid_strings():
    for invalid in INVALID_DATE_STRINGS:
        assert normalize_date(invalid) is None
        assert normalize_date(invalid.upper()) is None
        assert normalize_date(f"  {invalid}  ") is None


def test_normalize_date_iso_format():
    assert normalize_date("2022-11-08") == "2022-11-08"
    assert normalize_date("  2022-11-08  ") == "2022-11-08"


def test_normalize_date_dd_mm_yyyy():
    assert normalize_date("08.11.2022") == "2022-11-08"
    assert normalize_date("8.11.2022") == "2022-11-08"
    assert normalize_date("08.1.2022") == "2022-01-08"


def test_normalize_date_datetime_object():
    d = date(2022, 11, 8)
    assert normalize_date(d) == "2022-11-08"
    
    dt = datetime(2022, 11, 8, 12, 30, 45)
    assert normalize_date(dt) == "2022-11-08"


def test_normalize_date_iso_datetime_string():
    assert normalize_date("2022-11-08T12:30:45") == "2022-11-08"
    assert normalize_date("2022-11-08T12:30:45Z") == "2022-11-08"
    assert normalize_date("2022-11-08T12:30:45+00:00") == "2022-11-08"


def test_normalize_date_invalid_formats():
    assert normalize_date("2022") is None
    assert normalize_date("11-08") is None
    assert normalize_date("abc") is None
    assert normalize_date("2022/11/08") is None


def test_normalize_date_em_dash():
    assert normalize_date("—") is None
    assert normalize_date("–") is None
    assert normalize_date("  —  ") is None


def test_should_store_raw_value_empty():
    assert should_store_raw_value("", None) is False
    assert should_store_raw_value(None, None) is False
    assert should_store_raw_value("   ", None) is False


def test_should_store_raw_value_invalid_strings():
    for invalid in INVALID_DATE_STRINGS:
        assert should_store_raw_value(invalid, None) is False


def test_should_store_raw_value_when_normalized():
    assert should_store_raw_value("2022-11-08", "2022-11-08") is False
    assert should_store_raw_value("08.11.2022", "2022-11-08") is False


def test_should_store_raw_value_when_normalization_failed():
    assert should_store_raw_value("2022", None) is True
    assert should_store_raw_value("some weird format", None) is True
    assert should_store_raw_value("2022/11/08", None) is True


