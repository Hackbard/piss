"""Tests for mandate date backfill from legislature."""

import pytest
from unittest.mock import Mock, patch

from scraper.models.domain import Mandate, Legislature
from scraper.utils.date_normalize import normalize_date


def test_mandate_normalizes_dates():
    mandate = Mandate(
        id="test-id",
        person_id="person-1",
        parliament_id="NI",
        legislature_id="leg-1",
        start_date="unknown",
        end_date="",
    )
    
    start_normalized = normalize_date(mandate.start_date)
    end_normalized = normalize_date(mandate.end_date)
    
    assert start_normalized is None
    assert end_normalized is None


def test_mandate_stores_raw_when_normalization_fails():
    from scraper.utils.date_normalize import should_store_raw_value
    
    raw_start = "2022"
    normalized_start = normalize_date(raw_start)
    
    assert normalized_start is None
    assert should_store_raw_value(raw_start, normalized_start) is True


def test_backfill_from_legislature_start_date():
    legislature = Legislature(
        id="leg-1",
        parliament_id="NI",
        name="18. Wahlperiode",
        start_date="2017-10-15",
        end_date="2022-11-08",
    )
    
    mandate = Mandate(
        id="mandate-1",
        person_id="person-1",
        parliament_id="NI",
        legislature_id="leg-1",
        start_date=None,
        end_date=None,
    )
    
    if mandate.start_date is None and legislature.start_date:
        mandate.start_date = legislature.start_date
        mandate.start_date_source = "legislature"
    
    assert mandate.start_date == "2017-10-15"
    assert mandate.start_date_source == "legislature"


def test_backfill_from_legislature_end_date():
    legislature = Legislature(
        id="leg-1",
        parliament_id="NI",
        name="18. Wahlperiode",
        start_date="2017-10-15",
        end_date="2022-11-08",
    )
    
    mandate = Mandate(
        id="mandate-1",
        person_id="person-1",
        parliament_id="NI",
        legislature_id="leg-1",
        start_date="2017-10-15",
        end_date=None,
    )
    
    if mandate.end_date is None and legislature.end_date:
        mandate.end_date = legislature.end_date
        mandate.end_date_source = "legislature"
    
    assert mandate.end_date == "2022-11-08"
    assert mandate.end_date_source == "legislature"


def test_backfill_does_not_overwrite_existing_dates():
    legislature = Legislature(
        id="leg-1",
        parliament_id="NI",
        name="18. Wahlperiode",
        start_date="2017-10-15",
        end_date="2022-11-08",
    )
    
    mandate = Mandate(
        id="mandate-1",
        person_id="person-1",
        parliament_id="NI",
        legislature_id="leg-1",
        start_date="2018-01-01",
        end_date="2021-12-31",
    )
    
    original_start = mandate.start_date
    original_end = mandate.end_date
    
    if mandate.start_date is None and legislature.start_date:
        mandate.start_date = legislature.start_date
        mandate.start_date_source = "legislature"
    
    if mandate.end_date is None and legislature.end_date:
        mandate.end_date = legislature.end_date
        mandate.end_date_source = "legislature"
    
    assert mandate.start_date == original_start
    assert mandate.end_date == original_end
    assert mandate.start_date_source is None
    assert mandate.end_date_source is None


