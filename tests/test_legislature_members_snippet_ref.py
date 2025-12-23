import json
from pathlib import Path

import pytest

from scraper.parsers.legislature_members import parse_legislature_members
from scraper.cache.mediawiki_cache import get_cached_parse_response


def test_parse_legislature_members_creates_evidence_refs():
    """Test that parsing legislature members creates EvidenceRefs on Mandate (not Person)."""
    response = get_cached_parse_response("nds_lt_17")
    
    if not response:
        pytest.skip("No cached response for nds_lt_17")
    
    legislature_data = parse_legislature_members(response, "nds_lt_17")
    
    assert legislature_data.members
    assert len(legislature_data.members) > 0
    
    member_list_evidence_id = legislature_data.evidence_id
    
    # Check that each mandate has evidence_refs with membership_row purpose
    for person, mandate in legislature_data.members:
        assert person.evidence_ids
        assert member_list_evidence_id in person.evidence_ids
        
        # Mandate should have evidence_refs (preferred, not Person)
        assert mandate.evidence_refs, f"Mandate for {person.name} missing evidence_refs"
        
        # Find membership_row EvidenceRef
        membership_ref = None
        for ref in mandate.evidence_refs:
            if ref.purpose == "membership_row" and ref.evidence_id == member_list_evidence_id:
                membership_ref = ref
                break
        
        assert membership_ref is not None, f"Mandate for {person.name} missing membership_row EvidenceRef"
        assert membership_ref.snippet_ref is not None
        assert membership_ref.snippet_ref.get("type") == "table_row"
        assert membership_ref.snippet_ref.get("table_index") is not None
        assert membership_ref.snippet_ref.get("row_index") is not None
        assert membership_ref.snippet_ref.get("row_index") >= 0
        assert membership_ref.snippet_ref.get("table_index") >= 0
        assert membership_ref.snippet_ref.get("version") == 1


def test_evidence_refs_consistency():
    """Test that EvidenceRefs are consistent across runs."""
    response = get_cached_parse_response("nds_lt_17")
    
    if not response:
        pytest.skip("No cached response for nds_lt_17")
    
    # Parse twice
    data1 = parse_legislature_members(response, "nds_lt_17")
    data2 = parse_legislature_members(response, "nds_lt_17")
    
    # Find Stephan Weil in both
    mandate1 = None
    mandate2 = None
    
    for person, mandate in data1.members:
        if "Weil" in person.name:
            mandate1 = mandate
            break
    
    for person, mandate in data2.members:
        if "Weil" in person.name:
            mandate2 = mandate
            break
    
    if mandate1 and mandate2:
        # EvidenceRefs should be identical
        ref1 = None
        ref2 = None
        
        for ref in mandate1.evidence_refs:
            if ref.purpose == "membership_row":
                ref1 = ref
                break
        
        for ref in mandate2.evidence_refs:
            if ref.purpose == "membership_row":
                ref2 = ref
                break
        
        assert ref1 is not None and ref2 is not None
        assert ref1.evidence_id == ref2.evidence_id
        assert ref1.purpose == ref2.purpose
        assert ref1.snippet_ref == ref2.snippet_ref, "EvidenceRef snippet_refs should be deterministic"

