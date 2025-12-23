import pytest

from scraper.models.domain import Evidence


def test_evidence_model_no_snippet_ref():
    """Test that Evidence model does not have snippet_ref field (page-level only)."""
    evidence = Evidence(
        id="test-evidence-id",
        endpoint_kind="parse",
        page_title="Test_Page",
        page_id=12345,
        revision_id=67890,
        source_url="https://de.wikipedia.org/wiki/Test_Page",
        retrieved_at="2024-01-01T00:00:00Z",
        sha256="test-sha256",
    )
    
    # Evidence should not have snippet_ref attribute
    assert not hasattr(evidence, "snippet_ref")
    
    # Evidence should serialize without snippet_ref
    evidence_dict = evidence.model_dump()
    assert "snippet_ref" not in evidence_dict


def test_evidence_index_no_snippet_ref():
    """Test that evidence index update does not accept snippet_ref parameter."""
    from scraper.cache.evidence_index import update_evidence_index
    from pathlib import Path
    import tempfile
    import json
    
    with tempfile.TemporaryDirectory() as tmpdir:
        from scraper.config import Settings
        from unittest.mock import patch
        
        # Mock settings to use temp directory
        test_settings = Settings(
            scraper_cache_dir=Path(tmpdir),
            meili_url="http://localhost:7700",
            meili_master_key="test-key",
            neo4j_uri="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="test",
        )
        
        with patch("scraper.cache.evidence_index.settings", test_settings):
            metadata_path = Path(tmpdir) / "metadata.json"
            raw_path = Path(tmpdir) / "raw.json"
            metadata_path.write_text('{"sha256": "test"}')
            raw_path.write_text('{"test": "data"}')
            
            # update_evidence_index should not accept snippet_ref parameter
            # (it's been removed from the signature)
            update_evidence_index(
                evidence_id="test-id",
                source_kind="mediawiki",
                cache_metadata_path=metadata_path,
                cache_raw_path=raw_path,
                page_title="Test_Page",
                page_id=12345,
                revision_id=67890,
                sha256="test-sha256",
            )
            
            # Verify index entry does not contain snippet_ref
            index_path = test_settings.scraper_cache_dir / "index" / "evidence_index.jsonl"
            if index_path.exists():
                with open(index_path, "r", encoding="utf-8") as f:
                    for line in f:
                        entry = json.loads(line.strip())
                        if entry.get("evidence_id") == "test-id":
                            assert "snippet_ref" not in entry or entry.get("snippet_ref") is None

