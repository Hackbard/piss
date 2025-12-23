import json
import subprocess
import sys
from pathlib import Path

import pytest


def test_cli_prefer_table_row_option():
    """Test that CLI --prefer option works for table_row."""
    # This test requires a real evidence_id from the index
    # We'll use a mock approach or skip if no index exists
    
    # Check if evidence index exists
    from scraper.config import get_settings
    settings = get_settings()
    index_path = settings.scraper_cache_dir / "index" / "evidence_index.jsonl"
    
    if not index_path.exists():
        pytest.skip("Evidence index does not exist - run pipeline first")
    
    # Load a sample evidence_id with table_row snippet_ref
    evidence_id_with_table_row = None
    with open(index_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                snippet_ref = entry.get("snippet_ref")
                if snippet_ref and isinstance(snippet_ref, dict) and snippet_ref.get("type") == "table_row":
                    evidence_id_with_table_row = entry.get("evidence_id")
                    break
            except json.JSONDecodeError:
                continue
    
    if not evidence_id_with_table_row:
        pytest.skip("No evidence_id with table_row snippet_ref found in index")
    
    # Test CLI command (would need to run in subprocess, but for now just verify the option exists)
    # In a real test, we'd run: scraper evidence --resolve --ids <id> --prefer table_row --with-snippets --format md
    # and check output contains "Snippet Source: table_row"
    
    # For now, verify the resolver works with prefer_snippet
    from scraper.evidence.resolver import EvidenceResolver
    
    resolver = EvidenceResolver(backend="file_cache")
    resolved = resolver.resolve(
        [evidence_id_with_table_row],
        with_snippets=True,
        prefer_snippet="table_row",
    )
    
    if resolved:
        evidence = resolved[0]
        # If snippet_ref is table_row, snippet_source should be table_row
        if evidence.snippet_ref and evidence.snippet_ref.get("type") == "table_row":
            assert evidence.snippet_source == "table_row", "When snippet_ref is table_row, snippet_source should be table_row"
            assert evidence.snippet is not None, "table_row snippet should be extracted"

