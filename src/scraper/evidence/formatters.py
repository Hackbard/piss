import json
from typing import List

import yaml

from scraper.evidence.types import ResolvedEvidence


def format_resolved_evidence_json(resolved: List[ResolvedEvidence]) -> str:
    """Format resolved evidence as JSON."""
    return json.dumps(
        [e.model_dump(exclude_none=True) for e in resolved],
        indent=2,
        ensure_ascii=False,
    )


def format_resolved_evidence_yaml(resolved: List[ResolvedEvidence]) -> str:
    """Format resolved evidence as YAML."""
    data = [e.model_dump(exclude_none=True) for e in resolved]
    return yaml.dump(data, allow_unicode=True, default_flow_style=False)


def format_resolved_evidence_markdown(resolved: List[ResolvedEvidence]) -> str:
    """Format resolved evidence as Markdown."""
    lines = []
    
    for e in resolved:
        lines.append(f"- Evidence `{e.evidence_id}`")
        lines.append(f"  - **Source**: {e.source_kind}")
        if e.page_title:
            lines.append(f"  - **Page**: {e.page_title}")
        if e.revision_id:
            lines.append(f"  - **Revision**: {e.revision_id}")
        lines.append(f"  - **URL**: {e.canonical_url}")
        if e.retrieved_at_utc:
            lines.append(f"  - **Retrieved**: {e.retrieved_at_utc}")
        if e.sha256:
            lines.append(f"  - **SHA256**: `{e.sha256[:16]}...`")
        if e.snippet:
            lines.append(f"  - **Snippet**: \"{e.snippet}\"")
        if e.snippet_source:
            lines.append(f"  - **Snippet Source**: {e.snippet_source}")
        lines.append("")
    
    return "\n".join(lines)

