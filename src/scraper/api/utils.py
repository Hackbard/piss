import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from scraper.api.models import ToolMeta


def get_data_version() -> str:
    """Get data version (git sha or import run id)."""
    git_dir = Path(".git")
    if git_dir.exists():
        try:
            head_file = git_dir / "HEAD"
            if head_file.exists():
                head_content = head_file.read_text().strip()
                if head_content.startswith("ref: "):
                    ref_path = git_dir / head_content[5:]
                    if ref_path.exists():
                        return f"git:{ref_path.read_text().strip()[:7]}"
        except Exception:
            pass
    
    return "unknown"


def compute_result_hash(data: dict) -> str:
    """Compute SHA256 hash of canonical JSON (without meta.executed_at)."""
    data_copy = data.copy()
    if "meta" in data_copy and isinstance(data_copy["meta"], dict):
        meta_copy = data_copy["meta"].copy()
        meta_copy.pop("executed_at", None)
        data_copy["meta"] = meta_copy
    
    canonical_json = json.dumps(data_copy, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode()).hexdigest()


def create_tool_meta(tool_name: str, request_id: Optional[UUID] = None) -> ToolMeta:
    """Create ToolMeta with generated fields."""
    if request_id is None:
        request_id = uuid4()
    
    return ToolMeta(
        tool=tool_name,
        executed_at=datetime.now(timezone.utc),
        request_id=request_id,
        data_version=get_data_version(),
    )


def validate_evidence_strict(rows: list, strict: bool, tool_name: str) -> list[str]:
    """
    Validate evidence URLs in rows.
    
    Returns list of warnings/errors.
    If strict=True and any row has empty evidence_urls, raises ValueError.
    """
    warnings = []
    
    if strict:
        missing_evidence = []
        for i, row in enumerate(rows):
            evidence_urls = getattr(row, "evidence_urls", []) or []
            if not evidence_urls:
                row_id = getattr(row, "mandate_id", None) or getattr(row, "person_id", None) or f"row_{i}"
                missing_evidence.append(row_id)
        
        if missing_evidence:
            raise ValueError(
                f"EVIDENCE_MISSING: {len(missing_evidence)} row(s) without evidence_urls "
                f"(strict_evidence=true): {missing_evidence[:10]}"
            )
    else:
        empty_count = sum(
            1 for row in rows
            if not (getattr(row, "evidence_urls", []) or [])
        )
        if empty_count > 0:
            warnings.append(f"{empty_count} row(s) without evidence_urls (strict_evidence=false)")
    
    return warnings

