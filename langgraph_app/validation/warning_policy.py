"""Warning policy loader and validation."""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


class WarningPolicy:
    """Warning classification policy."""

    def __init__(
        self,
        allowed_codes: List[str],
        actionable_codes: List[str],
        ignored_codes: List[str],
        notes: Optional[Dict[str, str]] = None,
    ):
        self.allowed_codes = set(allowed_codes)
        self.actionable_codes = set(actionable_codes)
        self.ignored_codes = set(ignored_codes)
        self.notes = notes or {}

    def is_allowed(self, code: str) -> bool:
        """Check if a warning code is allowed (informational)."""
        return code in self.allowed_codes

    def is_actionable(self, code: str) -> bool:
        """Check if a warning code is actionable (should be fixed)."""
        return code in self.actionable_codes

    def is_ignored(self, code: str) -> bool:
        """Check if a warning code is explicitly ignored."""
        return code in self.ignored_codes

    def get_note(self, code: str) -> Optional[str]:
        """Get note for a warning code if available."""
        return self.notes.get(code)

    @classmethod
    def load(cls, policy_path: Path) -> "WarningPolicy":
        """Load warning policy from YAML file."""
        if not policy_path.exists():
            return cls(allowed_codes=[], actionable_codes=[], ignored_codes=[])

        with policy_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            raise ValueError(f"Invalid policy file format: {policy_path}")

        version = data.get("version", 1)
        if version != 1:
            raise ValueError(f"Unsupported policy version: {version}")

        allowed_codes = data.get("allowed_codes", [])
        actionable_codes = data.get("actionable_codes", [])
        ignored_codes = data.get("ignored_codes", [])
        notes = data.get("notes", {})

        if not isinstance(allowed_codes, list) or not all(isinstance(c, str) for c in allowed_codes):
            raise ValueError("allowed_codes must be a list of strings")
        if not isinstance(actionable_codes, list) or not all(isinstance(c, str) for c in actionable_codes):
            raise ValueError("actionable_codes must be a list of strings")
        if not isinstance(ignored_codes, list) or not all(isinstance(c, str) for c in ignored_codes):
            raise ValueError("ignored_codes must be a list of strings")
        if not isinstance(notes, dict):
            raise ValueError("notes must be a dictionary")

        return cls(
            allowed_codes=allowed_codes,
            actionable_codes=actionable_codes,
            ignored_codes=ignored_codes,
            notes=notes,
        )
