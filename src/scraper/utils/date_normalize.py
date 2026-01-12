"""Date normalization utilities for mandate dates."""

import re
from datetime import date, datetime
from typing import Optional


INVALID_DATE_STRINGS = {
    "",
    "unknown",
    "n/a",
    "na",
    "—",
    "–",
    "-",
    "?",
    "??",
    "???",
    "tbd",
    "to be determined",
    "unbekannt",
    "nicht bekannt",
}


def normalize_date(value: Optional[str | date | datetime]) -> Optional[str]:
    """
    Normalize date value to ISO format string or None.
    
    Rules:
    - None -> None
    - Empty/invalid strings ("unknown", "", "—", etc.) -> None
    - ISO format YYYY-MM-DD -> unverändert zurückgeben
    - date/datetime objects -> ISO string
    - DD.MM.YYYY -> ISO format (optional, nur wenn wirklich nötig)
    
    Args:
        value: Date value (None, str, date, or datetime)
    
    Returns:
        ISO date string (YYYY-MM-DD) or None
    """
    if value is None:
        return None
    
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    
    if not isinstance(value, str):
        return None
    
    value_trimmed = value.strip()
    
    if not value_trimmed:
        return None
    
    if value_trimmed.lower() in INVALID_DATE_STRINGS:
        return None
    
    if value_trimmed.startswith("—") or value_trimmed.startswith("–"):
        return None
    
    if value_trimmed == "-":
        return None
    
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value_trimmed):
        return value_trimmed
    
    try:
        if re.match(r"^\d{1,2}\.\d{1,2}\.\d{4}$", value_trimmed):
            parts = value_trimmed.split(".")
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2])
            d = date(year, month, day)
            return d.isoformat()
    except (ValueError, IndexError):
        pass
    
    try:
        dt = datetime.fromisoformat(value_trimmed.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except (ValueError, AttributeError):
        pass
    
    return None


def should_store_raw_value(raw: Optional[str], normalized: Optional[str]) -> bool:
    """
    Determine if raw value should be stored for traceability.
    
    Args:
        raw: Original raw value
        normalized: Normalized value (or None)
    
    Returns:
        True if raw value should be stored
    """
    if not raw or not isinstance(raw, str):
        return False
    
    raw_trimmed = raw.strip()
    
    if not raw_trimmed:
        return False
    
    if raw_trimmed.lower() in INVALID_DATE_STRINGS:
        return False
    
    if normalized is None:
        return True
    
    return False

