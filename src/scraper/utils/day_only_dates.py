import re
from dataclasses import dataclass
from typing import Any, Literal, Optional


DatePrecision = Literal["day", "month", "year", "unknown"]
DateSource = Literal["official", "wikidata", "wikipedia"]


@dataclass(frozen=True)
class DayOnlyDate:
    value: Optional[str]
    precision: DatePrecision
    raw: Optional[str]
    source: Optional[DateSource]
    source_meta: Optional[dict[str, Any]]


_ISO_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def apply_day_only_date(
    node_props: dict[str, Any],
    field: str,
    value_iso: Optional[str],
    precision: Optional[str],
    raw: Optional[str],
    source: Optional[str],
    source_meta: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    precision_norm: DatePrecision = (precision or "unknown")  # type: ignore[assignment]
    if precision_norm not in {"day", "month", "year", "unknown"}:
        precision_norm = "unknown"

    if value_iso is not None and not _ISO_DAY_RE.match(value_iso):
        raise ValueError(f"{field}: value must be ISO day (YYYY-MM-DD) or None, got {value_iso!r}")

    if value_iso is None or precision_norm != "day":
        node_props[f"{field}_raw"] = raw.strip() if isinstance(raw, str) and raw.strip() else None
        node_props[f"{field}_precision"] = precision_norm
        return node_props

    node_props[field] = value_iso
    node_props[f"{field}_precision"] = "day"
    if source:
        node_props[f"{field}_source"] = source
    if raw:
        node_props[f"{field}_raw"] = None
    if source_meta is not None:
        node_props[f"{field}_source_meta"] = source_meta
    return node_props


def assert_day_invariant(node_props: dict[str, Any], field: str) -> None:
    value = node_props.get(field)
    precision = node_props.get(f"{field}_precision")
    if value is None:
        return
    if precision != "day":
        raise ValueError(f"Invariant violated: {field} is set but {field}_precision != 'day'")

