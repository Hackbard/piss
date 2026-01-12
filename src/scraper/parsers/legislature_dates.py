import re
from dataclasses import dataclass
from datetime import date
from typing import Optional
import html as html_module

from scraper.mediawiki.types import MediaWikiParseResponse
from scraper.utils.date_normalize import normalize_date, should_store_raw_value


@dataclass(frozen=True)
class LegislatureDates:
    start_date: Optional[str]
    end_date: Optional[str]
    start_date_raw: Optional[str]
    end_date_raw: Optional[str]


_GERMAN_MONTHS: dict[str, int] = {
    "januar": 1,
    "februar": 2,
    "märz": 3,
    "maerz": 3,
    "april": 4,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "august": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "dezember": 12,
}


def _is_date_like(value: str) -> bool:
    v = value.strip()
    if not v:
        return False

    patterns = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{1,2}\.\d{1,2}\.\d{4}$",
        r"^\d{1,2}\.\s*(?:Januar|Februar|März|Maerz|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4}$",
        r"^(?:Januar|Februar|März|Maerz|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4}$",
        r"^(?:ab|seit)\s+\d{4}$",
        r"^\d{4}$",
    ]

    return any(re.match(p, v, flags=re.IGNORECASE) for p in patterns)


def _sanitize_raw(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    v = _strip_markup(value)
    if not v:
        return None

    return v if _is_date_like(v) else None


def _strip_markup(text: str) -> str:
    cleaned = re.sub(r"<ref[^>]*>.*?</ref>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\{\{[^{}]*\}\}", " ", cleaned)

    cleaned = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", cleaned)
    cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", cleaned)

    cleaned = html_module.unescape(cleaned)
    cleaned = (
        cleaned.replace("&nbsp;", " ")
        .replace("\u00a0", " ")
        .replace("&#160;", " ")
        .replace("&#xA0;", " ")
        .replace("&#xa0;", " ")
    )
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _parse_german_month_date(value: str) -> Optional[str]:
    m = re.search(
        r"(?P<day>\d{1,2})\.\s*(?P<month>Januar|Februar|März|Maerz|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(?P<year>\d{4})",
        value,
        flags=re.IGNORECASE,
    )
    if not m:
        return None

    day = int(m.group("day"))
    month_str = m.group("month").lower()
    month = _GERMAN_MONTHS.get(month_str)
    year = int(m.group("year"))
    if not month:
        return None

    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _normalize_candidate(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    normalized = normalize_date(value)
    if normalized:
        return normalized

    return _parse_german_month_date(value)


def _extract_from_kv_lines(text: str) -> LegislatureDates:
    start_raw: Optional[str] = None
    end_raw: Optional[str] = None

    start_keys = ("beginn", "konstituierung", "konstituierungstag", "zusammentritt")
    end_keys = ("ende", "auflösung", "aufloesung", "aufgelöst", "aufgeloest")

    for line in text.splitlines():
        if "|" not in line or "=" not in line:
            continue

        m = re.match(r"^\s*\|\s*(?P<key>[^=]+?)\s*=\s*(?P<value>.+?)\s*$", line)
        if not m:
            continue

        key = _strip_markup(m.group("key")).lower()
        value = _strip_markup(m.group("value"))
        if not value:
            continue

        if start_raw is None and any(k in key for k in start_keys):
            start_raw = value
            continue

        if end_raw is None and any(k in key for k in end_keys):
            end_raw = value
            continue

    start_raw = _sanitize_raw(start_raw)
    end_raw = _sanitize_raw(end_raw)

    start_date = _normalize_candidate(start_raw)
    end_date = _normalize_candidate(end_raw)

    return LegislatureDates(
        start_date=start_date,
        end_date=end_date,
        start_date_raw=start_raw if should_store_raw_value(start_raw, start_date) else None,
        end_date_raw=end_raw if should_store_raw_value(end_raw, end_date) else None,
    )


def _extract_from_prose(text: str) -> LegislatureDates:
    cleaned = _strip_markup(text)

    token_pattern = r"(?:\d{4}-\d{2}-\d{2}|\d{1,2}\.\d{1,2}\.\d{4}|\d{1,2}\.\s*(?:Januar|Februar|März|Maerz|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4}|(?:Januar|Februar|März|Maerz|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4}|\d{4})"
    date_pattern = rf"(?P<date>{token_pattern})"

    range_match = re.search(
        rf"\((?P<start>{token_pattern})\s*(?:–|—|-|bis)\s*(?P<end>{token_pattern})\)",
        cleaned,
        flags=re.IGNORECASE,
    )
    if range_match:
        start_raw = _sanitize_raw(range_match.group("start"))
        end_raw = _sanitize_raw(range_match.group("end"))

        start_date = _normalize_candidate(start_raw)
        end_date = _normalize_candidate(end_raw)

        return LegislatureDates(
            start_date=start_date,
            end_date=end_date,
            start_date_raw=start_raw if should_store_raw_value(start_raw, start_date) else None,
            end_date_raw=end_raw if should_store_raw_value(end_raw, end_date) else None,
        )

    start_patterns = [
        rf"(?:Beginn|begann|Konstituierung|konstituierte sich|Zusammentritt|eröffnete sich)[^0-9]{{0,40}}{date_pattern}",
        rf"(?:Beginn|Konstituierung)\s*:\s*{date_pattern}",
    ]
    end_patterns = [
        rf"(?:Ende|endete|Auflösung|Aufloesung|aufgelöst|aufgeloest)[^0-9]{{0,40}}{date_pattern}",
        rf"(?:Ende|Auflösung|Aufloesung)\s*:\s*{date_pattern}",
    ]

    def first_match(patterns: list[str]) -> Optional[str]:
        for p in patterns:
            m = re.search(p, cleaned, flags=re.IGNORECASE)
            if m:
                return m.group("date")
        return None

    start_raw = first_match(start_patterns)
    end_raw = first_match(end_patterns)
    start_raw = _sanitize_raw(start_raw)
    end_raw = _sanitize_raw(end_raw)

    start_date = _normalize_candidate(start_raw)
    end_date = _normalize_candidate(end_raw)

    return LegislatureDates(
        start_date=start_date,
        end_date=end_date,
        start_date_raw=start_raw if should_store_raw_value(start_raw, start_date) else None,
        end_date_raw=end_raw if should_store_raw_value(end_raw, end_date) else None,
    )


def extract_legislature_dates(response: MediaWikiParseResponse) -> LegislatureDates:
    if response.wikitext:
        candidates = [
            _extract_from_kv_lines(response.wikitext),
            _extract_from_prose(response.wikitext),
        ]
    else:
        candidates = [_extract_from_prose(response.html)]

    for c in candidates:
        if c.start_date or c.end_date or c.start_date_raw or c.end_date_raw:
            return c

    return LegislatureDates(start_date=None, end_date=None, start_date_raw=None, end_date_raw=None)


