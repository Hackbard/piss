from __future__ import annotations

import re
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LandtagSpec:
    key_prefix: str
    parliament_id: str
    parliament: str
    state: str


GERMAN_MONTHS: dict[str, int] = {
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


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_legislature_number(title: str) -> int | None:
    match = re.search(r"\((\d+)\.\s*Wahlperiode\)", title)
    if not match:
        return None
    return int(match.group(1))


def _normalize(text: str) -> str:
    return text.lower().replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")


def _choose_spec_by_title(title: str, specs: list[LandtagSpec]) -> LandtagSpec | None:
    t = _normalize(title)

    patterns: list[tuple[re.Pattern[str], str]] = [
        (re.compile(r"\bbaden[- ]wuerttemberg\b|\bbw[- ]wuerttemberg\b"), "BW"),
        (re.compile(r"\bbayer"), "BY"),
        (re.compile(r"\bberlin\b|abgeordnetenhaus"), "BE"),
        (re.compile(r"\bbrandenburg\b"), "BB"),
        (re.compile(r"\bbrem"), "HB"),
        (re.compile(r"\bhamburg"), "HH"),
        (re.compile(r"\bhess"), "HE"),
        (re.compile(r"\bmecklenburg[- ]vorpommern\b|\bm[- ]v\b"), "MV"),
        (re.compile(r"\bniedersaechs|\bniedersachs"), "NI"),
        (re.compile(r"\bnordrhein[- ]westfalen\b"), "NW"),
        (re.compile(r"\brheinland[- ]pfalz\b"), "RP"),
        (re.compile(r"\bsaar"), "SL"),
        (re.compile(r"\bsachsen[- ]anhalt\b"), "ST"),
        (re.compile(r"\bsaechs|\bsachsen\b"), "SN"),
        (re.compile(r"\bschleswig[- ]holstein"), "SH"),
        (re.compile(r"\bthuering|\bthuer"), "TH"),
    ]

    detected: str | None = None
    for rx, parliament_id in patterns:
        if rx.search(t):
            detected = parliament_id
            break

    if detected is None:
        return None

    for spec in specs:
        if spec.parliament_id == detected:
            return spec

    return None


def _extract_text_snippet(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:4000]


def _parse_iso_date(year: int, month: int, day: int) -> str:
    return f"{year:04d}-{month:02d}-{day:02d}"


def _extract_dates(text: str) -> list[str]:
    dates: list[str] = []

    for m in re.finditer(r"\b(\d{4})-(\d{2})-(\d{2})\b", text):
        dates.append(_parse_iso_date(int(m.group(1)), int(m.group(2)), int(m.group(3))))

    for m in re.finditer(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b", text):
        dates.append(_parse_iso_date(int(m.group(3)), int(m.group(2)), int(m.group(1))))

    for m in re.finditer(
        r"\b(\d{1,2})\.\s*([A-Za-zÄÖÜäöüß]+)\s*(\d{4})\b",
        text,
    ):
        day = int(m.group(1))
        month_raw = _normalize(m.group(2))
        year = int(m.group(3))
        month = GERMAN_MONTHS.get(month_raw)
        if month:
            dates.append(_parse_iso_date(year, month, day))

    return dates


def _best_effort_time_range(html: str) -> tuple[str, str]:
    snippet = _extract_text_snippet(html)
    dates = _extract_dates(snippet)
    if len(dates) < 2:
        return "", ""
    dates_sorted = sorted(dates)
    return dates_sorted[0], dates_sorted[-1]


def _iter_member_list_parse_raw(cache_dir: Path) -> list[Path]:
    raw_paths: list[Path] = []
    for latest in cache_dir.glob("Liste_der_Mitglieder*/**/parse/raw.json"):
        raw_paths.append(latest)
    return sorted(raw_paths)


def _iter_member_list_search_raw(cache_dir: Path) -> list[Path]:
    raw_paths: list[Path] = []
    for raw in cache_dir.glob("search_intitle__Liste_der_Mitglieder*/**/search/raw.json"):
        raw_paths.append(raw)
    return sorted(raw_paths)


def _extract_titles_from_search(raw_path: Path) -> list[str]:
    data = json.loads(raw_path.read_text(encoding="utf-8"))
    results = (data or {}).get("query", {}).get("search", [])
    titles: list[str] = []
    for item in results:
        title = item.get("title")
        if isinstance(title, str) and title:
            titles.append(title)
    return titles


def _load_title_and_html(raw_path: Path) -> tuple[str, str]:
    data = json.loads(raw_path.read_text(encoding="utf-8"))
    parse = (data or {}).get("parse", {})
    title = parse.get("title", "")
    html = (parse.get("text") or {}).get("*", "")
    return title, html


def _load_revision_id(raw_path: Path) -> int | None:
    try:
        return int(raw_path.parent.parent.name)
    except ValueError:
        return None


def _load_page_id_from_metadata(raw_path: Path) -> int | None:
    metadata_path = raw_path.parent / "metadata.json"
    if not metadata_path.exists():
        return None
    meta = json.loads(metadata_path.read_text(encoding="utf-8"))
    page_id = (meta or {}).get("page_id")
    if page_id is None:
        return None
    try:
        return int(page_id)
    except ValueError:
        return None


def _load_landtag_specs(registry_path: Path) -> list[LandtagSpec]:
    text = _read_text(registry_path)
    specs: list[LandtagSpec] = []

    current: dict[str, str] | None = None
    in_landtage = False
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")

        if line.strip() == "landtage:":
            in_landtage = True
            continue

        if not in_landtage:
            continue

        m_key = re.match(r"^\s{2}([a-z]{2}):\s*$", line)
        if m_key:
            if current:
                specs.append(
                    LandtagSpec(
                        key_prefix=current.get("key_prefix", ""),
                        parliament_id="",
                        parliament=current.get("parliament", ""),
                        state=current.get("state", ""),
                    )
                )
            current = {}
            continue

        if current is None:
            continue

        m_field = re.match(r"^\s{4}([a-z_]+):\s*(.+?)\s*$", line)
        if not m_field:
            continue

        field = m_field.group(1)
        value = m_field.group(2).strip().strip('"').strip("'")
        if field in {"key_prefix", "state", "parliament"}:
            current[field] = value

    if current:
        specs.append(
            LandtagSpec(
                key_prefix=current.get("key_prefix", ""),
                parliament_id="",
                parliament=current.get("parliament", ""),
                state=current.get("state", ""),
            )
        )

    return specs


def _attach_parliament_id(spec: LandtagSpec) -> LandtagSpec:
    mapping = {
        "Baden-Württemberg": "BW",
        "Bayern": "BY",
        "Berlin": "BE",
        "Brandenburg": "BB",
        "Bremen": "HB",
        "Hamburg": "HH",
        "Hessen": "HE",
        "Mecklenburg-Vorpommern": "MV",
        "Niedersachsen": "NI",
        "Nordrhein-Westfalen": "NW",
        "Rheinland-Pfalz": "RP",
        "Saarland": "SL",
        "Sachsen": "SN",
        "Sachsen-Anhalt": "ST",
        "Schleswig-Holstein": "SH",
        "Thüringen": "TH",
    }
    parliament_id = mapping.get(spec.state, spec.parliament_id)
    return LandtagSpec(
        key_prefix=spec.key_prefix,
        parliament_id=parliament_id,
        parliament=spec.parliament,
        state=spec.state,
    )


def generate_seeds(
    *,
    cache_dir: Path,
    registry_path: Path,
) -> dict[str, Any]:
    specs = [_attach_parliament_id(s) for s in _load_landtag_specs(registry_path)]
    spec_by_parliament_id: dict[str, LandtagSpec] = {s.parliament_id: s for s in specs if s.parliament_id}

    seeds: dict[str, Any] = {}
    max_leg_by_id: dict[str, int] = {}
    template_title_by_id: dict[str, str] = {}

    for raw_path in _iter_member_list_parse_raw(cache_dir):
        title, html = _load_title_and_html(raw_path)
        if not title:
            continue

        legislature_number = _extract_legislature_number(title)
        if not legislature_number:
            continue

        spec = _choose_spec_by_title(title, specs)
        if spec is None:
            continue

        seed_key = f"{spec.key_prefix}{legislature_number}"
        start, end = _best_effort_time_range(html)

        seed_data: dict[str, Any] = {
            "key": seed_key,
            "page_title": title,
            "expected_time_range": {"start": start, "end": end},
            "hints": {
                "parliament_id": spec.parliament_id,
                "parliament": spec.parliament,
                "state": spec.state,
                "legislature_number": legislature_number,
                "section_keywords": ["Mitglieder", "Abgeordnete"],
                "expected_table_keywords": ["Name", "Partei", "Wahlkreis"],
            },
        }

        revision_id = _load_revision_id(raw_path)
        page_id = _load_page_id_from_metadata(raw_path)
        if revision_id:
            seed_data["revision_id"] = revision_id
        if page_id:
            seed_data["page_id"] = page_id

        seeds[seed_key] = seed_data
        max_leg_by_id[spec.parliament_id] = max(max_leg_by_id.get(spec.parliament_id, 0), legislature_number)
        template_title_by_id.setdefault(spec.parliament_id, title)

    for search_raw in _iter_member_list_search_raw(cache_dir):
        for title in _extract_titles_from_search(search_raw):
            legislature_number = _extract_legislature_number(title)
            if not legislature_number:
                continue

            spec = _choose_spec_by_title(title, specs)
            if spec is None:
                continue

            seed_key = f"{spec.key_prefix}{legislature_number}"
            if seed_key in seeds:
                continue

            seeds[seed_key] = {
                "key": seed_key,
                "page_title": title,
                "expected_time_range": {"start": "", "end": ""},
                "hints": {
                    "parliament_id": spec.parliament_id,
                    "parliament": spec.parliament,
                    "state": spec.state,
                    "legislature_number": legislature_number,
                    "section_keywords": ["Mitglieder", "Abgeordnete"],
                    "expected_table_keywords": ["Name", "Partei", "Wahlkreis"],
                },
            }
            max_leg_by_id[spec.parliament_id] = max(max_leg_by_id.get(spec.parliament_id, 0), legislature_number)
            template_title_by_id.setdefault(spec.parliament_id, title)

    default_max: dict[str, int] = {
        "SH": 20,
    }

    fallback_title_template_by_id: dict[str, str] = {
        "SH": "Liste der Mitglieder des Schleswig-Holsteinischen Landtages ({n}. Wahlperiode)",
    }

    for parliament_id, spec in spec_by_parliament_id.items():
        max_leg = max_leg_by_id.get(parliament_id, default_max.get(parliament_id, 0))
        if max_leg <= 0:
            continue

        template_title = template_title_by_id.get(parliament_id)
        if template_title is None:
            fallback = fallback_title_template_by_id.get(parliament_id)
            if fallback:
                template_title = fallback.format(n=1)
            else:
                template_title = f"Liste der Mitglieder des {spec.parliament} (1. Wahlperiode)"

        for n in range(1, max_leg + 1):
            seed_key = f"{spec.key_prefix}{n}"
            if seed_key in seeds:
                continue

            if parliament_id in fallback_title_template_by_id:
                title = fallback_title_template_by_id[parliament_id].format(n=n)
            else:
                title = re.sub(r"\(\d+\.\s*Wahlperiode\)", f"({n}. Wahlperiode)", template_title)
            seeds[seed_key] = {
                "key": seed_key,
                "page_title": title,
                "expected_time_range": {"start": "", "end": ""},
                "hints": {
                    "parliament_id": spec.parliament_id,
                    "parliament": spec.parliament,
                    "state": spec.state,
                    "legislature_number": n,
                    "section_keywords": ["Mitglieder", "Abgeordnete"],
                    "expected_table_keywords": ["Name", "Partei", "Wahlkreis"],
                },
            }

    return dict(sorted(seeds.items(), key=lambda kv: kv[0]))


def _yaml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{escaped}\""


def _yaml_lines_for_seed(seed_key: str, seed: dict[str, Any]) -> list[str]:
    lines: list[str] = [f"{seed_key}:"]
    lines.append(f"  key: {_yaml_quote(str(seed['key']))}")
    lines.append(f"  page_title: {_yaml_quote(str(seed['page_title']))}")
    lines.append("  expected_time_range:")
    lines.append(f"    start: {_yaml_quote(str(seed['expected_time_range'].get('start', '')))}")
    lines.append(f"    end: {_yaml_quote(str(seed['expected_time_range'].get('end', '')))}")

    hints = seed.get("hints", {})
    lines.append("  hints:")
    lines.append(f"    parliament_id: {_yaml_quote(str(hints.get('parliament_id', '')))}")
    lines.append(f"    parliament: {_yaml_quote(str(hints.get('parliament', '')))}")
    lines.append(f"    state: {_yaml_quote(str(hints.get('state', '')))}")
    lines.append(f"    legislature_number: {int(hints.get('legislature_number', 0))}")
    lines.append("    section_keywords:")
    for kw in hints.get("section_keywords", []):
        lines.append(f"      - {_yaml_quote(str(kw))}")
    lines.append("    expected_table_keywords:")
    for kw in hints.get("expected_table_keywords", []):
        lines.append(f"      - {_yaml_quote(str(kw))}")

    if "page_id" in seed:
        lines.append(f"  page_id: {int(seed['page_id'])}")
    if "revision_id" in seed:
        lines.append(f"  revision_id: {int(seed['revision_id'])}")
    return lines


def _dump_seeds_yaml(seeds: dict[str, Any]) -> str:
    out_lines: list[str] = []
    for seed_key, seed in seeds.items():
        out_lines.extend(_yaml_lines_for_seed(seed_key, seed))
    out_lines.append("")
    return "\n".join(out_lines)

def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    cache_dir = repo_root / "data" / "cache" / "mediawiki"
    registry_path = repo_root / "config" / "landtage_registry.yaml"
    output_path = repo_root / "config" / "seeds.yaml"

    seeds = generate_seeds(cache_dir=cache_dir, registry_path=registry_path)
    output_path.write_text(_dump_seeds_yaml(seeds), encoding="utf-8")
    print(f"Wrote {len(seeds)} seeds to {output_path}")


if __name__ == "__main__":
    main()


