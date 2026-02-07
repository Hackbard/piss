from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher

from pis.models import Person


def _norm_name(name: str) -> str:
    n = name.lower().replace("ß", "ss")
    # remove common academic titles at beginning
    n = re.sub(r"^(prof\.?\s+)?dr\.?\s+", "", n).strip()
    # normalize punctuation/whitespace
    n = re.sub(r"[()]", " ", n)
    n = re.sub(r"[^a-z0-9äöü\s\-]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


@dataclass(frozen=True)
class LinkCandidate:
    dip_pis_person_id: str
    wikidata_pis_person_id: str
    score: float
    reason: str


@dataclass(frozen=True)
class ReconcileReport:
    accepted_links: list[LinkCandidate]
    pending_links: list[LinkCandidate]
    dip_unmatched: list[str]
    wikidata_unmatched: list[str]


def reconcile_wikidata_dip(
    *, wikidata_persons: list[Person], dip_persons: list[Person], min_score: float = 0.98
) -> tuple[list[Person], ReconcileReport]:
    """Reconcile DIP persons into Wikidata persons (conservative, high precision).

    Strategy (v0):
    - Primary key across sources is not available (no QID in DIP list), so we only use name.
    - Exact normalized display_name match is accepted if it is unique (1:1).
    - Everything else stays unmatched/pending.

    Canonical ID:
    - Keep the Wikidata `pis_person_id` when merging DIP into an existing Wikidata person.
    - DIP-only persons remain separate canonical persons.
    """

    wd_norm: dict[str, str] = {p.pis_person_id: _norm_name(p.display_name) for p in wikidata_persons}
    wd_by_last: dict[str, list[Person]] = {}
    for p in wikidata_persons:
        last = (wd_norm[p.pis_person_id].split() or [""])[-1]
        wd_by_last.setdefault(last, []).append(p)

    accepted: list[LinkCandidate] = []
    pending: list[LinkCandidate] = []

    merged_by_wd_id: dict[str, Person] = {p.pis_person_id: p for p in wikidata_persons}
    dip_matched_ids: set[str] = set()
    wd_matched_ids: set[str] = set()

    for dip in dip_persons:
        dip_n = _norm_name(dip.display_name)
        last = (dip_n.split() or [""])[-1]
        candidates = wd_by_last.get(last, wikidata_persons)

        scored: list[tuple[float, Person, str]] = []
        for wd in candidates:
            wd_n = wd_norm[wd.pis_person_id]
            if dip_n == wd_n and dip_n:
                scored.append((1.0, wd, "exact_normalized_name"))
                continue
            if not dip_n or not wd_n:
                continue
            ratio = SequenceMatcher(a=dip_n, b=wd_n).ratio()
            scored.append((ratio, wd, "name_similarity"))

        scored.sort(key=lambda t: t[0], reverse=True)
        top = scored[:3]
        for score, wd, reason in top:
            pending.append(
                LinkCandidate(
                    dip_pis_person_id=dip.pis_person_id,
                    wikidata_pis_person_id=wd.pis_person_id,
                    score=float(score),
                    reason=reason,
                )
            )

        if not scored:
            continue

        best_score, best_wd, best_reason = scored[0]
        second = scored[1][0] if len(scored) > 1 else 0.0
        if best_score >= min_score and (best_score - second) >= 0.02:
            cand = LinkCandidate(
                dip_pis_person_id=dip.pis_person_id,
                wikidata_pis_person_id=best_wd.pis_person_id,
                score=float(best_score),
                reason=f"accepted:{best_reason}",
            )
            accepted.append(cand)
            dip_matched_ids.add(dip.pis_person_id)
            wd_matched_ids.add(best_wd.pis_person_id)

            merged = merged_by_wd_id[best_wd.pis_person_id]
            merged.sources.extend(dip.sources)
            if merged.external_ids.dip_person_id is None:
                merged.external_ids.dip_person_id = dip.external_ids.dip_person_id
            merged.facts.setdefault("reconcile", {})
            merged.facts["reconcile"]["dip_linked_by"] = cand.reason

    dip_unmatched = [p.pis_person_id for p in dip_persons if p.pis_person_id not in dip_matched_ids]
    wd_unmatched = [p.pis_person_id for p in wikidata_persons if p.pis_person_id not in wd_matched_ids]

    # Output canonical persons = merged WD persons + DIP-only persons
    canonical: list[Person] = list(merged_by_wd_id.values()) + [
        p for p in dip_persons if p.pis_person_id in set(dip_unmatched)
    ]

    report = ReconcileReport(
        accepted_links=accepted,
        pending_links=pending,
        dip_unmatched=dip_unmatched,
        wikidata_unmatched=wd_unmatched,
    )
    return canonical, report

