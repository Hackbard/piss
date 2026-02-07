from __future__ import annotations

from dataclasses import dataclass

from pis.models import Person


def _norm_name(name: str) -> str:
    return " ".join(name.lower().replace("ß", "ss").split())


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

    wd_by_norm: dict[str, list[Person]] = {}
    for p in wikidata_persons:
        wd_by_norm.setdefault(_norm_name(p.display_name), []).append(p)

    dip_by_norm: dict[str, list[Person]] = {}
    for p in dip_persons:
        dip_by_norm.setdefault(_norm_name(p.display_name), []).append(p)

    accepted: list[LinkCandidate] = []
    pending: list[LinkCandidate] = []

    merged_by_wd_id: dict[str, Person] = {p.pis_person_id: p for p in wikidata_persons}
    dip_matched_ids: set[str] = set()
    wd_matched_ids: set[str] = set()

    for norm, dips in dip_by_norm.items():
        wds = wd_by_norm.get(norm, [])
        if not wds:
            continue
        # Only auto-accept strict 1:1
        if len(dips) == 1 and len(wds) == 1:
            dip = dips[0]
            wd = wds[0]
            cand = LinkCandidate(
                dip_pis_person_id=dip.pis_person_id,
                wikidata_pis_person_id=wd.pis_person_id,
                score=1.0,
                reason="exact_normalized_name_1to1",
            )
            accepted.append(cand)
            dip_matched_ids.add(dip.pis_person_id)
            wd_matched_ids.add(wd.pis_person_id)

            # Merge: keep WD as canonical, append DIP sources and identifiers if missing.
            merged = merged_by_wd_id[wd.pis_person_id]
            merged.sources.extend(dip.sources)
            if merged.external_ids.dip_person_id is None:
                merged.external_ids.dip_person_id = dip.external_ids.dip_person_id
            merged.facts.setdefault("reconcile", {})
            merged.facts["reconcile"]["dip_linked_by"] = cand.reason
        else:
            # ambiguous mapping
            for dip in dips:
                for wd in wds:
                    pending.append(
                        LinkCandidate(
                            dip_pis_person_id=dip.pis_person_id,
                            wikidata_pis_person_id=wd.pis_person_id,
                            score=0.5,
                            reason="ambiguous_name",
                        )
                    )

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

