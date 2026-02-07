from __future__ import annotations

from datetime import date

from pis.models import Person


def _fmt_date(d: date | None) -> str | None:
    return d.isoformat() if d else None


def build_persona_summary(person: Person) -> str:
    """Build a concise, factual summary suitable for RAG.

    This is intentionally conservative and only uses already-known structured facts.
    """
    parts: list[str] = []

    name = person.display_name
    b = _fmt_date(person.birth_date)
    d = _fmt_date(person.death_date)
    if b and d:
        parts.append(f"{name} (* {b}, † {d})")
    elif b:
        parts.append(f"{name} (* {b})")
    elif d:
        parts.append(f"{name} († {d})")
    else:
        parts.append(name)

    # Parties / factions (best-effort)
    dip_fraktion = person.facts.get("dip_fraktion")
    if isinstance(dip_fraktion, str) and dip_fraktion.strip():
        parts.append(f"Fraktion (DIP): {dip_fraktion.strip()}.")

    # Wikipedia intro snippet if present
    intro = person.facts.get("wikipedia_intro")
    if isinstance(intro, str):
        intro_clean = " ".join(intro.split()).strip()
        if intro_clean:
            parts.append(intro_clean)

    return " ".join(parts).strip()


def ensure_persona(person: Person) -> Person:
    """Ensure `persona_summary` is populated (pure update)."""
    if person.persona_summary and person.persona_summary.strip():
        return person
    return person.model_copy(update={"persona_summary": build_persona_summary(person)})

