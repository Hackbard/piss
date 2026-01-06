"""Evidence Gate Node: Strict evidence enforcement."""

from typing import Any

from langgraph_app.schemas import UserIntent


def evidence_gate_node(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node: Check evidence requirements."""
    intent = state.get("intent")
    tool_results = state.get("tool_results", [])

    if not intent or not intent.strict_evidence:
        return {"evidence_gate_passed": True}

    violations: list[str] = []

    for tr in tool_results:
        tool_name = tr.get("tool_name", "")
        data = tr.get("data", {})

        if tool_name == "mandates.search":
            rows = data.get("rows", [])
            for i, row in enumerate(rows):
                evidence_urls = row.get("evidence_urls", [])
                if not evidence_urls or len(evidence_urls) == 0:
                    mandate_id = row.get("mandate_id", f"row-{i}")
                    violations.append(f"mandate {mandate_id}")

        elif tool_name == "legislature.stats":
            evidence_urls = data.get("evidence_urls", [])
            if not evidence_urls or len(evidence_urls) == 0:
                legislature_id = data.get("legislature_id", "unknown")
                violations.append(f"legislature {legislature_id}")

        elif tool_name == "person.lookup":
            persons = data.get("persons", [])
            for person in persons:
                evidence_urls = person.get("evidence_urls", [])
                if not evidence_urls or len(evidence_urls) == 0:
                    person_id = person.get("person_id", "unknown")
                    violations.append(f"person {person_id}")

    if violations:
        return {
            "evidence_gate_passed": False,
            "evidence_gate_error": f"Cannot answer reliably: {len(violations)} item(s) without evidence_urls (strict_evidence=true): {', '.join(violations[:5])}",
        }

    return {"evidence_gate_passed": True}

