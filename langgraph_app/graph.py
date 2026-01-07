"""LangGraph definition for orchestrator."""

from __future__ import annotations

from datetime import date
import re
from typing import Any

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from langgraph_app.config import OrchestratorConfig
from langgraph_app.nodes.compute import compute_node
from langgraph_app.nodes.evidence_gate import evidence_gate_node
from langgraph_app.nodes.intent_parser import intent_parser_node
from langgraph_app.nodes.policy_guard import policy_guard_node
from langgraph_app.nodes.response_composer import response_composer_node
from langgraph_app.nodes.router import router_node
from langgraph_app.nodes.tool_executor import tool_executor_node
from langgraph_app.schemas import ComputedResult, ToolCall, ToolResult, UserIntent
from langgraph_app.settings import STRICT_EVIDENCE_DEFAULT
from langgraph_app.tools import members_list


class GraphState(TypedDict):
    """State for LangGraph orchestrator."""

    question: str
    intent: UserIntent | None
    tool_calls: list[dict[str, Any]]
    tool_results: list[dict[str, Any]]
    computed: ComputedResult | None
    evidence_gate_passed: bool
    evidence_gate_error: str | None
    policy_decision: str | None
    policy_warnings: list[str]
    safe_answer_plan: dict[str, Any]
    final_answer: str | None


def create_graph(config: OrchestratorConfig) -> StateGraph:
    """Create LangGraph orchestrator."""
    workflow = StateGraph(GraphState)

    workflow.add_node("intent_parser", lambda state: intent_parser_node(state, config))
    workflow.add_node("router", router_node)
    workflow.add_node("tool_executor", lambda state: tool_executor_node(state, config))
    workflow.add_node("evidence_gate", evidence_gate_node)
    workflow.add_node("compute", compute_node)
    workflow.add_node("policy_guard", lambda state: policy_guard_node(state, config))
    workflow.add_node("response_composer", lambda state: response_composer_node(state, config))

    workflow.set_entry_point("intent_parser")

    workflow.add_edge("intent_parser", "router")

    def route_after_router(state: GraphState) -> str:
        """Route after router based on intent."""
        intent = state.get("intent")
        if intent and intent.needs_clarification:
            return "response_composer"
        tool_calls = state.get("tool_calls", [])
        if not tool_calls:
            return "response_composer"
        return "tool_executor"

    workflow.add_conditional_edges("router", route_after_router)

    workflow.add_edge("tool_executor", "evidence_gate")

    def route_after_evidence(state: GraphState) -> str:
        """Route after evidence gate."""
        if not state.get("evidence_gate_passed", True):
            return "response_composer"
        return "compute"

    workflow.add_conditional_edges("evidence_gate", route_after_evidence)

    workflow.add_edge("compute", "policy_guard")
    workflow.add_edge("policy_guard", "response_composer")
    workflow.add_edge("response_composer", END)

    return workflow.compile()


class MembersListMvpState(TypedDict):
    question: str
    tool_input: dict[str, Any] | None
    tool_result: dict[str, Any] | None
    answer: str | None


def _iso_date(year: int, month: int, day: int) -> str:
    return date(year, month, day).isoformat()


def _parse_members_list_tool_input(question: str) -> dict[str, Any] | None:
    q = question.lower()

    parliament_id: str | None = None
    if "niedersachsen" in q or "landtag niedersachsen" in q:
        parliament_id = "NI"
    if "bundestag" in q or "deutscher bundestag" in q:
        parliament_id = "BT"

    party_code: str | None = None
    for code in ("SPD", "CDU", "CSU", "GRUENE", "GRÜNE", "FDP", "LINKE", "AFD"):
        normalized = code.lower().replace("ü", "u")
        if re.search(rf"\b{re.escape(normalized)}\b", q.replace("ü", "u")):
            party_code = "GRUENE" if code in {"GRUENE", "GRÜNE"} else code
            break

    from_date: str | None = None
    to_date: str | None = None
    years = [int(y) for y in re.findall(r"(?:19|20)\d{2}", q)]
    if len(years) >= 2:
        start_year, end_year = years[0], years[1]
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        from_date = _iso_date(start_year, 1, 1)
        to_date = _iso_date(end_year, 12, 31)

    if not parliament_id or not party_code or not from_date or not to_date:
        return None

    return {
        "parliament_id": parliament_id,
        "party_code": party_code,
        "from_date": from_date,
        "to_date": to_date,
        "limit": 200,
        "offset": 0,
        "strict_evidence": STRICT_EVIDENCE_DEFAULT,
    }


def members_list_plan_node(state: MembersListMvpState) -> dict[str, Any]:
    question = state.get("question", "")
    tool_input = _parse_members_list_tool_input(question)
    if tool_input is None:
        return {
            "tool_input": None,
            "answer": "Welche Partei (z. B. SPD) und welchen Zeitraum (von/bis) soll ich für welches Parlament abfragen?",
        }
    return {"tool_input": tool_input, "answer": None}


def members_list_call_tool_node(state: MembersListMvpState) -> dict[str, Any]:
    tool_input = state.get("tool_input")
    if not tool_input:
        return {"tool_result": None}
    try:
        result = members_list(**tool_input)
        return {"tool_result": result}
    except Exception as e:
        return {"tool_result": {"error": str(e)}}


def _extract_urls(value: Any) -> list[str]:
    urls: list[str] = []
    if isinstance(value, str):
        if value.startswith("http://") or value.startswith("https://"):
            urls.append(value)
        return urls
    if isinstance(value, list):
        for item in value:
            urls.extend(_extract_urls(item))
        return urls
    if isinstance(value, dict):
        for k in ("url", "href"):
            if k in value:
                urls.extend(_extract_urls(value[k]))
        for v in value.values():
            urls.extend(_extract_urls(v))
        return urls
    return urls


def _format_date_de(iso: str | None) -> str:
    if not iso or len(iso) < 10:
        return "?"
    try:
        y = int(iso[0:4])
        m = int(iso[5:7])
        d = int(iso[8:10])
        return f"{d:02d}.{m:02d}.{y:04d}"
    except ValueError:
        return "?"


def _coerce_members_list(tool_result: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("members", "rows", "items", "results"):
        val = tool_result.get(key)
        if isinstance(val, list):
            return [m for m in val if isinstance(m, dict)]
    data = tool_result.get("data")
    if isinstance(data, dict):
        for key in ("members", "rows", "items", "results"):
            val = data.get(key)
            if isinstance(val, list):
                return [m for m in val if isinstance(m, dict)]
    return []


def format_member_row(row: dict[str, Any]) -> str:
    person_name = (
        row.get("person_name")
        or row.get("name")
        or row.get("person", {}).get("name")
        or "?"
    )
    wikipedia_title = (
        row.get("wikipedia_title")
        or row.get("wikipedia")
        or row.get("person", {}).get("wikipedia_title")
    )

    active_first_start = row.get("active_first_start_date")
    active_last_end = row.get("active_last_end_date")
    raw_first_start = row.get("first_start_date") or row.get("from_date") or row.get("start_date")
    raw_last_end = row.get("last_end_date") or row.get("to_date") or row.get("end_date")

    start = active_first_start if active_first_start else raw_first_start
    end = active_last_end if active_last_end else raw_last_end

    start_str = start if isinstance(start, str) and len(start) >= 10 else None
    end_str = end if isinstance(end, str) and len(end) >= 10 else None

    if start_str:
        date_part = start_str[:10]
        if end_str:
            date_part = f"{date_part} … {end_str[:10]}"
        else:
            date_part = f"{date_part} … (offen)"
    else:
        date_part = "? … ?"

    mandate_note = ""
    raw_last_end_str = raw_last_end if isinstance(raw_last_end, str) and len(raw_last_end) >= 10 else None
    active_last_end_str = active_last_end if isinstance(active_last_end, str) and len(active_last_end) >= 10 else None
    if raw_last_end_str and active_last_end_str and raw_last_end_str > active_last_end_str:
        mandate_note = f" (Mandat bis {raw_last_end_str[:10]})"

    title_part = f" ({wikipedia_title})" if isinstance(wikipedia_title, str) and wikipedia_title else ""
    return f"- {person_name}{title_part} – {date_part}{mandate_note}"


def members_list_answer_node(state: MembersListMvpState) -> dict[str, Any]:
    if state.get("answer"):
        return {}

    tool_result = state.get("tool_result") or {}
    tool_input = state.get("tool_input") or {}
    if isinstance(tool_result, dict) and tool_result.get("error"):
        message = str(tool_result.get("error"))
        return {"answer": f"Tool-Fehler: {message}"}
    members = _coerce_members_list(tool_result)

    party = tool_input.get("party_code") or "?"
    parliament_id = tool_input.get("parliament_id") or "?"
    parliament_name = {"NI": "Landtag Niedersachsen", "BT": "Deutschen Bundestag"}.get(
        str(parliament_id),
        str(parliament_id),
    )
    from_de = _format_date_de(str(tool_input.get("from_date")) if tool_input.get("from_date") else None)
    to_de = _format_date_de(str(tool_input.get("to_date")) if tool_input.get("to_date") else None)

    headline = f"{party}-Mitglieder im {parliament_name} ({from_de}–{to_de})"
    lines: list[str] = [headline, f"Anzahl: {len(members)}", ""]

    sources: list[str] = []

    for m in members:
        lines.append(format_member_row(m))

        member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
        sources.extend(member_sources[:2])

    meta_sources = _extract_urls(tool_result.get("evidence_urls") or tool_result.get("sources"))
    sources.extend(meta_sources)

    deduped_sources: list[str] = []
    seen: set[str] = set()
    for s in sources:
        if s not in seen:
            deduped_sources.append(s)
            seen.add(s)
        if len(deduped_sources) >= 20:
            break

    lines.append("")
    lines.append("Quellen:")
    if not deduped_sources:
        lines.append("- (keine Quellen in der Tool-Antwort enthalten)")
    else:
        for s in deduped_sources:
            lines.append(f"- {s}")

    return {"answer": "\n".join(lines)}


def create_members_list_mvp_graph() -> StateGraph:
    workflow = StateGraph(MembersListMvpState)
    workflow.add_node("plan", members_list_plan_node)
    workflow.add_node("call_tool", members_list_call_tool_node)
    workflow.add_node("answer", members_list_answer_node)

    workflow.set_entry_point("plan")

    def _route_after_plan(state: MembersListMvpState) -> str:
        if state.get("tool_input") is None:
            return "answer"
        return "call_tool"

    workflow.add_conditional_edges("plan", _route_after_plan)
    workflow.add_edge("call_tool", "answer")
    workflow.add_edge("answer", END)
    return workflow.compile()



