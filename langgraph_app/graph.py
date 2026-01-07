"""LangGraph definition for orchestrator."""

from __future__ import annotations

from datetime import date, datetime
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


class MembersListMvpState(TypedDict, total=False):
    question: str
    tool_input: dict[str, Any] | None
    tool_result: dict[str, Any] | None
    answer: str | None
    output_format: str
    sources_mode: str
    max_sources: int


PARTY_ALIASES: dict[str, str] = {
    "spd": "SPD",
    "cdu": "CDU",
    "csu": "CSU",
    "grüne": "GRUENE",
    "gruene": "GRUENE",
    "grünen": "GRUENE",
    "gruenen": "GRUENE",
    "fdp": "FDP",
    "afd": "AFD",
    "linke": "LINKE",
    "die linke": "LINKE",
}

PARLIAMENT_ALIASES: dict[str, str] = {
    "niedersachsen": "NI",
    "landtag niedersachsen": "NI",
    "niedersächsisch": "NI",
    "bundestag": "BT",
    "deutscher bundestag": "BT",
    "hessen": "HE",
    "hessisch": "HE",
    "hessischer landtag": "HE",
    "baden-württemberg": "BW",
    "baden württemberg": "BW",
    "bw": "BW",
    "bayern": "BY",
    "bayerisch": "BY",
    "bayerischer landtag": "BY",
    "by": "BY",
    "berlin": "BE",
    "abgeordnetenhaus": "BE",
    "be": "BE",
    "brandenburg": "BB",
    "bb": "BB",
    "bremen": "HB",
    "bremisch": "HB",
    "bremische bürgerschaft": "HB",
    "hb": "HB",
    "hamburg": "HH",
    "hamburgisch": "HH",
    "hamburgische bürgerschaft": "HH",
    "hh": "HH",
    "mecklenburg": "MV",
    "vorpommern": "MV",
    "mv": "MV",
    "nordrhein-westfalen": "NW",
    "nordrhein westfalen": "NW",
    "nrw": "NW",
    "nw": "NW",
    "rheinland-pfalz": "RP",
    "rheinland pfalz": "RP",
    "rp": "RP",
    "saarland": "SL",
    "sl": "SL",
    "sachsen": "SN",
    "sn": "SN",
    "sachsen-anhalt": "ST",
    "sachsen anhalt": "ST",
    "st": "ST",
    "schleswig-holstein": "SH",
    "schleswig holstein": "SH",
    "sh": "SH",
    "thüringen": "TH",
    "thueringen": "TH",
    "th": "TH",
}


def _iso_date(year: int, month: int, day: int) -> str:
    return date(year, month, day).isoformat()


def _parse_date_range(question: str) -> tuple[str | None, str | None]:
    q = question.lower()
    today = datetime.now().date()
    
    years = [int(y) for y in re.findall(r"(?:19|20)\d{2}", q)]
    
    if "zwischen" in q and "und" in q:
        if len(years) >= 2:
            start_year, end_year = years[0], years[1]
            if start_year > end_year:
                start_year, end_year = end_year, start_year
            return _iso_date(start_year, 1, 1), _iso_date(end_year, 12, 31)
    
    range_match = re.search(r"(\d{4})\s*[-–]\s*(\d{4})", q)
    if range_match:
        start_year = int(range_match.group(1))
        end_year = int(range_match.group(2))
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        return _iso_date(start_year, 1, 1), _iso_date(end_year, 12, 31)
    
    if "ab" in q and years:
        start_year = years[0]
        return _iso_date(start_year, 1, 1), today.isoformat()
    
    if "bis" in q and years:
        end_year = years[0]
        return _iso_date(1, 1, 1), _iso_date(end_year, 12, 31)
    
    if len(years) >= 2:
        start_year, end_year = years[0], years[1]
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        return _iso_date(start_year, 1, 1), _iso_date(end_year, 12, 31)
    
    if len(years) == 1:
        year = years[0]
        return _iso_date(year, 1, 1), _iso_date(year, 12, 31)
    
    return None, None


def _parse_members_list_tool_input(question: str) -> dict[str, Any]:
    q = question.lower()
    
    parliament_id: str | None = None
    for alias, pid in PARLIAMENT_ALIASES.items():
        if alias in q:
            parliament_id = pid
            break
    
    party_code: str | None = None
    for alias, code in PARTY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", q):
            party_code = code
            break
    
    from_date, to_date = _parse_date_range(question)
    
    tool_input: dict[str, Any] = {
        "limit": 200,
        "offset": 0,
        "strict_evidence": STRICT_EVIDENCE_DEFAULT,
    }
    
    if parliament_id:
        tool_input["parliament_id"] = parliament_id
    if party_code:
        tool_input["party_code"] = party_code
    if from_date:
        tool_input["from_date"] = from_date
    if to_date:
        tool_input["to_date"] = to_date
    
    return tool_input


def members_list_plan_node(state: MembersListMvpState) -> dict[str, Any]:
    question = state.get("question", "")
    tool_input = _parse_members_list_tool_input(question)
    
    missing = []
    if not tool_input.get("parliament_id"):
        missing.append("Parlament")
    if not tool_input.get("party_code"):
        missing.append("Partei")
    if not tool_input.get("from_date") or not tool_input.get("to_date"):
        missing.append("Zeitraum")
    
    if missing:
        return {
            "tool_input": None,
            "answer": f"Welche {' / '.join(missing)} soll ich abfragen? Bitte spezifizieren Sie: {' / '.join(missing)}.",
        }
    
    return {"tool_input": tool_input, "answer": None}


def _merge_member_rows(rows: list[dict[str, Any]], max_sources: int = 20) -> list[dict[str, Any]]:
    by_person_id: dict[str, dict[str, Any]] = {}
    
    for row in rows:
        person_id = row.get("person_id") or row.get("id")
        if not person_id:
            continue
        
        if person_id not in by_person_id:
            by_person_id[person_id] = row.copy()
            continue
        
        existing = by_person_id[person_id]
        
        active_start = existing.get("active_first_start_date")
        new_start = row.get("active_first_start_date")
        if new_start and (not active_start or new_start < active_start):
            existing["active_first_start_date"] = new_start
        
        active_end = existing.get("active_last_end_date")
        new_end = row.get("active_last_end_date")
        if new_end and (not active_end or new_end > active_end):
            existing["active_last_end_date"] = new_end
        
        existing_urls = set(_extract_urls(existing.get("evidence_urls") or existing.get("sources") or existing.get("evidence")))
        new_urls = set(_extract_urls(row.get("evidence_urls") or row.get("sources") or row.get("evidence")))
        merged_urls = list(existing_urls | new_urls)[:max_sources]
        existing["evidence_urls"] = merged_urls
    
    return list(by_person_id.values())


def members_list_call_tool_node(state: MembersListMvpState) -> dict[str, Any]:
    tool_input = state.get("tool_input")
    if not tool_input:
        return {"tool_result": None}
    
    limit = tool_input.get("limit", 200)
    offset = 0
    all_rows: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    
    try:
        while True:
            page_input = tool_input.copy()
            page_input["offset"] = offset
            page_input["limit"] = limit
            
            result = members_list(**page_input)
            
            if isinstance(result, dict) and result.get("error"):
                return {"tool_result": result}
            
            page_rows = _coerce_members_list(result)
            if not page_rows:
                break
            
            all_rows.extend(page_rows)
            
            if meta:
                meta_urls = set(_extract_urls(meta.get("evidence_urls") or meta.get("sources")))
                result_urls = set(_extract_urls(result.get("evidence_urls") or result.get("sources")))
                meta["evidence_urls"] = list(meta_urls | result_urls)
            else:
                meta = result.copy()
                if "members" in meta:
                    del meta["members"]
                if "rows" in meta:
                    del meta["rows"]
                if "items" in meta:
                    del meta["items"]
                if "results" in meta:
                    del meta["results"]
            
            if len(page_rows) < limit:
                break
            
            offset += limit
        
        merged_rows = _merge_member_rows(all_rows)
        merged_result = meta.copy()
        merged_result["members"] = merged_rows
        
        return {"tool_result": merged_result}
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


def _format_output_text(
    members: list[dict[str, Any]],
    tool_input: dict[str, Any],
    tool_result: dict[str, Any],
    sources_mode: str,
    max_sources: int,
) -> str:
    party = tool_input.get("party_code") or "?"
    parliament_id = tool_input.get("parliament_id") or "?"
    parliament_name = {
        "NI": "Landtag Niedersachsen",
        "BT": "Deutschen Bundestag",
        "HE": "Hessischen Landtag",
        "BW": "Landtag von Baden-Württemberg",
        "BY": "Bayerischen Landtag",
        "BE": "Abgeordnetenhaus von Berlin",
        "BB": "Landtag Brandenburg",
        "HB": "Bremischen Bürgerschaft",
        "HH": "Hamburgischen Bürgerschaft",
        "MV": "Landtag Mecklenburg-Vorpommern",
        "NW": "Landtag Nordrhein-Westfalen",
        "RP": "Landtag Rheinland-Pfalz",
        "SL": "Landtag des Saarlandes",
        "SN": "Sächsischen Landtag",
        "ST": "Landtag von Sachsen-Anhalt",
        "SH": "Schleswig-Holsteinischen Landtag",
        "TH": "Thüringer Landtag",
    }.get(
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
        if sources_mode == "per-person":
            member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
            if member_sources:
                lines.append(f"  Quellen: {', '.join(member_sources[:max_sources])}")
        elif sources_mode == "top":
            member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
            sources.extend(member_sources[:2])

    if sources_mode == "top":
        meta_sources = _extract_urls(tool_result.get("evidence_urls") or tool_result.get("sources"))
        sources.extend(meta_sources)

        deduped_sources: list[str] = []
        seen: set[str] = set()
        for s in sources:
            if s not in seen:
                deduped_sources.append(s)
                seen.add(s)
            if len(deduped_sources) >= max_sources:
                break

        lines.append("")
        lines.append("Quellen:")
        if not deduped_sources:
            lines.append("- (keine Quellen in der Tool-Antwort enthalten)")
        else:
            for s in deduped_sources:
                lines.append(f"- {s}")

    return "\n".join(lines)


def _format_output_md(
    members: list[dict[str, Any]],
    tool_input: dict[str, Any],
    tool_result: dict[str, Any],
    sources_mode: str,
    max_sources: int,
) -> str:
    party = tool_input.get("party_code") or "?"
    parliament_id = tool_input.get("parliament_id") or "?"
    parliament_name = {
        "NI": "Landtag Niedersachsen",
        "BT": "Deutschen Bundestag",
        "HE": "Hessischen Landtag",
        "BW": "Landtag von Baden-Württemberg",
        "BY": "Bayerischen Landtag",
        "BE": "Abgeordnetenhaus von Berlin",
        "BB": "Landtag Brandenburg",
        "HB": "Bremischen Bürgerschaft",
        "HH": "Hamburgischen Bürgerschaft",
        "MV": "Landtag Mecklenburg-Vorpommern",
        "NW": "Landtag Nordrhein-Westfalen",
        "RP": "Landtag Rheinland-Pfalz",
        "SL": "Landtag des Saarlandes",
        "SN": "Sächsischen Landtag",
        "ST": "Landtag von Sachsen-Anhalt",
        "SH": "Schleswig-Holsteinischen Landtag",
        "TH": "Thüringer Landtag",
    }.get(
        str(parliament_id),
        str(parliament_id),
    )
    from_de = _format_date_de(str(tool_input.get("from_date")) if tool_input.get("from_date") else None)
    to_de = _format_date_de(str(tool_input.get("to_date")) if tool_input.get("to_date") else None)

    lines: list[str] = [
        f"# {party}-Mitglieder im {parliament_name}",
        "",
        f"**Zeitraum:** {from_de}–{to_de}",
        f"**Anzahl:** {len(members)}",
        "",
        "## Mitglieder",
        "",
    ]

    for m in members:
        person_name = (
            m.get("person_name")
            or m.get("name")
            or m.get("person", {}).get("name")
            or "?"
        )
        wikipedia_title = (
            m.get("wikipedia_title")
            or m.get("wikipedia")
            or m.get("person", {}).get("wikipedia_title")
        )
        active_first_start = m.get("active_first_start_date")
        active_last_end = m.get("active_last_end_date")
        raw_first_start = m.get("first_start_date") or m.get("from_date") or m.get("start_date")
        raw_last_end = m.get("last_end_date") or m.get("to_date") or m.get("end_date")
        first_start = active_first_start if active_first_start else raw_first_start
        last_end = active_last_end if active_last_end else raw_last_end
        start_str = first_start if isinstance(first_start, str) and len(first_start) >= 10 else None
        end_str = last_end if isinstance(last_end, str) and len(last_end) >= 10 else None

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
        lines.append(f"- **{person_name}**{title_part} – {date_part}{mandate_note}")

        if sources_mode == "per-person":
            member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
            if member_sources:
                lines.append(f"  - Quellen: {', '.join(member_sources[:max_sources])}")

    if sources_mode == "top":
        sources: list[str] = []
        for m in members:
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
            if len(deduped_sources) >= max_sources:
                break

        if deduped_sources:
            lines.append("")
            lines.append("## Quellen")
            lines.append("")
            for s in deduped_sources:
                lines.append(f"- {s}")

    return "\n".join(lines)


def _format_output_json(
    members: list[dict[str, Any]],
    tool_input: dict[str, Any],
    tool_result: dict[str, Any],
    sources_mode: str,
    max_sources: int,
) -> str:
    import json

    output = tool_result.copy()
    output["members"] = members
    return json.dumps(output, indent=2, ensure_ascii=False, default=str)


def members_list_answer_node(state: MembersListMvpState) -> dict[str, Any]:
    if state.get("answer"):
        return {}

    tool_result = state.get("tool_result") or {}
    tool_input = state.get("tool_input") or {}
    if isinstance(tool_result, dict) and tool_result.get("error"):
        message = str(tool_result.get("error"))
        return {"answer": f"Tool-Fehler: {message}"}
    members = _coerce_members_list(tool_result)

    output_format = state.get("output_format", "text")
    sources_mode = state.get("sources_mode", "top")
    max_sources = state.get("max_sources", 20)

    if output_format == "json":
        answer = _format_output_json(members, tool_input, tool_result, sources_mode, max_sources)
    elif output_format == "md":
        answer = _format_output_md(members, tool_input, tool_result, sources_mode, max_sources)
    else:
        answer = _format_output_text(members, tool_input, tool_result, sources_mode, max_sources)

    return {"answer": answer}


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



