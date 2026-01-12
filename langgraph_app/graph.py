"""LangGraph definition for orchestrator."""

from __future__ import annotations

from datetime import date, datetime
import json
import re
from typing import Any

from langchain_openai import ChatOpenAI
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
from langgraph_app.schemas import ComputedResult, MembersListToolInput, ToolCall, ToolResult, UserIntent
from langgraph_app.settings import OLLAMA_BASE_URL, OLLAMA_MODEL, OPENAI_API_KEY, STRICT_EVIDENCE_DEFAULT, TOOL_BASE_URL
from langgraph_app.tools import members_list, parliaments_coverage


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
    parliament_ids: list[str]
    active_only: bool
    resolved_from_date: str | None
    resolved_to_date: str | None
    tool_base_input: dict[str, Any] | None
    as_of_by_parliament: dict[str, str] | None
    coverage_data: dict[str, Any] | None
    coverage_missing_by_parliament: dict[str, bool] | None


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

ALL_LANDTAGE_IDS = ["BW", "BY", "BE", "BB", "HB", "HH", "HE", "MV", "NI", "NW", "RP", "SL", "SN", "ST", "SH", "TH"]

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


def _detect_active_only(question: str) -> bool:
    """Detect if question asks for active members only."""
    q = question.lower()
    active_keywords = ["aktiv", "aktuell", "heute", "noch", "derzeit", "bestehend"]
    return any(keyword in q for keyword in active_keywords)


def _alias_matches(alias: str, q: str) -> bool:
    """Match aliases safely.

    - Long phrases: substring match.
    - Short codes (<=3 letters): word-boundary match to avoid accidental hits.
    """
    a = alias.strip().lower()
    if not a:
        return False
    if len(a) <= 3 and a.isalpha():
        return re.search(rf"\b{re.escape(a)}\b", q) is not None
    return a in q


def _normalize_parliament_ids(question: str, parliament_ids: list[str] | None) -> list[str]:
    """Normalize / constrain scope.

    Rule of thumb: if deterministic scope resolution yields something, prefer it.
    Otherwise sanitize the provided list.
    """
    deterministic = _resolve_parliament_scope(question)
    if deterministic:
        return deterministic

    allowed = set(ALL_LANDTAGE_IDS + ["BT", "BR"])
    out: list[str] = []
    for pid in (parliament_ids or []):
        if not isinstance(pid, str):
            continue
        pid = pid.strip().upper()
        if pid in allowed and pid not in out:
            out.append(pid)
    return out


def _resolve_parliament_scope(question: str) -> list[str]:
    """Resolve parliament scope from question."""
    q = question.lower()
    
    detected_parliament_ids: set[str] = set()
    mentions_landtag = False
    mentions_bundestag = False
    mentions_bundesrat = False
    
    for alias, pid in PARLIAMENT_ALIASES.items():
        if _alias_matches(alias, q):
            if pid == "BT":
                mentions_bundestag = True
            else:
                detected_parliament_ids.add(pid)
    
    if "landtag" in q or "landtage" in q:
        mentions_landtag = True
    
    if "bundestag" in q:
        mentions_bundestag = True
    
    if "bundesrat" in q:
        mentions_bundesrat = True
    
    if "parlament" in q or "parlamente" in q:
        if not detected_parliament_ids and not mentions_landtag and not mentions_bundestag:
            return ALL_LANDTAGE_IDS + ["BT"]
    
    if detected_parliament_ids:
        result = list(detected_parliament_ids)
        if mentions_bundestag:
            if "BT" not in result:
                result.append("BT")
        return result
    
    if mentions_landtag and not detected_parliament_ids:
        if mentions_bundestag:
            return ALL_LANDTAGE_IDS + ["BT"]
        return ALL_LANDTAGE_IDS.copy()
    
    if mentions_bundestag and not detected_parliament_ids:
        return ["BT"]
    
    if mentions_bundesrat:
        return ["BR"]
    
    return []


def _resolve_stichtag(question: str, from_date: str | None, to_date: str | None) -> str:
    """Resolve stichtag for active_only filter."""
    today = datetime.now().date()
    
    if to_date:
        try:
            return to_date[:10]
        except (ValueError, IndexError):
            pass
    
    if from_date:
        try:
            return from_date[:10]
        except (ValueError, IndexError):
            pass
    
    return today.isoformat()


def _parse_date_range(question: str) -> tuple[str | None, str | None]:
    q = question.lower()
    today = datetime.now().date()
    
    iso_date_match = re.search(r"(\d{4}-\d{2}-\d{2})", q)
    if iso_date_match:
        iso_date_str = iso_date_match.group(1)
        if "am" in q or "zum" in q or "stichtag" in q or "an" in q:
            return iso_date_str, iso_date_str
        try:
            parsed_date = datetime.strptime(iso_date_str, "%Y-%m-%d").date()
            return iso_date_str, iso_date_str
        except ValueError:
            pass
    
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


def _parse_members_list_plan(question: str) -> dict[str, Any]:
    """Parse question into plan with parliament_ids, active_only, and base_input.
    
    DEPRECATED: This deterministic parser is no longer used in the MVP flow.
    Kept for reference/testing purposes only.
    """
    q = question.lower()
    
    party_code: str | None = None
    for alias, code in PARTY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", q):
            party_code = code
            break
    
    parliament_ids = _normalize_parliament_ids(question, _resolve_parliament_scope(question))
    active_only = _detect_active_only(question)
    
    from_date, to_date = _parse_date_range(question)
    
    resolved_from_date: str | None = None
    resolved_to_date: str | None = None
    
    today_iso = datetime.now().date().isoformat()

    if active_only:
        # "Aktiv" bedeutet: Stichtag-Abfrage.
        # - Wenn ein Zeitraum genannt ist, nutzen wir standardmäßig dessen Ende als Stichtag.
        # - Wenn kein Datum genannt ist, default = heute.
        stichtag = _resolve_stichtag(question, from_date, to_date)
        resolved_from_date = stichtag
        resolved_to_date = stichtag
    else:
        # members.list benötigt immer from/to.
        # Wenn kein Zeitraum genannt ist, interpretieren wir "alle" als "bis heute".
        resolved_from_date = from_date or "0001-01-01"
        resolved_to_date = to_date or today_iso
    
    tool_base_input: dict[str, Any] = {
        "limit": 200,
        "offset": 0,
        "strict_evidence": STRICT_EVIDENCE_DEFAULT,
    }
    
    if party_code:
        tool_base_input["party_code"] = party_code
    tool_base_input["from_date"] = resolved_from_date
    tool_base_input["to_date"] = resolved_to_date
    
    return {
        "parliament_ids": parliament_ids,
        "active_only": active_only,
        "resolved_from_date": resolved_from_date,
        "resolved_to_date": resolved_to_date,
        "tool_base_input": tool_base_input,
    }


def members_list_plan_llm_node(state: MembersListMvpState) -> dict[str, Any]:
    """Extract tool input parameters using LLM with new structure."""
    question = state.get("question", "")
    
    system_prompt = """Du extrahierst Parameter aus Nutzerfragen für eine Mitglieder-Abfrage.

WICHTIG:
- Du erfindest KEINE Fakten, du extrahierst nur Parameter aus der Frage
- Output ausschließlich JSON, keine Erklärungen, keine Markdown-Fences
- Erlaubte parliament_id Codes: NI, BT, HE, BW, BY, BE, BB, HB, HH, MV, NW, RP, SL, SN, ST, SH, TH
- Wenn "Landtag" ohne konkretes Bundesland: parliament_ids = alle 16 Landtage
- Wenn "Bundestag": parliament_ids enthält "BT"
- Wenn "Landtag oder Bundestag": parliament_ids = alle 16 Landtage + "BT"
- party_code immer UPPERCASE (SPD, CDU, CSU, GRUENE, FDP, AFD, LINKE, ...)
- Zeitraum immer als ISO YYYY-MM-DD; bei Jahresangaben: 01-01 bis 12-31
- active_only: true wenn "aktiv", "aktuell", "heute", "noch", "derzeit" erwähnt
- Wenn active_only: from_date = to_date = Stichtag (to_date aus Zeitraum, sonst heute)
- Wenn etwas nicht eindeutig ist: Felder als null lassen

Output-Format (JSON):
{
  "parliament_ids": ["NI", "BT"] | null,
  "party_code": "SPD" | null,
  "from_date": "2014-01-01" | null,
  "to_date": "2020-12-31" | null,
  "active_only": false,
  "limit": 200,
  "offset": 0,
  "strict_evidence": true
}"""

    try:
        llm = ChatOpenAI(
            base_url=OLLAMA_BASE_URL,
            model=OLLAMA_MODEL,
            api_key=OPENAI_API_KEY,
            temperature=0,
        )
        
        response = llm.invoke([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question}
        ])
        
        content = response.content.strip()
        
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        data = json.loads(content)
        
        parliament_ids = data.get("parliament_ids")
        if isinstance(parliament_ids, str):
            parliament_ids = [parliament_ids]
        elif not isinstance(parliament_ids, list):
            parliament_ids = None

        # Guardrail: prefer deterministic scope if implied by the question (e.g. "alle Landtage").
        parliament_ids = _normalize_parliament_ids(question, parliament_ids)
        
        party_code = data.get("party_code")
        if isinstance(party_code, str):
            party_code = party_code.strip().upper()
        active_only = bool(data.get("active_only", False)) or _detect_active_only(question)
        from_date = data.get("from_date")
        to_date = data.get("to_date")

        explicit_as_of = to_date or from_date
        
        if active_only:
            if not explicit_as_of:
                try:
                    coverage_result = parliaments_coverage(parliament_ids)
                    data_as_of_str = coverage_result.get("data_as_of")
                    if not data_as_of_str:
                        from datetime import date
                        data_as_of_str = date.today().isoformat()
                    
                    base_today = date.fromisoformat(data_as_of_str)
                    coverage_rows = coverage_result.get("rows", [])
                    coverage_by_pid: dict[str, dict[str, Any]] = {
                        row.get("parliament_id"): row for row in coverage_rows
                    }
                    
                    as_of_by_parliament: dict[str, str] = {}
                    coverage_missing_by_parliament: dict[str, bool] = {}
                    
                    for pid in parliament_ids:
                        coverage = coverage_by_pid.get(pid, {})
                        max_end_str = coverage.get("max_end")
                        mandates_count = coverage.get("mandates_count", 0)
                        
                        coverage_missing = False
                        if not coverage or mandates_count == 0:
                            as_of_by_parliament[pid] = base_today.isoformat()
                            coverage_missing = True
                        elif max_end_str:
                            try:
                                max_end_date = date.fromisoformat(max_end_str[:10])
                                as_of_date = min(base_today, max_end_date)
                                as_of_by_parliament[pid] = as_of_date.isoformat()
                            except (ValueError, TypeError):
                                as_of_by_parliament[pid] = base_today.isoformat()
                                coverage_missing = True
                        else:
                            as_of_by_parliament[pid] = base_today.isoformat()
                            coverage_missing = True
                        
                        coverage_missing_by_parliament[pid] = coverage_missing
                    
                    tool_base_input: dict[str, Any] = {
                        "limit": data.get("limit", 200),
                        "offset": data.get("offset", 0),
                        "strict_evidence": data.get("strict_evidence", STRICT_EVIDENCE_DEFAULT),
                    }
                    
                    if party_code:
                        tool_base_input["party_code"] = party_code
                    
                    resolved_from_date = None
                    resolved_to_date = None
                    
                    return {
                        "parliament_ids": parliament_ids,
                        "active_only": active_only,
                        "resolved_from_date": resolved_from_date,
                        "resolved_to_date": resolved_to_date,
                        "as_of_by_parliament": as_of_by_parliament,
                        "coverage_missing_by_parliament": coverage_missing_by_parliament,
                        "coverage_data": coverage_result,
                        "tool_base_input": tool_base_input,
                        "answer": None,
                    }
                except Exception as e:
                    import sys
                    error_msg = f"Coverage lookup failed: {e}"
                    print(f"[ERROR] {error_msg}", file=sys.stderr)
                    raise RuntimeError(
                        f"Coverage-Abfrage fehlgeschlagen. Tool-Gateway erreichbar? "
                        f"({TOOL_BASE_URL}). Fehler: {e}"
                    ) from e
            else:
                stichtag = explicit_as_of
                if isinstance(stichtag, str):
                    stichtag = stichtag[:10]
                else:
                    from datetime import date
                    stichtag = date.today().isoformat()
                resolved_from_date = stichtag
                resolved_to_date = stichtag
        else:
            from datetime import date
            today_iso = date.today().isoformat()
            resolved_from_date = from_date or "0001-01-01"
            resolved_to_date = to_date or today_iso
        
        tool_base_input: dict[str, Any] = {
            "limit": data.get("limit", 200),
            "offset": data.get("offset", 0),
            "strict_evidence": data.get("strict_evidence", STRICT_EVIDENCE_DEFAULT),
        }
        
        if party_code:
            tool_base_input["party_code"] = party_code
        tool_base_input["from_date"] = resolved_from_date
        tool_base_input["to_date"] = resolved_to_date
        
        missing = []
        if not parliament_ids:
            missing.append("Parlament")
        if not party_code:
            missing.append("Partei")
        
        if missing:
            return {
                "parliament_ids": [],
                "active_only": False,
                "resolved_from_date": None,
                "resolved_to_date": None,
                "tool_base_input": None,
                "answer": f"Welche {' / '.join(missing)} soll ich abfragen? Bitte spezifizieren Sie: {' / '.join(missing)}.",
            }
        
        return {
            "parliament_ids": parliament_ids,
            "active_only": active_only,
            "resolved_from_date": resolved_from_date,
            "resolved_to_date": resolved_to_date,
            "tool_base_input": tool_base_input,
            "answer": None,
        }
    except Exception as e:
        import sys
        error_msg = str(e)
        print(f"[DEBUG] LLM parsing failed: {error_msg}", file=sys.stderr)
        return {
            "parliament_ids": [],
            "active_only": False,
            "resolved_from_date": None,
            "resolved_to_date": None,
            "tool_base_input": None,
            "answer": (
                "LLM-Fehler: Parameter konnten nicht extrahiert werden "
                "(Ollama nicht erreichbar oder ungültiges JSON). "
                "Bitte Ollama prüfen oder Frage präzisieren."
            ),
        }


def members_list_plan_node(state: MembersListMvpState) -> dict[str, Any]:
    """Plan node using LLM-only for parameter extraction.
    
    This node always uses the LLM (Ollama) for parameter extraction.
    If the LLM fails or returns invalid JSON, a clear error message is returned.
    """
    result = members_list_plan_llm_node(state)
    
    if result.get("answer"):
        return result
    
    if not result.get("parliament_ids") or not result.get("tool_base_input"):
        return {
            "parliament_ids": [],
            "active_only": False,
            "resolved_from_date": None,
            "resolved_to_date": None,
            "tool_base_input": None,
            "answer": (
                "LLM-Fehler: Parameter konnten nicht extrahiert werden "
                "(Ollama nicht erreichbar oder ungültiges JSON). "
                "Bitte Ollama prüfen oder Frage präzisieren."
            ),
        }
    
    return result


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
    """Call members.list for multiple parliament_ids and aggregate results."""
    parliament_ids = state.get("parliament_ids", [])
    tool_base_input = state.get("tool_base_input")
    as_of_by_parliament = state.get("as_of_by_parliament")
    coverage_data = state.get("coverage_data")
    
    if not parliament_ids or not tool_base_input:
        return {"tool_result": None}
    
    if not tool_base_input.get("party_code"):
        return {"tool_result": {"error": "party_code is required"}}
    
    results_by_parliament: dict[str, list[dict[str, Any]]] = {}
    errors_by_parliament: dict[str, str] = {}
    all_meta_urls: set[str] = set()
    
    limit = tool_base_input.get("limit", 200)
    
    coverage_by_pid: dict[str, dict[str, Any]] = {}
    if coverage_data:
        coverage_rows = coverage_data.get("rows", [])
        coverage_by_pid = {
            row.get("parliament_id"): row for row in coverage_rows
        }
    
    for parliament_id in parliament_ids:
        coverage = coverage_by_pid.get(parliament_id, {})
        mandates_count = coverage.get("mandates_count", 0)
        
        if mandates_count == 0:
            errors_by_parliament[parliament_id] = "NO_DATA_IMPORTED"
            continue
        
        try:
            offset = 0
            parliament_rows: list[dict[str, Any]] = []
            parliament_meta: dict[str, Any] = {}
            
            as_of_for_parliament = None
            if as_of_by_parliament and parliament_id in as_of_by_parliament:
                as_of_for_parliament = as_of_by_parliament[parliament_id]
            else:
                as_of_for_parliament = tool_base_input.get("from_date") or tool_base_input.get("to_date")
            
            while True:
                page_input = tool_base_input.copy()
                page_input["parliament_id"] = parliament_id
                page_input["offset"] = offset
                page_input["limit"] = limit
                
                if as_of_for_parliament:
                    page_input["from_date"] = as_of_for_parliament
                    page_input["to_date"] = as_of_for_parliament
                
                result = members_list(**page_input)
                
                if isinstance(result, dict) and result.get("error"):
                    errors_by_parliament[parliament_id] = str(result.get("error"))
                    break
                
                page_rows = _coerce_members_list(result)
                if not page_rows:
                    break
                
                parliament_rows.extend(page_rows)
                
                if not parliament_meta:
                    parliament_meta = result.copy()
                    for key in ("members", "rows", "items", "results"):
                        if key in parliament_meta:
                            del parliament_meta[key]
                
                result_urls = set(_extract_urls(result.get("evidence_urls") or result.get("sources")))
                all_meta_urls.update(result_urls)
                
                if len(page_rows) < limit:
                    break
                
                offset += limit
            
            if parliament_rows:
                merged_rows = _merge_member_rows(parliament_rows)
                results_by_parliament[parliament_id] = merged_rows
        
        except Exception as e:
            errors_by_parliament[parliament_id] = str(e)
    
    tool_result: dict[str, Any] = {
        "results_by_parliament": results_by_parliament,
        "errors_by_parliament": errors_by_parliament,
        "evidence_urls": list(all_meta_urls),
    }
    
    return {"tool_result": tool_result}


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


PARLIAMENT_NAMES: dict[str, str] = {
    "NI": "Landtag Niedersachsen",
    "BT": "Deutscher Bundestag",
    "HE": "Hessischer Landtag",
    "BW": "Landtag von Baden-Württemberg",
    "BY": "Bayerischer Landtag",
    "BE": "Abgeordnetenhaus von Berlin",
    "BB": "Landtag Brandenburg",
    "HB": "Bremische Bürgerschaft",
    "HH": "Hamburgische Bürgerschaft",
    "MV": "Landtag Mecklenburg-Vorpommern",
    "NW": "Landtag Nordrhein-Westfalen",
    "RP": "Landtag Rheinland-Pfalz",
    "SL": "Landtag des Saarlandes",
    "SN": "Sächsischer Landtag",
    "ST": "Landtag von Sachsen-Anhalt",
    "SH": "Schleswig-Holsteinischer Landtag",
    "TH": "Thüringer Landtag",
}


def _format_scope_description(parliament_ids: list[str]) -> str:
    """Format scope description from parliament_ids."""
    if not parliament_ids:
        return "?"
    
    if len(parliament_ids) == 1:
        return PARLIAMENT_NAMES.get(parliament_ids[0], parliament_ids[0])
    
    if len(parliament_ids) == 17 and "BT" in parliament_ids:
        return "alle Landtage und Bundestag"
    
    if len(parliament_ids) == 16 and "BT" not in parliament_ids:
        return "alle Landtage"
    
    if "BT" in parliament_ids:
        return f"{len(parliament_ids)} Parlamente (inkl. Bundestag)"
    
    return f"{len(parliament_ids)} Landtage"


def _format_output_text_grouped(
    results_by_parliament: dict[str, list[dict[str, Any]]],
    tool_base_input: dict[str, Any],
    tool_result: dict[str, Any],
    parliament_ids: list[str],
    active_only: bool,
    resolved_from_date: str | None,
    resolved_to_date: str | None,
    sources_mode: str,
    max_sources: int,
    as_of_by_parliament: dict[str, str] | None = None,
) -> str:
    """Format grouped output by parliament."""
    party = tool_base_input.get("party_code") or "?"
    scope_desc = _format_scope_description(parliament_ids)
    
    total_count = sum(len(members) for members in results_by_parliament.values())
    
    from_de = _format_date_de(resolved_from_date)
    to_de = _format_date_de(resolved_to_date)
    
    today_iso = datetime.now().date().isoformat()
    if active_only:
        date_part = f"Stichtag: {from_de}"
    elif resolved_from_date == "0001-01-01":
        if resolved_to_date and resolved_to_date[:10] == today_iso:
            date_part = "alle verfügbaren Daten bis heute"
        else:
            date_part = f"alle verfügbaren Daten bis {to_de}"
    elif resolved_from_date and resolved_to_date:
        date_part = f"{from_de}–{to_de}"
    else:
        date_part = "alle Zeiträume"
    
    headline = f"{party}-Mitglieder ({scope_desc}) [{date_part}]"
    lines: list[str] = [headline, f"Gesamtanzahl: {total_count}", ""]
    
    for parliament_id in sorted(parliament_ids):
        members = results_by_parliament.get(parliament_id, [])
        if not members:
            continue
        
        parliament_name = PARLIAMENT_NAMES.get(parliament_id, parliament_id)
        as_of_str = ""
        if as_of_by_parliament and parliament_id in as_of_by_parliament:
            as_of_de = _format_date_de(as_of_by_parliament[parliament_id])
            as_of_str = f" (Stichtag: {as_of_de})"
        lines.append(f"{parliament_name}: {len(members)}{as_of_str}")
        
        for m in members:
            lines.append(f"  {format_member_row(m)}")
            if sources_mode == "per-person":
                member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
                if member_sources:
                    lines.append(f"    Quellen: {', '.join(member_sources[:2])}")
        
        lines.append("")
    
    if sources_mode == "top":
        all_sources: list[str] = []
        for members in results_by_parliament.values():
            for m in members:
                member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
                all_sources.extend(member_sources[:2])
        
        meta_sources = _extract_urls(tool_result.get("evidence_urls") or tool_result.get("sources"))
        all_sources.extend(meta_sources)
        
        deduped_sources: list[str] = []
        seen: set[str] = set()
        for s in all_sources:
            if s not in seen:
                deduped_sources.append(s)
                seen.add(s)
            if len(deduped_sources) >= max_sources:
                break
        
        if deduped_sources:
            lines.append("Quellen:")
            for s in deduped_sources:
                lines.append(f"- {s}")
    
    return "\n".join(lines)


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
    from_raw = str(tool_input.get("from_date")) if tool_input.get("from_date") else None
    to_raw = str(tool_input.get("to_date")) if tool_input.get("to_date") else None
    from_de = _format_date_de(from_raw)
    to_de = _format_date_de(to_raw)

    today_iso = datetime.now().date().isoformat()
    if from_raw == "0001-01-01":
        if to_raw and to_raw[:10] == today_iso:
            headline = f"{party}-Mitglieder im {parliament_name} (alle verfügbaren Daten bis heute)"
        else:
            headline = f"{party}-Mitglieder im {parliament_name} (alle verfügbaren Daten bis {to_de})"
    elif from_raw and to_raw and from_raw[:10] == to_raw[:10]:
        headline = f"{party}-Mitglieder im {parliament_name} (Stichtag: {from_de})"
    else:
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
    from_raw = str(tool_input.get("from_date")) if tool_input.get("from_date") else None
    to_raw = str(tool_input.get("to_date")) if tool_input.get("to_date") else None
    from_de = _format_date_de(from_raw)
    to_de = _format_date_de(to_raw)

    today_iso = datetime.now().date().isoformat()
    if from_raw == "0001-01-01":
        if to_raw and to_raw[:10] == today_iso:
            date_label = "alle verfügbaren Daten bis heute"
        else:
            date_label = f"alle verfügbaren Daten bis {to_de}"
    elif from_raw and to_raw and from_raw[:10] == to_raw[:10]:
        date_label = f"Stichtag: {from_de}"
    else:
        date_label = f"{from_de}–{to_de}"

    lines: list[str] = [
        f"# {party}-Mitglieder im {parliament_name}",
        "",
        f"**Zeitraum:** {date_label}",
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


def _format_output_md_grouped(
    results_by_parliament: dict[str, list[dict[str, Any]]],
    tool_base_input: dict[str, Any],
    tool_result: dict[str, Any],
    parliament_ids: list[str],
    active_only: bool,
    resolved_from_date: str | None,
    resolved_to_date: str | None,
    sources_mode: str,
    max_sources: int,
) -> str:
    """Format grouped markdown output by parliament."""
    party = tool_base_input.get("party_code") or "?"
    scope_desc = _format_scope_description(parliament_ids)
    
    total_count = sum(len(members) for members in results_by_parliament.values())
    
    from_de = _format_date_de(resolved_from_date)
    to_de = _format_date_de(resolved_to_date)
    
    today_iso = datetime.now().date().isoformat()
    if active_only:
        date_part = f"Stichtag: {from_de}"
    elif resolved_from_date == "0001-01-01":
        if resolved_to_date and resolved_to_date[:10] == today_iso:
            date_part = "alle verfügbaren Daten bis heute"
        else:
            date_part = f"alle verfügbaren Daten bis {to_de}"
    elif resolved_from_date and resolved_to_date:
        date_part = f"{from_de}–{to_de}"
    else:
        date_part = "alle Zeiträume"
    
    lines: list[str] = [
        f"# {party}-Mitglieder ({scope_desc})",
        "",
        f"**{date_part}**",
        f"**Gesamtanzahl:** {total_count}",
        "",
    ]
    
    for parliament_id in sorted(parliament_ids):
        members = results_by_parliament.get(parliament_id, [])
        if not members:
            continue
        
        parliament_name = PARLIAMENT_NAMES.get(parliament_id, parliament_id)
        lines.append(f"## {parliament_name} ({parliament_id}): {len(members)}")
        lines.append("")
        
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
            
            title_part = f" ({wikipedia_title})" if isinstance(wikipedia_title, str) and wikipedia_title else ""
            lines.append(f"- **{person_name}**{title_part} – {date_part}")
            
            if sources_mode == "per-person":
                member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
                if member_sources:
                    lines.append(f"  - Quellen: {', '.join(member_sources[:2])}")
        
        lines.append("")
    
    if sources_mode == "top":
        all_sources: list[str] = []
        for members in results_by_parliament.values():
            for m in members:
                member_sources = _extract_urls(m.get("evidence_urls") or m.get("sources") or m.get("evidence"))
                all_sources.extend(member_sources[:2])
        
        meta_sources = _extract_urls(tool_result.get("evidence_urls") or tool_result.get("sources"))
        all_sources.extend(meta_sources)
        
        deduped_sources: list[str] = []
        seen: set[str] = set()
        for s in all_sources:
            if s not in seen:
                deduped_sources.append(s)
                seen.add(s)
            if len(deduped_sources) >= max_sources:
                break
        
        if deduped_sources:
            lines.append("## Quellen")
            lines.append("")
            for s in deduped_sources:
                lines.append(f"- {s}")
    
    return "\n".join(lines)


def _format_output_json_grouped(
    results_by_parliament: dict[str, list[dict[str, Any]]],
    tool_base_input: dict[str, Any],
    tool_result: dict[str, Any],
    parliament_ids: list[str],
    active_only: bool,
    resolved_from_date: str | None,
    resolved_to_date: str | None,
    sources_mode: str,
    max_sources: int,
) -> str:
    """Format grouped JSON output by parliament."""
    import json
    
    output: dict[str, Any] = {
        "party_code": tool_base_input.get("party_code"),
        "parliament_ids": parliament_ids,
        "active_only": active_only,
        "resolved_from_date": resolved_from_date,
        "resolved_to_date": resolved_to_date,
        "results_by_parliament": results_by_parliament,
        "total_count": sum(len(members) for members in results_by_parliament.values()),
        "errors_by_parliament": tool_result.get("errors_by_parliament", {}),
        "evidence_urls": tool_result.get("evidence_urls", []),
    }
    
    return json.dumps(output, indent=2, ensure_ascii=False, default=str)


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
    tool_base_input = state.get("tool_base_input") or {}
    parliament_ids = state.get("parliament_ids", [])
    active_only = state.get("active_only", False)
    resolved_from_date = state.get("resolved_from_date")
    resolved_to_date = state.get("resolved_to_date")
    coverage_data = state.get("coverage_data")
    as_of_by_parliament = state.get("as_of_by_parliament", {})
    
    if isinstance(tool_result, dict) and tool_result.get("error"):
        message = str(tool_result.get("error"))
        return {"answer": f"Tool-Fehler: {message}"}
    
    results_by_parliament = tool_result.get("results_by_parliament", {})
    errors_by_parliament = tool_result.get("errors_by_parliament", {})
    
    coverage_by_pid: dict[str, dict[str, Any]] = {}
    if coverage_data:
        coverage_rows = coverage_data.get("rows", [])
        coverage_by_pid = {
            row.get("parliament_id"): row for row in coverage_rows
        }
    
    if not results_by_parliament:
        if errors_by_parliament:
            error_parts = []
            for pid, err in errors_by_parliament.items():
                if err == "NO_DATA_IMPORTED":
                    parliament_name = PARLIAMENT_NAMES.get(pid, pid)
                    error_parts.append(f"für {parliament_name} keine Daten importiert")
                else:
                    error_parts.append(f"{pid}: {err}")
            error_msg = "; ".join(error_parts)
            return {"answer": f"Fehler bei der Abfrage: {error_msg}"}

        if active_only and resolved_from_date and resolved_to_date and resolved_from_date[:10] == resolved_to_date[:10]:
            stichtag_de = _format_date_de(resolved_from_date)
            scope_desc = _format_scope_description(parliament_ids)
            
            hints = []
            for pid in parliament_ids:
                coverage = coverage_by_pid.get(pid, {})
                max_end = coverage.get("max_end")
                mandates_count = coverage.get("mandates_count", 0)
                
                if mandates_count == 0:
                    parliament_name = PARLIAMENT_NAMES.get(pid, pid)
                    hints.append(f"für {parliament_name} keine Daten importiert")
                elif max_end and max_end[:10] < resolved_from_date[:10]:
                    max_end_de = _format_date_de(max_end)
                    parliament_name = PARLIAMENT_NAMES.get(pid, pid)
                    hints.append(f"Datenstand für {parliament_name} endet am {max_end_de}")
            
            answer = f"Keine Ergebnisse gefunden ({scope_desc}, Stichtag: {stichtag_de})."
            if hints:
                answer += f"\nHinweis: {'; '.join(hints)}."
            
            return {"answer": answer}

        return {"answer": "Keine Ergebnisse gefunden."}

    output_format = state.get("output_format", "text")
    sources_mode = state.get("sources_mode", "top")
    max_sources = state.get("max_sources", 20)
    as_of_by_parliament = state.get("as_of_by_parliament")

    coverage_missing_by_parliament = state.get("coverage_missing_by_parliament")
    
    if output_format == "json":
        answer = _format_output_json_grouped(results_by_parliament, tool_base_input, tool_result, parliament_ids, active_only, resolved_from_date, resolved_to_date, sources_mode, max_sources)
    elif output_format == "md":
        answer = _format_output_md_grouped(results_by_parliament, tool_base_input, tool_result, parliament_ids, active_only, resolved_from_date, resolved_to_date, sources_mode, max_sources)
    else:
        answer = _format_output_text_grouped(results_by_parliament, tool_base_input, tool_result, parliament_ids, active_only, resolved_from_date, resolved_to_date, sources_mode, max_sources, as_of_by_parliament, coverage_data, coverage_missing_by_parliament)

    return {"answer": answer}


def create_members_list_mvp_graph() -> StateGraph:
    workflow = StateGraph(MembersListMvpState)
    workflow.add_node("plan", members_list_plan_node)
    workflow.add_node("call_tool", members_list_call_tool_node)
    workflow.add_node("answer", members_list_answer_node)

    workflow.set_entry_point("plan")

    def _route_after_plan(state: MembersListMvpState) -> str:
        if state.get("answer"):
            return "answer"
        parliament_ids = state.get("parliament_ids", [])
        tool_base_input = state.get("tool_base_input")
        if not parliament_ids or not tool_base_input:
            return "answer"
        return "call_tool"

    workflow.add_conditional_edges("plan", _route_after_plan)
    workflow.add_edge("call_tool", "answer")
    workflow.add_edge("answer", END)
    return workflow.compile()



