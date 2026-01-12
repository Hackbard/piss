"""Response Composer Node: LLM optional, facts from data only."""

import json
from typing import Any

from langchain_openai import ChatOpenAI

from langgraph_app.config import OrchestratorConfig
from langgraph_app.nodes.policy_guard import PolicyDecision
from langgraph_app.schemas import ComputedResult, OutputFormat, UserIntent


RESPONSE_COMPOSER_SYSTEM_PROMPT = """Du formulierst Antworten basierend auf berechneten Daten.

WICHTIG:
- Neutraler Ton: keine Wertungen, keine emotionalen oder parteiischen Formulierungen
- Verwende NUR Fakten aus den bereitgestellten Daten
- Füge KEINE neuen Fakten hinzu
- Jede Antwort muss Evidence-URLs enthalten
- Zeitspannen müssen explizite Datumswerte enthalten
- Bei Prozentwerten: kurzer Hinweis auf Berechnungsformel
- Keine Spekulation: keine ungestützten Behauptungen, keine Halluzinationen
- Wenn nach Bewertungen gefragt wird: nur datenbasierte Alternativen anbieten, klar als Interpretation kennzeichnen

Format:
- BULLETS: Aufzählung mit Evidence-URLs
- TABLE: Tabellenformat (wenn möglich)
- JSON: Strukturiertes JSON

Antworte in der gewünschten Sprache des Benutzers."""


def format_sectioned_response(
    intent: UserIntent,
    computed: ComputedResult | None,
    tool_results: list[dict[str, Any]],
    safe_plan: dict[str, Any],
    config: OrchestratorConfig,
) -> str:
    """Format response with structured sections (deterministic template)."""
    sections: list[str] = []

    scope = safe_plan.get("scope", {})
    sources = safe_plan.get("sources", [])
    computations = safe_plan.get("computations", {})

    if config.response_sections:
        sections.append("## Ergebnis\n")

        if computed:
            if computed.grouped_data:
                sections.append("Gruppierte Daten verfügbar.\n")
            if computed.computed_metrics:
                sections.append("Berechnete Metriken verfügbar.\n")

        sections.append("\n## Datenbasis\n")
        data_basis_parts: list[str] = []
        if scope.get("parliament_id"):
            data_basis_parts.append(f"Parlament: {scope['parliament_id']}")
        if scope.get("legislature_id"):
            data_basis_parts.append(f"Legislatur: {scope['legislature_id']}")
        if scope.get("from_date"):
            data_basis_parts.append(f"Von: {scope['from_date']}")
        if scope.get("to_date"):
            data_basis_parts.append(f"Bis: {scope['to_date']}")

        if data_basis_parts:
            sections.append("; ".join(data_basis_parts) + "\n")
        else:
            sections.append("Keine spezifischen Filter angegeben.\n")

        if computed and computed.grouped_data:
            sections.append("\n## Details\n")
            for group_key, group_items in computed.grouped_data.items():
                sections.append(f"\n### {group_key}\n")
                if isinstance(group_items, dict):
                    for sub_key, sub_items in group_items.items():
                        if isinstance(sub_items, list):
                            sections.append(f"{sub_key}: {len(sub_items)} Einträge\n")
                elif isinstance(group_items, list):
                    sections.append(f"{len(group_items)} Einträge\n")

        if computations:
            sections.append("\n## Berechnungen\n")
            for metric_name, metric_value in computations.items():
                if isinstance(metric_value, dict):
                    sections.append(f"\n### {metric_name}\n")
                    for key, val in metric_value.items():
                        sections.append(f"- {key}: {val}\n")
                else:
                    sections.append(f"- {metric_name}: {metric_value}\n")

        if sources:
            sections.append("\n## Quellen\n")
            for i, url in enumerate(sources[: config.max_sources], 1):
                sections.append(f"{i}. {url}\n")
        else:
            sections.append("\n## Quellen\n")
            sections.append("Keine Quellen verfügbar.\n")

    else:
        if computed and computed.computed_metrics:
            sections.append("Berechnete Metriken:\n")
            for metric_name, metric_value in computed.computed_metrics.items():
                if isinstance(metric_value, dict):
                    for key, val in metric_value.items():
                        sections.append(f"- {key}: {val}\n")
                else:
                    sections.append(f"- {metric_name}: {metric_value}\n")

        if sources:
            sections.append("\nQuellen:\n")
            for url in sources[: config.max_sources]:
                sections.append(f"- {url}\n")

    return "".join(sections)


def format_debug_output(
    state: dict[str, Any],
    config: OrchestratorConfig,
) -> str:
    """Format debug output for explain queries."""
    if not config.debug_explain_queries:
        return ""

    debug_sections: list[str] = []
    debug_sections.append("\n\n---\n")
    debug_sections.append("## [Developer Debug] Explain Query\n")

    intent = state.get("intent")
    if intent:
        debug_sections.append("\n### Intent\n")
        debug_sections.append(f"Type: {intent.intent_type.value}\n")
        debug_sections.append(f"Filters: {json.dumps(intent.filters, indent=2, ensure_ascii=False, default=str)}\n")
        debug_sections.append(f"Needs Clarification: {intent.needs_clarification}\n")

    tool_calls = state.get("tool_calls", [])
    if tool_calls:
        debug_sections.append("\n### Tool Calls\n")
        for i, tc in enumerate(tool_calls, 1):
            tool_name = tc.get("tool_name", "unknown")
            params = tc.get("params", {})
            debug_sections.append(f"\n{i}. {tool_name}\n")
            debug_sections.append(f"   Params: {json.dumps(params, indent=2, ensure_ascii=False, default=str)}\n")

    tool_results = state.get("tool_results", [])
    if tool_results:
        debug_sections.append("\n### Tool Results Summary\n")
        for i, tr in enumerate(tool_results, 1):
            tool_name = tr.get("tool_name", "unknown")
            request_id = tr.get("request_id")
            data = tr.get("data", {})
            error = tr.get("error")

            debug_sections.append(f"\n{i}. {tool_name}\n")
            if request_id:
                debug_sections.append(f"   Request ID: {request_id}\n")
            if error:
                debug_sections.append(f"   Error: {error}\n")
            else:
                if tool_name == "mandates.search":
                    rows = data.get("rows", [])
                    debug_sections.append(f"   Rows: {len(rows)}\n")
                elif tool_name == "legislature.stats":
                    total_seats = data.get("total_seats")
                    party_seats = data.get("party_seats", {})
                    debug_sections.append(f"   Total Seats: {total_seats}\n")
                    debug_sections.append(f"   Parties: {len(party_seats)}\n")

            if config.debug_include_raw_tool_payloads and i == 1:
                redacted_data = json.dumps(data, indent=2, ensure_ascii=False, default=str)
                if len(redacted_data) > 1000:
                    redacted_data = redacted_data[:1000] + "... [truncated]"
                debug_sections.append(f"   Raw Payload (example, redacted):\n{redacted_data}\n")

    computed = state.get("computed", {})
    if computed:
        debug_sections.append("\n### Computed Results\n")
        debug_sections.append(json.dumps(computed, indent=2, ensure_ascii=False, default=str) + "\n")

    policy_warnings = state.get("policy_warnings", [])
    if policy_warnings:
        debug_sections.append("\n### Policy Warnings\n")
        for warning in policy_warnings:
            debug_sections.append(f"- {warning}\n")

    return "".join(debug_sections)


def response_composer_node(state: dict[str, Any], config: OrchestratorConfig) -> dict[str, Any]:
    """LangGraph node: Compose final response from computed data."""
    intent: UserIntent | None = state.get("intent")
    tool_results = state.get("tool_results", [])
    computed_dict = state.get("computed")
    computed: ComputedResult | None = ComputedResult(**computed_dict) if computed_dict else None
    policy_decision = state.get("policy_decision")
    policy_warnings = state.get("policy_warnings", [])
    safe_plan = state.get("safe_answer_plan", {})

    if not intent:
        return {"final_answer": "Keine Intent gefunden."}

    if intent.needs_clarification:
        clarifying = intent.clarifying_question or "Bitte spezifizieren Sie Ihre Frage."
        return {"final_answer": clarifying}

    evidence_gate_passed = state.get("evidence_gate_passed", True)
    if not evidence_gate_passed:
        error_msg = state.get("evidence_gate_error", "Cannot answer reliably: missing evidence.")
        return {"final_answer": error_msg}

    if policy_decision == PolicyDecision.REFUSE_RANKING:
        alternative = safe_plan.get("alternative", "Ich kann keine wertenden Rankings geben.")
        return {"final_answer": alternative}

    if policy_decision == PolicyDecision.NEEDS_CLARIFICATION:
        clarifying = safe_plan.get("clarifying_question", "Bitte spezifizieren Sie Ihre Frage.")
        return {"final_answer": clarifying}

    if intent.output_format == OutputFormat.JSON:
        result = {
            "tool_results": tool_results,
            "computed": computed.model_dump() if computed else {},
        }
        if config.debug_explain_queries:
            result["debug"] = {
                "intent": intent.model_dump(),
                "policy_decision": policy_decision,
                "policy_warnings": policy_warnings,
            }
        return {
            "final_answer": json.dumps(result, indent=2, ensure_ascii=False, default=str)
        }

    if config.response_sections and not config.debug_explain_queries:
        answer = format_sectioned_response(intent, computed, tool_results, safe_plan, config)
    else:
        prompt_data = {
            "intent": intent.model_dump(),
            "tool_results": tool_results,
            "computed": computed.model_dump() if computed else {},
            "safe_answer_plan": safe_plan,
        }

        if intent.output_format == OutputFormat.TABLE:
            prompt = f"""{RESPONSE_COMPOSER_SYSTEM_PROMPT}

Daten:
{json.dumps(prompt_data, indent=2, ensure_ascii=False, default=str)}

Formatiere als Tabelle mit Evidence-URLs."""
        else:
            prompt = f"""{RESPONSE_COMPOSER_SYSTEM_PROMPT}

Daten:
{json.dumps(prompt_data, indent=2, ensure_ascii=False, default=str)}

Formatiere als Aufzählung mit Evidence-URLs."""

        try:
            llm = ChatOpenAI(
                base_url=config.ollama_base_url,
                model=config.ollama_model,
                temperature=0.1,
                max_tokens=512,
                api_key=config.openai_api_key,
            )

            response = llm.invoke(prompt)
            answer = response.content.strip()
        except Exception as e:
            answer = f"Fehler beim Generieren der Antwort: {e}\n\nRohdaten:\n{json.dumps(prompt_data, indent=2, ensure_ascii=False, default=str)}"

    debug_output = format_debug_output(state, config)
    final_answer = answer + debug_output

    return {"final_answer": final_answer}



