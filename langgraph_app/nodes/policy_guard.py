"""Policy Guard Node: Deterministic policy enforcement (no LLM)."""

import re
from enum import Enum
from typing import Any

from langgraph_app.config import OrchestratorConfig, PolicyMode
from langgraph_app.schemas import ComputedResult, UserIntent


class PolicyDecision(str, Enum):
    """Policy decision enum values."""

    PASS = "PASS"
    REWRITE_REQUIRED = "REWRITE_REQUIRED"
    REFUSE_RANKING = "REFUSE_RANKING"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"


VALUATIVE_ADJECTIVES = [
    "skandalös",
    "korrupt",
    "schlimm",
    "schlechter",
    "schlecht",
    "besser",
    "gut",
    "beste",
    "schlechteste",
    "schlimmste",
    "schlechtester",
    "bester",
    "verwerflich",
    "unverantwortlich",
    "katastrophal",
    "exzellent",
    "perfekt",
    "miserabel",
]

NORMATIVE_PHRASES = [
    "wähle",
    "vermeide",
    "solltest",
    "müsstest",
    "sollte wählen",
    "sollte vermeiden",
    "wähle nicht",
]

RANKING_PATTERNS = [
    r"beste\s+\w+",
    r"schlechteste\s+\w+",
    r"schlimmste\s+\w+",
    r"bester\s+\w+",
    r"schlechtester\s+\w+",
    r"wer\s+ist\s+(am\s+)?(besten|schlechtesten|schlimmsten)",
    r"welche\s+(ist|sind)\s+(die\s+)?(beste|schlechteste|schlimmste)",
    r"ranking",
    r"top\s+\d+",
    r"worst\s+\d+",
]


def extract_evidence_urls(tool_results: list[dict[str, Any]]) -> list[str]:
    """Extract and deduplicate evidence URLs from tool results."""
    urls: set[str] = set()

    for tr in tool_results:
        tool_name = tr.get("tool_name", "")
        data = tr.get("data", {})

        if tool_name == "mandates.search":
            rows = data.get("rows", [])
            for row in rows:
                evidence_urls = row.get("evidence_urls", [])
                if evidence_urls:
                    urls.update(evidence_urls)

        elif tool_name == "legislature.stats":
            evidence_urls = data.get("evidence_urls", [])
            if evidence_urls:
                urls.update(evidence_urls)

        elif tool_name == "person.lookup":
            persons = data.get("persons", [])
            for person in persons:
                evidence_urls = person.get("evidence_urls", [])
                if evidence_urls:
                    urls.update(evidence_urls)

    return sorted(list(urls))


def check_valuative_language(text: str, disallowed_phrases: list[str] | None = None) -> list[str]:
    """Check for valuative language in text."""
    warnings: list[str] = []
    text_lower = text.lower()

    for adj in VALUATIVE_ADJECTIVES:
        if adj in text_lower:
            warnings.append(f"Wertendes Adjektiv gefunden: '{adj}'")

    for phrase in NORMATIVE_PHRASES:
        if phrase in text_lower:
            warnings.append(f"Normative Aufforderung gefunden: '{phrase}'")

    if disallowed_phrases:
        for phrase in disallowed_phrases:
            if phrase.lower() in text_lower:
                warnings.append(f"Blockierte Phrase gefunden: '{phrase}'")

    return warnings


def check_ranking_request(question: str) -> bool:
    """Check if question requests a ranking/evaluation."""
    question_lower = question.lower()
    for pattern in RANKING_PATTERNS:
        if re.search(pattern, question_lower, re.IGNORECASE):
            return True
    return False


def check_data_binding(computed: ComputedResult | None, tool_results: list[dict[str, Any]]) -> list[str]:
    """Check that all data comes from tool results."""
    warnings: list[str] = []

    if not computed:
        warnings.append("Keine computed results vorhanden")
        return warnings

    if not tool_results:
        warnings.append("Keine tool results vorhanden")
        return warnings

    for tr in tool_results:
        data = tr.get("data", {})
        if not data:
            warnings.append(f"Tool {tr.get('tool_name')} hat keine Daten")

    return warnings


def check_scope_clarity(intent: UserIntent | None) -> list[str]:
    """Check that scope (timeframe, parliament) is clear."""
    warnings: list[str] = []

    if not intent:
        return warnings

    if not intent.parliament_id and not intent.legislature_id:
        warnings.append("Kein Parlament/Legislatur angegeben")

    if not intent.from_date and not intent.to_date:
        if intent.intent_type.value in ["MANDATES_LIST", "COMBINED_MANDATES_AND_STATS"]:
            warnings.append("Kein Zeitraum angegeben (from_date/to_date)")

    return warnings


def check_sources_requirement(
    tool_results: list[dict[str, Any]], strict: bool = True
) -> tuple[bool, list[str]]:
    """Check that sources are present."""
    urls = extract_evidence_urls(tool_results)
    warnings: list[str] = []

    if strict and len(urls) == 0:
        return False, ["Keine Evidence-URLs gefunden (strict_evidence=true)"]

    if len(urls) == 0:
        warnings.append("Keine Evidence-URLs gefunden")

    return True, warnings


def create_safe_answer_plan(
    intent: UserIntent | None,
    computed: ComputedResult | None,
    tool_results: list[dict[str, Any]],
    config: OrchestratorConfig,
) -> dict[str, Any]:
    """Create structured plan for safe answer."""
    evidence_urls = extract_evidence_urls(tool_results)
    if config.max_sources > 0:
        evidence_urls = evidence_urls[: config.max_sources]

    plan: dict[str, Any] = {
        "sections": [],
        "sources": evidence_urls,
        "scope": {},
        "computations": {},
    }

    if intent:
        plan["scope"] = {
            "parliament_id": intent.parliament_id,
            "legislature_id": intent.legislature_id,
            "from_date": intent.from_date.isoformat() if intent.from_date else None,
            "to_date": intent.to_date.isoformat() if intent.to_date else None,
        }

    if computed:
        plan["computations"] = computed.computed_metrics

    if config.response_sections:
        plan["sections"] = [
            "Ergebnis",
            "Datenbasis",
            "Details",
            "Quellen",
        ]
        if computed and computed.computed_metrics:
            plan["sections"].insert(3, "Berechnungen")

    return plan


def policy_guard_node(state: dict[str, Any], config: OrchestratorConfig) -> dict[str, Any]:
    """LangGraph node: Enforce policy rules deterministically."""
    if config.policy_mode == PolicyMode.OFF:
        return {
            "policy_decision": PolicyDecision.PASS,
            "policy_warnings": [],
            "safe_answer_plan": {},
        }

    intent: UserIntent | None = state.get("intent")
    tool_results = state.get("tool_results", [])
    computed_dict = state.get("computed")
    computed: ComputedResult | None = ComputedResult(**computed_dict) if computed_dict else None
    question = state.get("question", "")

    warnings: list[str] = []
    decision = PolicyDecision.PASS

    if not intent:
        return {
            "policy_decision": PolicyDecision.PASS,
            "policy_warnings": ["Kein Intent vorhanden"],
            "safe_answer_plan": {},
        }

    if intent.needs_clarification:
        decision = PolicyDecision.NEEDS_CLARIFICATION
        clarifying = intent.clarifying_question or "Bitte spezifizieren Sie Ihre Frage."
        return {
            "policy_decision": decision,
            "policy_warnings": [],
            "safe_answer_plan": {"clarifying_question": clarifying},
        }

    if config.policy_mode == PolicyMode.NEUTRAL_STRICT:
        if check_ranking_request(question):
            decision = PolicyDecision.REFUSE_RANKING
            warnings.append("Ranking-Anfrage erkannt - wird abgelehnt")
            return {
                "policy_decision": decision,
                "policy_warnings": warnings,
                "safe_answer_plan": {
                    "refuse_reason": "ranking",
                    "alternative": "Ich kann keine wertenden Rankings geben. Ich kann dir stattdessen objektive Kennzahlen aus den Daten liefern.",
                },
            }

        disallowed_phrases = None
        if config.disallowed_phrases_strict:
            disallowed_phrases = [p.strip() for p in config.disallowed_phrases_strict.split(",")]

        lang_warnings = check_valuative_language(question, disallowed_phrases)
        if lang_warnings:
            warnings.extend(lang_warnings)
            decision = PolicyDecision.REWRITE_REQUIRED

        data_warnings = check_data_binding(computed, tool_results)
        if data_warnings:
            warnings.extend(data_warnings)

        scope_warnings = check_scope_clarity(intent)
        if scope_warnings and config.policy_mode == PolicyMode.NEUTRAL_STRICT:
            warnings.extend(scope_warnings)

        sources_ok, source_warnings = check_sources_requirement(tool_results, intent.strict_evidence if intent else True)
        if not sources_ok:
            decision = PolicyDecision.REWRITE_REQUIRED
            warnings.extend(source_warnings)
        elif source_warnings:
            warnings.extend(source_warnings)

    safe_plan = create_safe_answer_plan(intent, computed, tool_results, config)

    return {
        "policy_decision": decision,
        "policy_warnings": warnings,
        "safe_answer_plan": safe_plan,
    }

