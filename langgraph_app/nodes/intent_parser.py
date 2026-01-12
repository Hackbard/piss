"""Intent Parser Node: LLM → structured JSON."""

import json
from typing import Any

from langchain_openai import ChatOpenAI

from langgraph_app.config import OrchestratorConfig
from langgraph_app.schemas import IntentType, UserIntent


INTENT_PARSER_SYSTEM_PROMPT = """Du bist ein Intent-Parser für Parlamentsdaten-Abfragen.

Deine Aufgabe: Analysiere die Benutzerfrage und gib AUSSCHLIESSLICH valides JSON aus, das dem UserIntent-Schema entspricht.

WICHTIG:
- Gib NUR JSON aus, keine Erklärungen, keine Fakten, keine Namen raten
- Zeiträume IMMER als YYYY-MM-DD (ISO-Format)
- Partei-Codes IMMER UPPERCASE (z.B. "SPD", "CDU")
- Wenn Parlament/Wahlperiode nicht eindeutig: setze needs_clarification=true und clarifying_question
- Wenn unklar: setze needs_clarification=true

Intent-Typen:
- MANDATES_LIST: Liste von Mandaten (z.B. "Alle SPD-Mitglieder im Landtag Niedersachsen")
- LEGISLATURE_STATS: Statistiken einer Wahlperiode (z.B. "Sitzanteil SPD im 17. Landtag Niedersachsen")
- COMBINED_MANDATES_AND_STATS: Beides kombiniert
- PERSON_LOOKUP: Personensuche (z.B. "Gib mir Informationen über Stephan Weil")

Output-Format:
- BULLETS: Aufzählung
- TABLE: Tabelle
- JSON: Roh-JSON

Gib NUR das JSON-Objekt aus, ohne Markdown-Code-Blöcke."""


def parse_intent(question: str, config: OrchestratorConfig) -> UserIntent:
    """Parse user question into structured intent."""
    llm = ChatOpenAI(
        base_url=config.ollama_base_url,
        model=config.ollama_model,
        temperature=0,
        api_key=config.openai_api_key,
        max_tokens=512,
    )

    prompt = f"{INTENT_PARSER_SYSTEM_PROMPT}\n\nBenutzerfrage: {question}\n\nJSON:"

    try:
        response = llm.invoke(prompt)
        content = response.content.strip()

        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

        intent_data = json.loads(content)
        intent_data.setdefault("strict_evidence", config.tool_strict_evidence)

        return UserIntent(**intent_data)
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        return UserIntent(
            intent_type=IntentType.MANDATES_LIST,
            needs_clarification=True,
            clarifying_question=f"Konnte Ihre Frage nicht vollständig verstehen. Bitte spezifizieren Sie Parlament, Zeitraum und/oder Partei. (Fehler: {e})",
            strict_evidence=config.tool_strict_evidence,
        )


def intent_parser_node(state: dict[str, Any], config: OrchestratorConfig) -> dict[str, Any]:
    """LangGraph node: Parse user question into intent."""
    question = state.get("question", "")
    if not question:
        return {
            "intent": UserIntent(
                intent_type=IntentType.MANDATES_LIST,
                needs_clarification=True,
                clarifying_question="Bitte stellen Sie eine Frage.",
                strict_evidence=config.tool_strict_evidence,
            )
        }

    intent = parse_intent(question, config)
    return {"intent": intent}

