# Policy Layer

Der Policy Layer stellt sicher, dass Antworten zu politischen Inhalten neutral, nachvollziehbar und datenbasiert sind. Optional kann für Entwickler eine Explain-Query-Debug-Ausgabe aktiviert werden.

## Übersicht

Der Policy Layer ist als deterministischer Node (`policy_guard`) zwischen `compute` und `response_composer` im LangGraph-Orchestrator eingebunden. Er prüft alle Antworten auf Einhaltung der Policy-Regeln, bevor sie formatiert werden.

## Policy Modes

### NEUTRAL_STRICT (Standard)

Strikte Neutralität und Datenbindung:

- **Wertungs-Guardrail**: Blockiert wertende Adjektive, pauschale Unterstellungen, normative Aufforderungen
- **Ranking-Verbot**: Lehnt Ranking-Anfragen ab und bietet stattdessen objektive Kennzahlen
- **Datenbindung**: Jede Zahl, jeder Name, jedes Datum muss aus Tool-Daten stammen
- **Scope-Klarheit**: Erzwingt explizite Angaben zu Zeitraum und Geltungsbereich
- **Quellenpflicht**: Deduplizierte Evidence-URLs müssen vorhanden sein

### NEUTRAL_LENIENT

Entwickler-Modus mit weniger strikten Regeln:

- Wertungs-Guardrail bleibt aktiv
- Scope-Klarheit wird nicht erzwungen
- Quellenpflicht bleibt bestehen

### OFF

Policy-Layer deaktiviert (nur für interne Tests):

- Alle Prüfungen werden übersprungen
- Policy-Entscheidung ist immer `PASS`

## Konfiguration

Umgebungsvariablen (mit `PISS_` Prefix):

```bash
# Policy Mode (NEUTRAL_STRICT, NEUTRAL_LENIENT, OFF)
PISS_POLICY_MODE=NEUTRAL_STRICT

# Debug Explain Queries (true/false)
PISS_DEBUG_EXPLAIN_QUERIES=false

# Include Raw Tool Payloads in Debug (true/false)
PISS_DEBUG_INCLUDE_RAW_TOOL_PAYLOADS=false

# Strukturierte Sektionen erzwingen (true/false)
PISS_RESPONSE_SECTIONS=true

# Maximale Anzahl Quellen in Quellenliste
PISS_MAX_SOURCES=20

# Blockierte Phrasen (kommagetrennt)
PISS_DISALLOWED_PHRASES_STRICT="korrupt,skandalös"
```

## Policy-Entscheidungen

Der Policy Guard kann folgende Entscheidungen treffen:

### PASS

Antwort erfüllt alle Policy-Anforderungen und kann formatiert werden.

### REWRITE_REQUIRED

Antwort muss überarbeitet werden (z.B. wertende Sprache gefunden, fehlende Quellen).

### REFUSE_RANKING

Ranking-Anfrage wird abgelehnt. Stattdessen wird eine alternative Antwort mit objektiven Kennzahlen angeboten.

### NEEDS_CLARIFICATION

Frage ist unklar und benötigt Klärung (z.B. fehlender Zeitraum, unklare Zuordnung).

## Erlaubte vs. Nicht Erlaubte Antworten

### ✅ Erlaubt

- **Neutrale Fakten**: "Die SPD hatte im 17. Landtag Niedersachsen 49 von 137 Sitzen (35.8%)."
- **Datenbasierte Metriken**: "Im Zeitraum 2014-2020 gab es 12 SPD-Mandate im Landtag Niedersachsen."
- **Objektive Kennzahlen**: "Sitzanteile: SPD 35.8%, CDU 39.4%."
- **Klare Scope-Angaben**: "Landtag Niedersachsen, 17. Wahlperiode (2013-2017)"

### ❌ Nicht Erlaubt

- **Wertende Sprache**: "Die SPD ist die schlimmste Partei."
- **Ranking-Anfragen**: "Welche Partei ist am besten?"
- **Normative Aufforderungen**: "Wähle die SPD."
- **Ungestützte Behauptungen**: "Die Partei X lügt."
- **Fehlende Quellen**: Antworten ohne Evidence-URLs (bei `strict_evidence=true`)

## Explain Query Debug Output

Wenn `PISS_DEBUG_EXPLAIN_QUERIES=true` gesetzt ist, wird an jede Antwort ein Developer-Block angehängt (klar markiert mit `[Developer Debug]`).

### Inhalt

- **Intent**: Normalisierter Intent mit Filtern
- **Tool Calls**: Liste aller Tool-Calls mit Endpoint, Payload-Zusammenfassung, request_id, executed_at
- **Tool Results Summary**: Zusammenfassung der Tool-Ergebnisse (Row-Counts, Totals)
- **Computed Results**: Berechnete Metriken
- **Policy Warnings**: Falls vorhanden, alle Policy-Warnungen

### Raw Payloads

Standardmäßig werden keine raw Payloads ausgegeben. Nur wenn `PISS_DEBUG_INCLUDE_RAW_TOOL_PAYLOADS=true` gesetzt ist, wird max. 1 Beispiel-Payload (redacted) angezeigt.

### Beispiel

```
## [Developer Debug] Explain Query

### Intent
Type: MANDATES_LIST
Filters:
  parliament_id: "NI"
  party_code: "SPD"
  from_date: "2014-01-01"
  to_date: "2020-12-31"

### Tool Calls
1. mandates.search
   Params:
     parliament_id: "NI"
     party_code: "SPD"
     from_date: "2014-01-01"
     to_date: "2020-12-31"
   Request ID: 550e8400-e29b-41d4-a716-446655440000

### Tool Results Summary
1. mandates.search
   Request ID: 550e8400-e29b-41d4-a716-446655440000
   Rows: 12

### Computed Results
{
  "computed_metrics": {},
  "grouped_data": null,
  "raw_data": {...}
}
```

## Strukturierte Antworten

Wenn `PISS_RESPONSE_SECTIONS=true` (Standard), werden Antworten in strukturierte Sektionen unterteilt:

1. **Ergebnis**: Kurze Zusammenfassung
2. **Datenbasis**: Was genau abgefragt wurde (Parlament, Zeitraum, Filter)
3. **Details**: Liste/Tabelle der Mandate (optional gruppiert)
4. **Berechnungen**: Wenn Prozentwerte gezeigt werden, Formel + Inputwerte
5. **Quellen**: Deduplizierte URLs, begrenzt durch `PISS_MAX_SOURCES`

## Akzeptanzkriterien (Definition of Done)

✅ **Antworten sind neutral und datenbasiert**

- Keine wertenden Adjektive
- Keine emotionalen oder parteiischen Formulierungen
- Alle Fakten stammen aus Tool-Daten

✅ **Klare Scope-/Zeitraum-Angabe**

- Explizite Datumswerte für Zeiträume
- Klarer Geltungsbereich (Parlament, Wahlperiode)

✅ **Keine Rankings/Meinungsurteile**

- Ranking-Anfragen werden abgelehnt
- Stattdessen werden objektive Metriken angeboten

✅ **Quellen werden dedupliziert und angezeigt**

- Mindestens eine Quelle pro Antwort (bei `strict_evidence=true`)
- Quellenliste ist dedupliziert
- Begrenzt durch `PISS_MAX_SOURCES`

✅ **Debug-Ausgabe existiert und ist per Flag steuerbar**

- Debug-Output nur wenn `PISS_DEBUG_EXPLAIN_QUERIES=true`
- Enthält Intent, Tool Calls, Results, Computations, Policy Warnings
- Raw Payloads nur wenn `PISS_DEBUG_INCLUDE_RAW_TOOL_PAYLOADS=true`

✅ **Tests grün und fangen Policy-Regressions ab**

- Unit Tests für Policy Guard
- Integration/E2E Tests für gesamten Flow
- Tests für Ranking-Verbot, Quellenpflicht, wertende Sprache

## Implementierungsdetails

### Policy Guard Node

Der Policy Guard Node (`langgraph_app/nodes/policy_guard.py`) ist vollständig deterministisch und macht keine LLM-Calls. Er prüft:

- **Sprach-/Wertungs-Guardrail**: Phrase-Listen und Regex-Patterns
- **Datenbindung**: Prüft, ob Tool-Results vorhanden sind
- **Scope-Klarheit**: Prüft Intent auf fehlende Angaben
- **Quellenpflicht**: Extrahiert und dedupliziert Evidence-URLs

### Response Composer Anpassung

Der Response Composer (`langgraph_app/nodes/response_composer.py`) wurde angepasst:

- **System Prompt erweitert**: Neutraler Ton, keine Wertungen, keine Spekulation
- **Template-basierte Formatierung**: Deterministische Sektionen (wenn `response_sections=true`)
- **Debug-Output**: Formatierung des Explain-Query-Blocks

### Graph-Integration

Der Policy Guard wurde zwischen `compute` und `response_composer` im Graph eingefügt:

```
Intent → Router → Tool Executor → Evidence Gate → Compute → Policy Guard → Response Composer
```

## Nicht-Ziele

Der Policy Layer deckt **nicht** ab:

- ❌ Wahl-/Abstimmungsprozeduren
- ❌ Rechtsberatung
- ❌ Live-News
- ❌ "Meinungs-Bot": Wenn nach Bewertungen gefragt wird, nur datenbasierte Alternativen anbieten

## Weitere Informationen

- [LangGraph Orchestrator Dokumentation](./langgraph-orchestrator.md)
- [Tool Contracts](./tool-contracts.md)
- [Query Contracts](./query-contracts.md)

