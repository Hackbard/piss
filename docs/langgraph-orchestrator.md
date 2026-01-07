# LangGraph Orchestrator

## Übersicht

Der LangGraph Orchestrator übersetzt natürliche Sprache in deterministische Tool-Aufrufe und generiert Antworten ausschließlich aus Tool-Daten. Jede Antwort enthält Evidence-URLs; keine Halluzinationen.

## Architektur

```
User Question (Natural Language)
    ↓
Intent Parser (LLM → structured JSON)
    ↓
Router (deterministic mapping)
    ↓
Tool Executor (HTTP calls to Laravel Tools)
    ↓
Evidence Gate (strict_evidence enforcement)
    ↓
Deterministic Compute (Python-only calculations)
    ↓
Response Composer (LLM optional, facts from data only)
    ↓
Final Answer (with Evidence URLs)
```

### Nodes

1. **Intent Parser**: LLM analysiert Frage → `UserIntent` (JSON)
2. **Router**: Deterministisches Mapping Intent → Tool Calls
3. **Tool Executor**: HTTP Calls auf Tool API mit Contract Validation
4. **Evidence Gate**: Prüft `strict_evidence` (hard fail bei Verletzung)
5. **Compute**: Deterministische Berechnungen (Python, keine LLM)
6. **Response Composer**: Formatiert Antwort (LLM optional, nur Fakten aus Daten)

## Tool Endpoints

### POST /api/tools/mandates/search

Sucht Mandate mit Filtern.

**Request:**
```json
{
  "parliament_id": "NI",
  "party_code": "SPD",
  "from_date": "2014-01-01",
  "to_date": "2020-12-31",
  "limit": 50,
  "strict_evidence": true
}
```

**Response:**
```json
{
  "meta": {
    "tool": "mandates.search",
    "request_id": "550e8400-e29b-41d4-a716-446655440000"
  },
  "rows": [
    {
      "person_name": "Stephan Weil",
      "mandate_id": "mandate-456",
      "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
      "start_date": "2013-01-20",
      "party_code": "SPD",
      "evidence_urls": ["https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"]
    }
  ]
}
```

### POST /api/tools/legislatures/stats

Holt Statistiken für eine Legislature.

**Request:**
```json
{
  "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "strict_evidence": true
}
```

**Response:**
```json
{
  "meta": {
    "tool": "legislature.stats",
    "request_id": "550e8400-e29b-41d4-a716-446655440001"
  },
  "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "total_seats": 137,
  "party_seats": {
    "SPD": 49,
    "CDU": 54
  },
  "evidence_urls": ["https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"]
}
```

### POST /api/tools/persons/lookup

Lookup Person nach ID oder Name.

**Request:**
```json
{
  "name_contains": "Weil",
  "limit": 10
}
```

**Response:**
```json
{
  "meta": {
    "tool": "person.lookup",
    "request_id": "550e8400-e29b-41d4-a716-446655440002"
  },
  "persons": [
    {
      "person_id": "person-123",
      "name": "Stephan Weil",
      "wikipedia_title": "Stephan_Weil",
      "evidence_urls": ["https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=245123456"]
    }
  ]
}
```

## Konfiguration

### Environment Variables

```bash
# Tool API
PISS_TOOL_BASE_URL=http://localhost:8000/api/tools
PISS_TOOL_TIMEOUT_SECONDS=20
PISS_TOOL_STRICT_EVIDENCE=true

# Ollama (OpenAI-kompatibel)
OLLAMA_BASE_URL=http://192.168.178.185:11434/v1
OLLAMA_MODEL=ministral-3:14b
OPENAI_API_KEY=ollama

# LangSmith (optional)
LANGSMITH_TRACING=false
LANGSMITH_ENDPOINT=https://api.smith.langchain.com
LANGSMITH_API_KEY=your_key_here
LANGSMITH_PROJECT=parliament-orchestrator
```

## MVP vs. Vollständiger Orchestrator

**MVP (ohne LLM):**
- Verwendet Regex-Parsing für Parameter-Extraktion
- Keine LLM-Abhängigkeit
- CLI: `python -m langgraph_app.cli "..." --format json`
- Siehe [langgraph_app/README.md](../langgraph_app/README.md)

**Vollständiger Orchestrator (mit LLM):**
- Verwendet LLM für Intent-Parsing
- Erfordert Ollama oder OpenAI API
- Server: `python -m langgraph_app.server "..."`

## Starten

### MVP (ohne LLM)

```bash
# Einfache Abfrage
python -m langgraph_app.cli "Alle SPD-Mitglieder im Landtag Niedersachsen zwischen 2014-2020"

# Mit JSON Output
python -m langgraph_app.cli "Liste CDU im Bundestag 2018-2021" --format json
```

### Local Server (vollständiger Orchestrator)

```bash
# Environment setzen
export PISS_TOOL_BASE_URL=http://localhost:8000/api/tools
export OLLAMA_BASE_URL=http://192.168.178.185:11434/v1
export OLLAMA_MODEL=ministral-3:14b

# Server starten
python -m langgraph_app.server "Alle SPD-Mitglieder im Landtag Niedersachsen 2014-2020"
```

### Python API

```python
import asyncio
from langgraph_app.server import run_query

answer = asyncio.run(run_query("Alle SPD-Mitglieder im Landtag Niedersachsen 2014-2020"))
print(answer)
```

### LangGraph Local Server (falls verfügbar)

```bash
langgraph dev
```

## Beispiel-Fragen

### 1. Mandate-Liste

**Frage:** "Alle Mitglieder der SPD im Landtag Niedersachsen zwischen 2014-01-01 und 2020-12-31."

**Intent:** `MANDATES_LIST`
- `parliament_id`: "NI"
- `party_code`: "SPD"
- `from_date`: "2014-01-01"
- `to_date`: "2020-12-31"

**Tool Call:** `mandates.search`

**Antwort:** Liste mit Evidence-URLs für jede Person.

### 2. Legislature-Statistiken

**Frage:** "Wie hoch war der Sitzanteil der SPD im 17. Landtag Niedersachsen?"

**Intent:** `LEGISLATURE_STATS`
- `legislature_id`: "7219e8b8-3d63-59ae-823e-df5a7a0d2253"
- `metrics`: ["SEAT_SHARE_PERCENT"]

**Tool Call:** `legislature.stats`

**Compute:** `seat_share_percent = (party_seats / total_seats) * 100`

**Antwort:** "Der 17. Landtag Niedersachsen hatte 137 Sitze. SPD: 35.8% (49 Sitze).\n\nEvidence: https://..."

### 3. Personensuche

**Frage:** "Gib mir Mandate von Stephan Weil 2014–2020."

**Intent:** `COMBINED_MANDATES_AND_STATS`
- `person_name_contains`: "Stephan Weil"
- `from_date`: "2014-01-01"
- `to_date`: "2020-12-31"

**Tool Calls:**
1. `person.lookup` (finde Person)
2. `mandates.search` (mit `person_id`)

**Antwort:** Mandate-Liste mit Evidence-URLs.

## Evidence Gate

Wenn `strict_evidence=true` (default):

- **mandates.search**: Jede `row` muss `evidence_urls` mit min. 1 URL haben
- **legislature.stats**: `evidence_urls` muss min. 1 URL haben
- **person.lookup**: Jede `person` muss `evidence_urls` mit min. 1 URL haben

**Bei Verletzung:** Graph endet mit Fehlermeldung (keine Antwort generiert).

## Deterministic Compute

Alle Berechnungen erfolgen in Python (keine LLM-Rechnungen):

- **Seat Share**: `round(party_seats / total_seats * 100, 1)`
- **Grouping**: Nach `legislature_id` oder `party_code`
- **Aggregation**: Summen, Durchschnitte, etc.

## Response Composer

- **LLM optional**: Nur für Formulierung/Formatierung
- **Fakten fix**: Alle Fakten müssen aus Tool-Daten stammen
- **Evidence-URLs**: Jede Antwort enthält mindestens 1 Evidence-URL
- **Datumswerte**: Explizite Datumswerte (nicht "vor 5 Jahren")

## Tests

```bash
# Unit Tests
pytest tests/test_langgraph_intent_parser.py -v
pytest tests/test_langgraph_router.py -v
pytest tests/test_langgraph_evidence_gate.py -v
pytest tests/test_langgraph_compute.py -v

# E2E Tests (stubbed)
pytest tests/test_langgraph_graph_e2e_stubbed.py -v
```

## Akzeptanzkriterien

✅ Graph läuft lokal und kann mindestens 2 Beispiel-Fragen beantworten

✅ Keine Fakten ohne Tool-Daten (Tests decken das ab)

✅ `strict_evidence` enforced

✅ Deterministic compute vorhanden und getestet

✅ LangSmith optional per Env aktivierbar

✅ Jede Antwort enthält Evidence-URLs



