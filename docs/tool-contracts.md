# Tool Contracts - LangGraph/LLM Integration

## Übersicht

Dieses Dokument beschreibt die Tool Contracts für LangGraph/LLM-Integration. Die Tool API stellt deterministische, schema-validierte Endpoints bereit, die sicher von LLM-Orchestratoren aufgerufen werden können.

## Architektur

```
LangGraph/LLM
    ↓
Python Tool Client (httpx)
    ↓
FastAPI HTTP Gateway
    ↓
Query Services (Neo4j)
    ↓
Neo4j Database
```

## Endpoints

### POST /api/tools/mandates/search

Sucht Mandate mit Evidence-by-default.

**Request:**
```json
{
  "parliament_id": "NI",
  "party_code": "SPD",
  "from_date": "2014-01-01",
  "to_date": "2020-12-31",
  "limit": 50,
  "offset": 0,
  "sort": "start_date",
  "sort_dir": "ASC",
  "strict_evidence": true,
  "active_only": false,
  "as_of": "2020-01-01"
}
```

**Request Parameters:**
- `active_only` (optional, bool): When `true`, excludes mandates with `start_date=NULL` and returns telemetry in `meta`.
- `as_of` (optional, string, YYYY-MM-DD): Stichtag for `active_only=true` queries. If omitted and `active_only=true`, uses coverage-based clamping.

**Response:**
```json
{
  "meta": {
    "tool": "mandates.search",
    "executed_at": "2024-01-15T10:30:00Z",
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "result_hash": "a1b2c3d4e5f6...",
    "data_version": "git:abc123def456",
    "warnings": [],
    "active_only": true,
    "as_of": "2020-01-01",
    "coverage_degraded": true,
    "excluded_due_to_missing_start_date_count": 123,
    "excluded_due_to_missing_legislature_start_date_count": 45
  },
  "applied_filter": { ... },
  "total": null,
  "rows": [
    {
      "person_id": "person-123",
      "person_name": "Stephan Weil",
      "wikipedia_title": "Stephan_Weil",
      "mandate_id": "mandate-456",
      "parliament_id": "NI",
      "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
      "legislature": "17. Landtag Niedersachsen",
      "start_date": "2013-01-20",
      "end_date": "2017-11-14",
      "party_code": "SPD",
      "evidence_urls": [
        "https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"
      ]
    }
  ]
}
```

**Response Meta Telemetry (when `active_only=true`):**
- `active_only` (bool): Indicates that `active_only=true` was used in the request.
- `as_of` (string, YYYY-MM-DD): The effective stichtag used for filtering (may be clamped to data coverage).
- `coverage_degraded` (bool, optional): `true` if data coverage is incomplete (e.g., missing legislature start dates).
- `excluded_due_to_missing_start_date_count` (int, optional): Number of mandates excluded because `mandate.start_date IS NULL`.
- `excluded_due_to_missing_legislature_start_date_count` (int, optional): Number of mandates excluded because the legislature has no day-precision `start_date`.

**Note:** When `active_only=false`, these telemetry fields may be omitted from `meta`.

### POST /api/tools/legislatures/stats

Holt Statistiken für eine Legislature.

**Request:**
```json
{
  "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "strict_evidence": true
}
```

**Response:**
```json
{
  "meta": {
    "tool": "legislature.stats",
    "executed_at": "2024-01-15T10:30:00Z",
    "request_id": "550e8400-e29b-41d4-a716-446655440001",
    "data_version": "git:abc123def456"
  },
  "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "legislature_name": "17. Landtag Niedersachsen",
  "total_seats": 137,
  "party_seats": {
    "SPD": 49,
    "CDU": 54,
    "B90/GRÜNE": 20,
    "FDP": 14
  },
  "party_vote_share": {},
  "evidence_urls": [
    "https://de.wikipedia.org/w/index.php?title=...&oldid=256198867"
  ]
}
```

### POST /api/tools/persons/lookup

Lookup Person nach ID oder Suche nach Name.

**Request (by ID):**
```json
{
  "person_id": "person-123"
}
```

**Request (by name):**
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
    "executed_at": "2024-01-15T10:30:00Z",
    "request_id": "550e8400-e29b-41d4-a716-446655440002",
    "data_version": "git:abc123def456"
  },
  "persons": [
    {
      "person_id": "person-123",
      "name": "Stephan Weil",
      "wikipedia_title": "Stephan_Weil",
      "wikipedia_url": "https://de.wikipedia.org/wiki/Stephan_Weil",
      "birth_date": "1958-03-15",
      "death_date": null,
      "intro": "Stephan Weil (* 15. März 1958 in Hamburg)...",
      "evidence_urls": [
        "https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=245123456"
      ]
    }
  ]
}
```

## strict_evidence Verhalten

### strict_evidence=true (Default)

- **mandates.search**: Jede `row` muss `evidence_urls` mit min. 1 URL haben
- **legislature.stats**: `evidence_urls` muss min. 1 URL haben
- **person.lookup**: Jede `person` muss `evidence_urls` mit min. 1 URL haben

**Bei Verletzung:** HTTP 422 mit Error Code `EVIDENCE_MISSING`:

```json
{
  "detail": {
    "error": "EVIDENCE_MISSING: 5 row(s) without evidence_urls (strict_evidence=true): [mandate-1, mandate-2, ...]",
    "error_code": "EVIDENCE_MISSING",
    "request_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### strict_evidence=false

- Erlaubt leere `evidence_urls` Arrays
- Gibt Warnung in `meta.warnings` zurück

## Sorting/Pagination

### Sorting

**Stable Sort:**
- Primary: Nach `sort`-Feld (default: `person_name`)
- Secondary: Immer `start_date ASC` (für deterministische Reihenfolge)

**Verfügbare Sort-Felder:**
- `person_name`
- `start_date`
- `end_date`
- `party_code`

**Sort-Direktion:**
- `ASC` (default)
- `DESC`

### Pagination

- **limit**: 1-1000 (default: 200)
- **offset**: 0+ (default: 0)
- **total**: Optional verfügbar (wenn performant)

## Meta Felder und Reproducibility

Jede Response enthält `meta` mit:

- **tool**: Tool-Name (z.B. `"mandates.search"`)
- **executed_at**: ISO 8601 Timestamp (UTC)
- **request_id**: UUID v4 für Request-Tracking
- **result_hash**: SHA256-Hash des canonical JSON (ohne `meta.executed_at`) für Reproduzierbarkeit
- **data_version**: Git SHA oder Import Run ID (z.B. `"git:abc123"`)
- **warnings**: Array von Warnungen (z.B. `["5 row(s) without evidence_urls"]`)

### Reproducibility

Mit `result_hash` kann die Response-Integrität geprüft werden:

```python
import hashlib
import json

# Compute hash (without meta.executed_at)
data_copy = response_data.copy()
data_copy["meta"].pop("executed_at")
canonical_json = json.dumps(data_copy, sort_keys=True, separators=(",", ":"))
computed_hash = hashlib.sha256(canonical_json.encode()).hexdigest()

assert computed_hash == response_data["meta"]["result_hash"]
```

## JSON Schemas

Alle Request/Response Schemas liegen in `contracts/tools/`:

- `common.schema.json` - Shared Types (Date, DateTime, UrlArray, ToolMeta)
- `mandates.search.request.schema.json`
- `mandates.search.response.schema.json`
- `legislature.stats.request.schema.json`
- `legislature.stats.response.schema.json`
- `person.lookup.request.schema.json`
- `person.lookup.response.schema.json`

**Beispiele:** `contracts/tools/examples/`

## Python LangGraph Tool Client

Thin Wrapper für LangGraph-Integration:

```python
from langgraph_tools import mandates_search, legislature_stats, person_lookup

# Async usage
result = await mandates_search(
    parliament_id="NI",
    party_code="SPD",
    from_date="2014-01-01",
    to_date="2020-12-31",
    limit=50,
    strict_evidence=True,
)

stats = await legislature_stats("7219e8b8-3d63-59ae-823e-df5a7a0d2253")

persons = await person_lookup(name_contains="Weil", limit=10)
```

**Location:** `langgraph_tools/parliament_api.py`

**Features:**
- httpx async client
- Timeout + Retries (konservativ)
- Error handling mit tool name + request_id
- Keine LangGraph Graph-Definition (nur Tool-Call-Funktionen)

## Server Starten

### CLI

```bash
# Start API Server
docker compose run --rm scraper scraper api --host 0.0.0.0 --port 8000

# Mit Auto-Reload (Development)
docker compose run --rm scraper scraper api --reload
```

### Docker Compose

Füge Service hinzu:

```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    command: scraper api --host 0.0.0.0 --port 8000
    depends_on:
      neo4j:
        condition: service_healthy
    env_file: .env
```

## Contract Tests

Tests validieren, dass Responses dem Schema entsprechen:

```bash
# Run contract tests
pytest tests/test_api_contracts.py -v
```

**Tests:**
- Request schema validation
- Response schema validation
- Invalid date range → 422
- strict_evidence enforcement → 422 wenn verletzt

## Error Codes

- **VALIDATION_ERROR**: Request-Validierung fehlgeschlagen (z.B. `from_date > to_date`)
- **EVIDENCE_MISSING**: `strict_evidence=true` aber keine Evidence URLs
- **INTERNAL_ERROR**: Server-Fehler

## Akzeptanzkriterien (Definition of Done)

✅ **JSON Schemas liegen im Repo** und sind in Tests genutzt

✅ **FastAPI Tool Endpoints validieren strikt** und liefern deterministische DTO-basierte Ergebnisse

✅ **Evidence-by-default enforced** (strict mode)

✅ **Contract Tests grün**

✅ **Python Tool Client vorhanden** (LangGraph-ready)

✅ **Meta Felder** mit Reproducibility (result_hash, data_version)

✅ **Stable Sorting** (Primary + Secondary Sort)

✅ **Pagination & Limits** (max 1000)

## Integration mit LangGraph

Die Tool Contracts sind direkt als LangGraph Tools verwendbar:

```python
from langchain.tools import Tool
from langgraph_tools import mandates_search

mandate_tool = Tool(
    name="search_mandates",
    description="Search mandates with filters (parliament, party, date range, person)",
    func=lambda **kwargs: await mandates_search(**kwargs),
)
```

Die Tools liefern deterministische, evidence-basierte Ergebnisse, die sicher von LLMs verwendet werden können.

