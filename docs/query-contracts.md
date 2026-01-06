# Query Contracts - Tool Contracts für deterministische Abfragen

## Übersicht

Dieses Dokument beschreibt die standardisierten Query-Contracts (Tool Contracts) für deterministische, reproduzierbare Datenabfragen. Diese Contracts sind als LangGraph/LLM-Tools verwendbar und garantieren:

- **Evidence-by-default**: Jede Ergebniszeile enthält `evidence_urls` (dedupliziert, nie null)
- **Deterministisch & reproduzierbar**: Stabile Sortierung, keine zufälligen Ergebnisse
- **Testbar**: Klare DTOs statt "array soup"
- **Zeitfilter mit Overlap-Logik**: Offene Endzeiten werden korrekt behandelt
- **Pagination & Limits**: Schutz vor unbounded queries

## MandateQueryFilter

Filter-DTO für Mandats-Abfragen.

### Felder

```python
class MandateQueryFilter:
    parliament_id: Optional[str]          # Parliament-Code Filter (z.B. 'NI', 'BY', 'BT', 'BR')
    legislature_id: Optional[str]         # Legislature ID Filter
    party_code: Optional[str]             # Party code (z.B. 'SPD', 'CDU')
    from_date: Optional[date]             # Start-Datum (inclusive)
    to_date: Optional[date]               # End-Datum (inclusive)
    person_id: Optional[str]              # Person ID Filter
    person_name_contains: Optional[str]   # Person Name Contains (case-insensitive)
    limit: int = 200                      # Max Results (1-1000, default: 200)
    offset: int = 0                       # Offset für Pagination
    sort: SortField = SortField.PERSON_NAME  # Sort-Feld
    sort_direction: SortDirection = SortDirection.ASC  # ASC oder DESC
```

### Sort-Felder

- `person_name`: Nach Person-Name sortieren
- `start_date`: Nach Start-Datum sortieren
- `end_date`: Nach End-Datum sortieren (NULL = offen = 9999-12-31)
- `party_code`: Nach Parteikürzel sortieren

### Validierung

- `from_date <= to_date` wenn beide gesetzt
- `limit` wird im Service-Layer automatisch auf max 1000 geklemmt
- `offset >= 0`

### Beispiel

```python
from datetime import date
from scraper.models.query import MandateQueryFilter, SortField, SortDirection

filter_obj = MandateQueryFilter(
    parliament_id="NI",
    party_code="SPD",
    from_date=date(2014, 1, 1),
    to_date=date(2020, 12, 31),
    limit=50,
    sort=SortField.START_DATE,
    sort_direction=SortDirection.ASC,
)
```

## MandateQueryResult

Response-DTO für Mandats-Abfragen.

### Felder

```python
class MandateQueryResult:
    rows: list[MandateRow]                # Ergebnis-Zeilen
    total: Optional[int]                  # Total Count (wenn verfügbar)
    applied_filter: MandateQueryFilter    # Angewendeter Filter (normalisiert)
```

### MandateRow

```python
class MandateRow:
    person_id: str                        # Person ID
    person_name: str                      # Vollständiger Name
    wikipedia_title: Optional[str]        # Wikipedia-Seitentitel
    mandate_id: str                       # Mandate ID
    legislature_id: str                    # Legislature ID
    legislature_name: Optional[str]       # Legislature Name
    parliament_id: str                    # Parliament ID
    start_date: date                      # Start-Datum
    end_date: Optional[date]              # End-Datum (None = offen)
    party_code: Optional[str]            # Parteikürzel
    evidence_urls: list[str]             # Deduplizierte Evidence URLs (nie null)
```

**Wichtig**: `evidence_urls` ist **immer** eine Liste (nie null), kann aber leer sein. URLs sind dedupliziert und sortiert.

## Overlap-Regel

Die Zeitfilter-Logik verwendet **Overlap-Detection**:

### Regel

Ein Mandat überlappt mit dem Filter-Zeitraum, wenn:

```
mandate.start_date <= toDate AND (mandate.end_date IS NULL OR mandate.end_date >= fromDate)
```

### Spezialfälle

1. **Nur `from_date` gesetzt**: `to_date` wird auf "heute" gesetzt
2. **Nur `to_date` gesetzt**: `from_date` wird auf `1900-01-01` gesetzt
3. **Beide gesetzt**: Normale Overlap-Prüfung
4. **Offenes Mandat** (`end_date = NULL`): Wird als unbegrenzt behandelt

### Beispiele

```python
# Mandat: 2020-01-01 bis 2020-12-31
# Filter: 2020-06-01 bis 2020-06-30
# → Überlappt ✓

# Mandat: 2020-01-01 bis NULL (offen)
# Filter: 2020-06-01 bis 2020-12-31
# → Überlappt ✓

# Mandat: 2020-01-01 bis 2020-05-31
# Filter: 2020-06-01 bis 2020-12-31
# → Keine Überlappung ✗
```

## Evidence-by-default Regel

**Jede Ergebniszeile muss `evidence_urls` enthalten.**

### Implementierung

1. **Evidence-Join**: Alle Queries joinen automatisch `(Mandate)-[:SUPPORTED_BY]->(Evidence)`
2. **Deduplizierung**: URLs werden dedupliziert (`DISTINCT`)
3. **Null-Filter**: `NULL`-URLs werden herausgefiltert
4. **Sortierung**: URLs werden alphabetisch sortiert
5. **Nie null**: `evidence_urls` ist immer eine Liste (kann leer sein)

### Beispiel

```python
result = service.search(filter_obj)
for row in result.rows:
    assert isinstance(row.evidence_urls, list)  # Immer eine Liste
    # Kann leer sein, wenn keine Evidence vorhanden
    if row.evidence_urls:
        print(f"Evidence: {row.evidence_urls[0]}")
```

## Limits/Pagination

### Limits

- **Default**: 200 Ergebnisse
- **Maximum**: 1000 Ergebnisse (automatisch geklemmt)
- **Minimum**: 1 Ergebnis

### Pagination

- **Offset**: Start-Index (0-basiert)
- **Limit**: Anzahl Ergebnisse pro Seite
- **Total**: Optional verfügbar (wenn performant möglich)

### Beispiel

```python
# Seite 1
filter1 = MandateQueryFilter(limit=50, offset=0)
result1 = service.search(filter1)

# Seite 2
filter2 = MandateQueryFilter(limit=50, offset=50)
result2 = service.search(filter2)
```

## Stable Sorting

Alle Queries verwenden **stabile Sortierung**:

1. **Primary Sort**: Nach `sort`-Feld (z.B. `person_name`)
2. **Secondary Sort**: Immer `start_date ASC` (für deterministische Reihenfolge bei gleichen Primary-Werten)

### Beispiel

```python
# Sortierung: person_name ASC, start_date ASC
filter_obj = MandateQueryFilter(
    sort=SortField.PERSON_NAME,
    sort_direction=SortDirection.ASC,
)
```

## Service Interfaces

### MandateQueryServiceInterface

```python
def search(filter: MandateQueryFilter) -> MandateQueryResult:
    """
    Search mandates with evidence-by-default.
    
    Rules:
    - ALWAYS evidence joined (evidence_urls never null, may be empty)
    - Stable sort (person_name ASC, start_date ASC as default)
    - Uses overlap logic
    """
```

### LegislatureStatsServiceInterface

```python
def get_legislature_stats(legislature_id: str) -> LegislatureStats:
    """
    Get statistics for a legislature.
    
    Returns party seat counts and vote shares (if available).
    Always includes evidence_urls for statistics source.
    """
```

### PersonLookupServiceInterface

```python
def find_by_id(person_id: str) -> PersonDTO | None:
    """Find person by ID. Returns None if not found."""

def search_by_name(needle: str, limit: int = 20) -> list[PersonDTO]:
    """
    Search persons by name (case-insensitive contains).
    
    Args:
        needle: Search string (case-insensitive)
        limit: Maximum results (default 20, max 100)
    
    Returns:
        List of PersonDTO sorted by name
    """
```

## CLI Usage

### Mandate Query

```bash
# SPD-Mandate im Landtag Niedersachsen 2014-2020
docker compose run --rm scraper scraper mandates \
  --parliament NI \
  --party SPD \
  --from 2014-01-01 \
  --to 2020-12-31 \
  --limit 50

# JSON Output
docker compose run --rm scraper scraper mandates \
  --parliament NI \
  --party SPD \
  --json

# Mit Person-Name Filter
docker compose run --rm scraper scraper mandates \
  --person-name "Weil" \
  --limit 10
```

### Legislature Stats

```bash
# Statistiken für eine Legislature
docker compose run --rm scraper scraper legislature-stats \
  --legislature-id 7219e8b8-3d63-59ae-823e-df5a7a0d2253 \
  --json
```

### Person Lookup

```bash
# Person nach ID
docker compose run --rm scraper scraper person \
  --id person-123

# Person nach Name suchen
docker compose run --rm scraper scraper person \
  --name "Stephan Weil" \
  --limit 10
```

## Response Beispiel

### MandateQueryResult (JSON)

```json
{
  "rows": [
    {
      "person_id": "person-123",
      "person_name": "Stephan Weil",
      "wikipedia_title": "Stephan_Weil",
      "mandate_id": "mandate-456",
      "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
      "legislature_name": "17. Landtag Niedersachsen",
      "parliament_id": "NI",
      "start_date": "2013-01-20",
      "end_date": "2017-11-14",
      "party_code": "SPD",
      "evidence_urls": [
        "https://de.wikipedia.org/w/index.php?title=Liste_der_Mitglieder_des_Niedersächsischen_Landtages_(17._Wahlperiode)&oldid=256198867"
      ]
    }
  ],
  "total": 1,
  "applied_filter": {
    "parliament_id": "NI",
    "party_code": "SPD",
    "from_date": "2014-01-01",
    "to_date": "2020-12-31",
    "limit": 50,
    "offset": 0,
    "sort": "person_name",
    "sort_direction": "ASC"
  }
}
```

## Akzeptanzkriterien (Definition of Done)

✅ **Es gibt typed DTOs** für Filter/Rows/Results/Stats

✅ **MandateQueryServiceInterface::search()** liefert reproduzierbare Ergebnisse inkl. `evidence_urls`

✅ **Zeitlogik ist zentral** und getestet (Overlap-Logik)

✅ **Es gibt mindestens 1 CLI-Adapter** zum manuellen Testen (`scraper mandates`, `scraper legislature-stats`, `scraper person`)

✅ **Tests grün** (Unit-Tests für DTOs, Filter-Validierung, Overlap-Logik)

✅ **Evidence-by-default**: Jede Zeile hat `evidence_urls` (nie null, dedupliziert)

✅ **Stable Sorting**: Deterministische Reihenfolge (Primary + Secondary Sort)

✅ **Pagination & Limits**: Schutz vor unbounded queries (max 1000)

## Fehlerbehandlung

### Invalid Filter

Bei ungültigem Filter wird `ValueError` oder `DomainException` geworfen:

```python
# Ungültiges Datum
filter_obj = MandateQueryFilter(from_date="2020-12-31", to_date="2020-01-01")
# → ValueError: to_date must be >= from_date
```

### Query Execution Errors

Bei DB-Fehlern wird `QueryExecutionException` geworfen:

```python
try:
    result = service.search(filter_obj)
except QueryExecutionException as e:
    # Handle error
```

## Integration mit LangGraph/LLM

Die Service-Interfaces sind 1:1 als LangGraph Tools verwendbar:

```python
from langchain.tools import Tool
from scraper.services import Neo4jMandateQueryService

mandate_service = Neo4jMandateQueryService(settings)

mandate_tool = Tool(
    name="search_mandates",
    description="Search mandates with filters (parliament, party, date range, person)",
    func=lambda **kwargs: mandate_service.search(
        MandateQueryFilter(**kwargs)
    ).model_dump(mode="json"),
)
```

Die Tools liefern deterministische, evidence-basierte Ergebnisse, die direkt von LLMs verwendet werden können.

