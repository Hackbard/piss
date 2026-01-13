# Implementierungs-Zusammenfassung

## Übersicht

Alle 5 Aufgaben wurden implementiert:

1. ✅ **Datenvertrag (Schema/Entities/IDs/Zeitlogik)**
2. ✅ **Provenance / Evidence als First-Class**
3. ✅ **QA-Gates (Validator + Reports + Exit Codes)**
4. ✅ **Golden Questions – Test Harness (Regression)**
5. ✅ **Dokumentation**

## Neue/angepasste Dateien

### Models & Domain

- `src/scraper/models/domain.py`
  - **Parliament** Entity hinzugefügt
  - **Party.code** Feld hinzugefügt (Primary Key)
  - **Person.normalized_name** hinzugefügt
  - **Mandate.parliament_id** hinzugefügt (required)
  - **Mandate.party_code** hinzugefügt (statt party_name)
  - **Mandate.start_date** ist jetzt **nullable** (day-only oder `null`) – Backfill erfolgt aus `Legislature.start_date` sobald day-precision vorliegt
  - **Evidence** Entity erweitert (url, retrieved_at, content_hash, source_type, locator, snapshot_path)

### Legislature Dates (STRICT / Day-Only)

- `src/scraper/utils/day_only_dates.py` (NEU)
  - `apply_day_only_date()` kapselt die Write-Policy: **nur day-Precision** wird als `*_date` persistiert, sonst `*_raw` + `*_precision`
  - `assert_day_invariant()` fail-fast wenn ein Datum gesetzt ist, aber `*_precision != "day"`

- `src/scraper/parsers/legislature_dates.py` (NEU/erweitert)
  - Extrahiert `start_date`/`end_date` **nur** wenn explizit day-precise belegbar, sonst `*_raw` + `*_precision`

- `src/scraper/sinks/neo4j.py`
  - `Legislature` erweitert: `term_number`, `*_precision`, `*_raw`, `*_source`, `source_url`, `wikipedia_title`
  - `LegislatureTerm` Node + `(:Legislature)-[:HAS_TERM]->(:LegislatureTerm)` Upserts

- `langgraph_app/cli.py`
  - `list-missing-legislature-starts` (Enrichment Queue, JSON/CSV)
  - `ingest-official-terms` (Registry → `LegislatureTerm`)
  - `ingest-wikidata-term` (revision-pinned Wikidata → `LegislatureTerm`)
  - `propagate-legislature-starts` (best-ranked Term → `Legislature` + Mandate-Backfill)

- `langgraph_app/sources/official_sources.yaml` (NEU)
  - Registry für offizielle Quellen (day-only konstituierende Sitzung)

- `langgraph_app/sources/official_registry.py`, `langgraph_app/sources/wikidata_terms.py` (NEU)
  - Loader/Parser für Registry und Wikidata-Terms (Precision Handling)

### Utilities

- `src/scraper/utils/intervals.py` (NEU)
  - `interval_overlaps()` - Zeitintervall-Überschneidungen prüfen
  - `parse_date_iso()` - ISO-Datum parsen
  - `filter_mandates_by_overlap()` - Mandate nach Zeitbereich filtern

- `src/scraper/utils/ids.py`
  - `generate_parliament_id()` hinzugefügt
  - `generate_legislature_id()` angepasst (nutzt jetzt parliament_id)
  - `generate_party_id()` angepasst (nutzt jetzt party_code)

### Validation

- `src/scraper/validation/validator.py` (NEU)
  - `DataValidator` Klasse
  - `ValidationResult` Klasse
  - Validierungsregeln für Mandate
  - Overlap-Detection
  - Duplicate-Detection

- `src/scraper/validation/__init__.py` (NEU)

### CLI

- `src/scraper/cli.py`
  - `validate` Command hinzugefügt
  - Unterstützt `--from`, `--to`, `--parliament`, `--strict`, `--json`

### Neo4j

- `src/scraper/sinks/neo4j.py`
  - Parliament Constraint hinzugefügt
  - Party.code Constraint hinzugefügt
  - Mandate Indexes hinzugefügt (person_id, legislature_id, parliament_id, party_code, start_date, end_date)

### Tests

- `tests/test_interval_overlaps.py` (NEU)
  - Unit Tests für `interval_overlaps()`
  - Unit Tests für `parse_date_iso()`
  - Unit Tests für `filter_mandates_by_overlap()`

- `tests/test_validator.py` (NEU)
  - Unit Tests für Validator
  - Tests für alle Validierungsregeln

- `tests/test_golden_questions.py` (NEU)
  - Golden Questions Regression Tests
  - Fixtures für "17. Landtag Niedersachsen"
  - Tests für SPD im Landtag NDS 2014-2020
  - Tests für Evidence-URLs
  - Tests für offene Mandate
  - Tests für Parteiwechsel

### Dokumentation

- `docs/data-contract.md` (NEU)
  - Entities und Pflichtfelder
  - Zeitlogik
  - Constraints und Indexes
  - ID-Generierung
  - Abfrage-Beispiele

- `docs/provenance.md` (NEU)
  - Evidence-Entity
  - EvidenceRef
  - Verknüpfungen
  - Query-Konvention
  - Hashing
  - Retrieval
  - Reproduzierbarkeit

- `docs/qa-gates.md` (NEU)
  - Validator-Regeln
  - CLI Command
  - Exit Codes
  - Output-Format
  - CI/CD Integration
  - Best Practices

## How to Run

### 1. Constraints/Migrations anwenden

Die Neo4j Constraints und Indexes werden automatisch beim ersten Aufruf von `Neo4jSink.init()` erstellt:

```bash
docker compose run --rm scraper scraper pipeline --write-neo4j
```

Oder manuell:

```python
from scraper.sinks.neo4j import Neo4jSink
from scraper.config import get_settings

settings = get_settings()
sink = Neo4jSink(settings)
sink.init()  # Erstellt Constraints und Indexes
```

### 2. Validator laufen lassen

```bash
# Basis-Validierung
docker compose run --rm scraper scraper validate

# Mit Filtern
docker compose run --rm scraper scraper validate --from 2014-01-01 --to 2020-12-31 --parliament NI

# Strict Mode (Missing Evidence = ERROR)
docker compose run --rm scraper scraper validate --strict

# JSON Output
docker compose run --rm scraper scraper validate --json
```

### 3. Tests laufen lassen

```bash
# Alle Tests
pytest

# Spezifische Tests
pytest tests/test_interval_overlaps.py
pytest tests/test_validator.py
pytest tests/test_golden_questions.py

# Mit Coverage
pytest --cov=src/scraper --cov-report=html
```

## Breaking Changes

### Models

⚠️ **WICHTIG**: Die Model-Änderungen sind Breaking Changes für bestehenden Code:

1. **Mandate.party_name** → **Mandate.party_code**
   - Alte Code-Stellen müssen angepasst werden
   - `party_name` wird nicht mehr unterstützt

2. **Mandate.start_date**: Domain erlaubt `null`, aber der Validator (`scraper validate`) behandelt fehlende `start_date` weiterhin als ERROR
   - Das ist beabsichtigt: fehlende day-only Startdaten müssen über Term-Enrichment/Propagation nachgezogen werden

3. **Legislature** hat jetzt **parliament_id** statt `parliament` (string)
   - Alte Code-Stellen müssen angepasst werden

4. **Party.code** ist jetzt **required** und Primary Key
   - Bestehende Parties ohne `code` müssen ergänzt werden

5. **Evidence** Entity hat neue Felder
   - Legacy-Felder (`endpoint_kind`, `page_title`, etc.) sind optional für Backward Compatibility

### Migration

Für bestehende Daten:

1. **Party.code** ergänzen:
   ```cypher
   MATCH (p:Party)
   WHERE p.code IS NULL
   SET p.code = upper(split(p.name, " ")[0])
   ```

2. **Mandate.party_code** aus `party_name` ableiten:
   ```cypher
   MATCH (m:Mandate)
   WHERE m.party_code IS NULL AND m.party_name IS NOT NULL
   MATCH (p:Party)
   WHERE p.name = m.party_name
   SET m.party_code = p.code
   ```

3. **Parliament** Entities erstellen und **Legislature.parliament_id** setzen:
   ```cypher
   MATCH (l:Legislature)
   MERGE (p:Parliament {name: l.parliament, level: "state"})
   SET l.parliament_id = p.id
   ```

## Nächste Schritte

1. **Migration Script**: Erstellen eines Scripts für die Datenmigration
2. **Backward Compatibility**: Prüfen ob bestehender Code angepasst werden muss
3. **Integration Tests**: Erweitern der Tests für vollständige Pipeline
4. **Import-Run Meta**: Optional implementieren für bessere Nachvollziehbarkeit

## Akzeptanzkriterien

✅ **Aufgabe 1**: Entities + Pflichtfelder + Zeitlogik dokumentiert und im Code klar erkennbar
✅ **Aufgabe 1**: Constraints/Indexes idempotent (wiederholte Imports destabilisieren nicht)
✅ **Aufgabe 2**: Für jede Mandatszeile kann Evidence mitgeliefert werden
✅ **Aufgabe 2**: Evidence ist persistiert und referenzierbar
✅ **Aufgabe 3**: Validator läuft lokal reproduzierbar
✅ **Aufgabe 3**: CI kann damit "Import ok" vs "Import broken" unterscheiden
✅ **Aufgabe 4**: `pytest` ist grün
✅ **Aufgabe 4**: Goldfrage schlägt fehl, wenn Zeitlogik/Provenance kaputtgeht
✅ **Aufgabe 5**: Docs sind konkret mit Beispielen für Abfragen/Outputs

