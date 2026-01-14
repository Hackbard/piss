# Quick Start Guide

## Kompletter Workflow: Bundestag + Landtag Daten laden

### Schritt 1: Seeds für alle Landtage automatisch entdecken

```bash
# Services starten
docker compose up -d neo4j meilisearch

# Seeds für alle 16 Landtage automatisch entdecken
docker compose run --rm --build scraper scraper seed --discover --landtage --pin-revisions
```

**Was passiert:**
- Durchsucht Wikipedia nach Mitgliederlisten aller 16 Landtage
- Validiert, dass gefundene Seiten Member-Listen enthalten (Name/Partei/Wahlkreis)
- Erzeugt deterministische Seeds mit gepinnten `page_id` und `revision_id`
- **Output:** `data/exports/seeds_landtage.yaml` (~167 Seeds)

**Cache:** Alle Discovery-Requests werden gecacht. Zweiter Run ist idempotent.

### Schritt 2: ALLE Daten laden (Bundestag + alle Landtage)

**Schnellste Variante - lädt ALLES:**
```bash
# Environment-Variablen prüfen (falls noch nicht geschehen)
# DIP_API_KEY muss in .env gesetzt sein

# Pipeline OHNE --seed = lädt ALLE Seeds automatisch
docker compose run --rm --build scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

**Was passiert:**
1. **DIP Ingest**: Lädt **ALLE** Bundestags-Personen (Wahlperioden 1-50, konfigurierbar via `DIP_MAX_WAHLPERIODE` in `.env`)
2. **Wikipedia Scraping**: Lädt **ALLE** Landtags-Mitgliederlisten aus Wikipedia (alle 167+ Seeds automatisch)
3. **Personenseiten**: Lädt für **ALLE** Personen die individuellen Wikipedia-Seiten (Intro, Geburtsdatum, etc.)
4. **Reconciliation**: Führt Wikipedia- und DIP-Personen zusammen (Identity Resolution)
5. **Sinks**: Speichert alles in Neo4j und Meilisearch

**Mit `--force` (ignoriert Cache, lädt alles neu):**
```bash
docker compose run --rm --build scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages \
  --force
```

**Ohne Personenseiten (schneller, aber weniger Daten):**
```bash
docker compose run --rm --build scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --no-fetch-person-pages
```

**Hinweis:** 
- **Ohne `--seed`**: Lädt automatisch **ALLE** Seeds (alle 167+ Landtags-Mitgliederlisten)
- **Mit `--seed <key>`**: Lädt nur einen einzelnen Seed
- `--fetch-person-pages` (default: aktiviert) lädt auch einzelne Personenseiten für Intro, Geburtsdatum, etc.
- Ohne `--no-fetch-person-pages` ist es schneller, aber weniger Daten

### Schritt 2.5: Legislature-Startdaten vervollständigen (Teil vom Gesamt-Quickstart)

Damit `scraper validate` **grün** werden kann, müssen Mandate day-only `start_date` haben. Diese werden **nicht geschätzt**, sondern streng aus **day-precise** `Legislature.start_date` propagiert (konstituierende Sitzung / erste Sitzung). Wenn der Tag nicht belegt ist, bleibt `Legislature.start_date = null` und es wird nur `start_date_raw` + `start_date_precision` gespeichert.

Der komplette Ablauf besteht aus sechs Schritten:

```bash
# A) Wikipedia-Listen erneut parsen (oldid-pinned) → füllt source_url/wikipedia_title + RAW/PRECISION
docker compose run --rm --build scraper scraper repair-legislature-dates --limit 500

# B) Enrichment-Queue erzeugen → zeigt, welche Wahlperioden noch KEIN day-only start_date haben
docker compose run --rm --build scraper python -m langgraph_app.cli list-missing-legislature-starts \
  --format json

# C) Offizielle Quellen einpflegen (einmalig): langgraph_app/sources/official_sources.yaml
#    - Pro Parlament `terms[]` mit `term_number` + day-only `start_date` hinterlegen (konstituierende Sitzung / erste Sitzung)
#    - Wenn kein day-only Datum belegbar ist: start_date leer lassen (NULL bleibt korrekt)
docker compose run --rm --build scraper python -m langgraph_app.cli ingest-official-terms

# D) Wikidata-Terms einpflegen (optional, aber empfohlen): langgraph_app/sources/wikidata_mapping.yaml
#    - Mapping-Datei mit QIDs pro Parliament/Term befüllen
#    - Nur day-precision Terms werden verarbeitet (precision=11)
docker compose run --rm --build scraper python -m langgraph_app.cli ingest-wikidata-terms --all

# E) Propagieren: official > wikidata > wikipedia (nur precision == day)
#    → setzt Legislature.start_date und backfilled Mandate.start_date automatisch
docker compose run --rm --build scraper python -m langgraph_app.cli propagate-legislature-starts

# F) Mandate-IDs nach Backfill reparieren (verhindert Duplikate/Overlap-Errors im Validator)
docker compose run --rm --build scraper scraper repair-mandate-ids
```

### Schritt 3: Daten validieren

```bash
# Validator ausführen (prüft Datenqualität)
# Default (integrity mode): Missing start_date ist WARNING (completeness gap, nicht hard error)
# Nur Integrity-Fehler (Dateninkonsistenzen) blockieren die Pipeline
docker compose run --rm --build scraper scraper validate

# Mit Datumsfilter
docker compose run --rm --build scraper scraper validate --from 2014-01-01 --to 2020-12-31

# Mit Parliament-Filter
docker compose run --rm --build scraper scraper validate --parliament NI

# Integrity Mode (explizit, Standard)
docker compose run --rm --build scraper scraper validate --mode integrity

# Completeness Mode (nur Completeness-Gaps)
docker compose run --rm --build scraper scraper validate --mode completeness

# All Mode (beide Arten von Fehlern blockieren)
docker compose run --rm --build scraper scraper validate --mode all

# Strict Completeness Mode (Legacy-Alias für --mode all)
docker compose run --rm --build scraper scraper validate --strict-completeness

# Strict Mode (Missing Evidence = ERROR)
docker compose run --rm --build scraper scraper validate --strict

# Kombiniert: Strict Evidence + Strict Completeness
docker compose run --rm --build scraper scraper validate --strict --strict-completeness

# JSON Output (für CI/CD): Reines JSON zu stdout, Logs zu stderr (jq-kompatibel)
docker compose run --rm scraper scraper validate --json | jq '{error_count, warning_count, meta: .meta}'

# Quiet Mode: Unterdrückt alle Logs (nur JSON Output)
docker compose run --rm scraper scraper validate --json --quiet | jq '.error_count'
```

**Was wird geprüft:**
- ✅ Fehlende `start_date` → **WARNING** (default/integrity mode) oder **ERROR** (mit `--mode all` / `--strict-completeness`)
- ✅ `end_date < start_date` → ERROR (Integrity-Fehler)
- ✅ Doppelte Mandate → ERROR (Integrity-Fehler)
- ✅ Überlappende Mandate (gleiche Partei) → ERROR (Integrity-Fehler)
- ✅ Überlappende Mandate (verschiedene Parteien) → WARN (Parteiwechsel)
- ✅ Unbekannte `party_code` → WARN
- ✅ Fehlende Evidence → WARN (oder ERROR im strict mode)
- ✅ `DATE_CANONICAL_WITHOUT_EVIDENCE` → ERROR (Integrity-Fehler)
- ✅ `DATE_CONFLICT` → ERROR (Integrity-Fehler) oder WARN (je nach Mode)

**Hinweis:** 
- **Default-Modus**: Missing `start_date` ist ein **WARNING** (Completeness-Gap, kein Hard-Error). Dies erlaubt es, die Pipeline auch dann auszuführen, wenn noch nicht alle Term-Startdaten verfügbar sind.
- **Strict-Completeness-Modus**: Mit `--strict-completeness` werden Missing `start_date` als **ERROR** behandelt (für CI/CD-Gates, wenn 100% Coverage erforderlich ist).
- Wenn du noch keine day-only `Legislature.start_date` propagiert hast, sind Mandate oft noch ohne `start_date` → im Default-Modus gibt es Warnings, aber keine Errors.

**Exit Codes:**
- `0` = Keine Errors (Warnings sind OK)
- `2` = Mindestens ein ERROR (CI-tauglich)

### Schritt 4: Daten prüfen

```bash
# Neo4j: Canonical Persons zählen
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (c:CanonicalPerson) RETURN count(c) as canonical_count"

# Neo4j: Link Assertions prüfen
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (a:PersonLinkAssertion) RETURN a.status, count(a) as count"

# Neo4j: Legislatures ohne start_date zählen
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (l:Legislature) WHERE l.start_date IS NULL RETURN count(l) AS missing_leg_start"

# Neo4j: Welche Parlamente/Terms blockieren die meisten Mandate?
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature) WHERE m.start_date IS NULL RETURN l.parliament_id AS parliament_id, coalesce(l.term_number, -1) AS term_number, coalesce(l.name, l.parliament) AS legislature, count(m) AS mandates_missing_start ORDER BY mandates_missing_start DESC LIMIT 50"

# Neo4j: Legislatures mit start_date zählen (Validierung der Propagation)
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (l:Legislature) WHERE l.start_date IS NOT NULL RETURN l.parliament_id AS parliament_id, count(l) AS legislatures_with_start ORDER BY legislatures_with_start DESC"

# Enrichment-Queue: Welche Terms brauchen noch Quellen?
docker compose run --rm --build scraper python -m langgraph_app.cli list-missing-starts --format json

# Meilisearch: Personen suchen
curl "http://localhost:7700/indexes/persons/search" \
  -H "Authorization: Bearer masterKey" \
  -H "Content-Type: application/json" \
  --data-binary '{"q": "Merkel"}'
```


## Konfiguration

Siehe [README.md](README.md) für vollständige Liste der Environment-Variablen.


## Dokumentation

Weitere Details:

- **Data Contract**: `docs/data-contract.md` - Entities, Zeitlogik, Constraints
- **Provenance**: `docs/provenance.md` - Evidence-Modell, Hashing, Reproduzierbarkeit
- **QA-Gates**: `docs/qa-gates.md` - Validator-Regeln, CLI, CI/CD Integration
- **Implementation Summary**: `docs/IMPLEMENTATION_SUMMARY.md` - Übersicht aller Änderungen

## Troubleshooting

### Container-Rebuild nach Code-Änderungen

Wenn Code geändert wurde, muss der Container neu gebaut werden:
```bash
docker compose build scraper
```

### Validator findet Legacy-Daten

Wenn der Validator Legacy-Daten findet (ohne `parliament_id` oder `code`):
- Validator zeigt Warnungen für übersprungene Legacy-Daten
- **Lösung:** Neue Daten importieren (siehe "Vollständiger Befehl zum Neuaufbau" oben)

### Legislature-Daten fehlen

Wenn viele Mandate ohne `start_date` sind:
1. Prüfen, ob Legislatures `start_date` haben:
   ```bash
   docker compose exec neo4j cypher-shell -u neo4j -p password \
     "MATCH (l:Legislature) WHERE l.start_date IS NULL RETURN count(l) AS missing"
   ```
2. `repair-legislature-dates` ausführen (siehe Schritt 2.5)
3. Enrichment-Queue prüfen:
   ```bash
   docker compose run --rm --build scraper python -m langgraph_app.cli list-missing-starts --format json
   ```

### DIP_API_KEY fehlt
```bash
# Fehler: "DIP_API_KEY not set"
# Lösung: In .env setzen
```

## Vollständiger Befehl zum Neuaufbau (ALLE Daten)

**Wichtig:** Nach Code-Änderungen muss der Container neu gebaut werden!

```bash
# 1. Container neu bauen (wichtig nach Code-Änderungen!)
docker compose build scraper

# 2. Services starten
docker compose up -d neo4j meilisearch

# 3. Seeds entdecken (nutzt Cache, findet ~167 Seeds)
docker compose run --rm --build scraper scraper seed --discover --landtage --pin-revisions

# 4. ALLES laden (ALLE Seeds + ALLE DIP Wahlperioden + ALLE Personenseiten)
#    Cache wird automatisch genutzt - keine Duplikate!
docker compose run --rm --build scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages

# 5. Legislature-Startdaten vervollständigen (siehe Schritt 2.5)
docker compose run --rm --build scraper scraper repair-legislature-dates --limit 500
docker compose run --rm --build scraper python -m langgraph_app.cli ingest-official-terms
docker compose run --rm --build scraper python -m langgraph_app.cli ingest-wikidata-terms --all
docker compose run --rm --build scraper python -m langgraph_app.cli propagate-legislature-starts
docker compose run --rm --build scraper scraper repair-mandate-ids

# 6. Daten validieren (Default: Missing start_date = WARNING, nicht ERROR)
docker compose run --rm --build scraper scraper validate --json

# 7. Enrichment-Queue prüfen (welche Terms brauchen noch Quellen?)
docker compose run --rm --build scraper python -m langgraph_app.cli list-missing-starts --format json

# 8. Evidence Resolver testen - mit Row-level Citations
docker compose run --rm --build scraper scraper evidence --resolve-from-meili \
  --query "Stephan Weil" \
  --index persons \
  --limit 1 \
  --prefer table_row \
  --with-snippets \
  --format md
```

## Daten regenerieren (ohne Cache zu verlieren)

**Cache wird automatisch genutzt** - keine Duplikate, idempotente Imports!

```bash
# Container neu bauen (falls Code geändert wurde)
docker compose build scraper

# Pipeline läuft - nutzt automatisch Cache, schreibt nur in Neo4j/Meili neu
docker compose run --rm --build scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages

# Legislature-Daten aktualisieren
docker compose run --rm --build scraper scraper repair-legislature-dates --limit 500
docker compose run --rm --build scraper python -m langgraph_app.cli propagate-legislature-starts
docker compose run --rm --build scraper scraper repair-mandate-ids

# Validieren
docker compose run --rm --build scraper scraper validate
```

**Cache-Verhalten:**
- **Ohne `--force`**: Cache wird automatisch genutzt (empfohlen)
- **Mit `--force`**: Cache wird ignoriert, alles neu geladen (langsam, 30-60 Minuten)

