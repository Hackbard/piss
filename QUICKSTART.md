# Quick Start Guide

## Kompletter Workflow: Bundestag + Landtag Daten laden

### Schritt 1: Seeds für alle Landtage automatisch entdecken

```bash
# Services starten
docker compose up -d neo4j meilisearch

# Seeds für alle 16 Landtage automatisch entdecken
docker compose run --rm scraper scraper seed --discover --landtage --pin-revisions
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
docker compose run --rm scraper scraper pipeline \
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
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages \
  --force
```

**Ohne Personenseiten (schneller, aber weniger Daten):**
```bash
docker compose run --rm scraper scraper pipeline \
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

### Schritt 3: Daten prüfen

```bash
# Neo4j: Canonical Persons zählen
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (c:CanonicalPerson) RETURN count(c) as canonical_count"

# Neo4j: Link Assertions prüfen
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (a:PersonLinkAssertion) RETURN a.status, count(a) as count"

# Meilisearch: Personen suchen
curl "http://localhost:7700/indexes/persons/search" \
  -H "Authorization: Bearer masterKey" \
  -H "Content-Type: application/json" \
  --data-binary '{"q": "Merkel"}'
```

## Verschiedene Szenarien

### Szenario 1: ALLES laden (empfohlen für vollständige Daten)

```bash
# Lädt ALLE Seeds (167+ Landtags-Mitgliederlisten) + ALLE DIP Wahlperioden (1-50)
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

**Ergebnis:**
- Alle 167+ Landtags-Mitgliederlisten
- Alle Bundestags-Personen (Wahlperioden 1-50)
- Alle Personenseiten (Intro, Geburtsdatum, etc.)
- Vollständige Reconciliation

### Szenario 2: Nur Bundestag (ohne Landtag)

```bash
# Lädt nur Bundestags-Daten (alle Wahlperioden 1-50)
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

**Ergebnis:**
- Alle Bundestags-Personen
- Keine Landtags-Daten

### Szenario 3: Nur ein einzelner Landtag

```bash
# Z.B. Berlin Abgeordnetenhaus, 1. Wahlperiode
docker compose run --rm scraper scraper pipeline \
  --seed be_ah_1 \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

**Ergebnis:**
- Nur eine Mitgliederliste (be_ah_1)
- Keine DIP-Daten
- Keine Reconciliation

### Szenario 4: Einzelner Landtag + bestimmte Bundestags-Wahlperioden

```bash
# Lädt einen Landtag + nur bestimmte Bundestags-Wahlperioden
docker compose run --rm scraper scraper pipeline \
  --seed be_ah_1 \
  --ingest-dip \
  --dip-wahlperiode "19,20" \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

**Ergebnis:**
- Eine Mitgliederliste (be_ah_1)
- Nur Bundestags-Wahlperioden 19 und 20
- Reconciliation zwischen diesem Landtag und den 2 Wahlperioden

## Konfiguration

### Environment-Variablen (`.env`)

```bash
# DIP API (für Bundestag)
DIP_API_KEY=your_api_key_here
DIP_BASE_URL=https://search.dip.bundestag.de/api/v1
DIP_MAX_WAHLPERIODE=50  # Maximum Wahlperiode (default: 50)

# Neo4j
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Meilisearch
MEILI_URL=http://meilisearch:7700
MEILI_MASTER_KEY=masterKey
```

### Registry anpassen

Die Registry `config/landtage_registry.yaml` kann angepasst werden:
- Suchqueries erweitern
- Neue Landtage hinzufügen
- Key-Prefixes ändern

Nach Änderungen: Discovery erneut ausführen.

## Troubleshooting

### Container-Rebuild nach Code-Änderungen

Wenn Code geändert wurde, muss der Container neu gebaut werden:
```bash
docker compose build scraper
```

**Warum:** Der Python-Code wird beim Build in das Image kopiert. Änderungen am Code sind erst nach einem Rebuild aktiv.

### Evidence Resolver findet keine Evidence-IDs

Wenn der Evidence Resolver keine Evidence-IDs findet:
1. **Pipeline neu laufen:** Die Evidence-IDs müssen erst generiert und im Index gespeichert werden
2. **Meilisearch leeren:** Falls alte Evidence-IDs vorhanden sind, die nicht mehr im Cache sind:
   ```bash
   docker compose exec meilisearch curl -X DELETE "http://localhost:7700/indexes/persons" -H "Authorization: Bearer masterKey"
   ```
3. **Pipeline erneut ausführen:** Siehe "Vollständiger Befehl zum Neuaufbau" oben

### DIP_API_KEY fehlt
```bash
# Fehler: "DIP_API_KEY not set"
# Lösung: In .env setzen
```

### Cache leeren
```bash
# Cache-Verzeichnis löschen
rm -rf data/cache/*
```

### Seeds kombinieren
```bash
# Landtage-Seeds mit bestehenden Seeds kombinieren
cat config/seeds.yaml data/exports/seeds_landtage.yaml > config/seeds_combined.yaml
```

## Vollständiger Befehl zum Neuaufbau (ALLE Daten)

**Wichtig:** Nach Code-Änderungen muss der Container neu gebaut werden!

```bash
# 1. Container neu bauen (wichtig nach Code-Änderungen!)
docker compose build scraper

# 2. Services starten
docker compose up -d neo4j meilisearch

# 3. Seeds entdecken (nutzt Cache, findet ~167 Seeds)
docker compose run --rm scraper scraper seed --discover --landtage --pin-revisions

# 4. ALLES laden (ALLE Seeds + ALLE DIP Wahlperioden + ALLE Personenseiten)
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages

# 5. Evidence Resolver testen (Phase 3) - mit Row-level Citations
docker compose run --rm scraper scraper evidence --resolve-from-meili \
  --query "Stephan Weil" \
  --index persons \
  --limit 1 \
  --prefer table_row \
  --with-snippets \
  --format md
```

**Was wird geladen:**
- ✅ **167+ Landtags-Mitgliederlisten** (alle 16 Bundesländer, alle Wahlperioden)
- ✅ **Alle Bundestags-Personen** (Wahlperioden 1-50, konfigurierbar via `DIP_MAX_WAHLPERIODE`)
- ✅ **Alle Personenseiten** (Intro, Geburtsdatum, etc. für alle gefundenen Personen)
- ✅ **Vollständige Reconciliation** (Wikipedia ↔ DIP Identity Resolution)
- ✅ **Row-level Citations** (snippet_ref für Tabellenzeilen)

**Dauer:** Abhängig von Cache-Status. Mit Cache: ~5-10 Minuten. Ohne Cache: ~30-60 Minuten (wegen Rate-Limiting).

**Erwartete Ausgabe des Evidence Resolvers:**
- Evidence-Referenzen werden aus Meilisearch geladen (preferred: `evidence_refs` mit Row-level `snippet_ref`)
- Zwei Evidence-IDs werden gefunden (Mitgliederliste + Personenseite)
- **Mitgliederliste**: Zeigt `table_row` snippet mit der korrekten Tabellenzeile (Stephan Weil + SPD)
- **Personenseite**: Zeigt `lead_paragraph` snippet (Intro-Text)
- Canonical URLs mit `oldid` Parameter für Reproduzierbarkeit
- Snippets werden extrahiert und bereinigt (ohne Fußnoten-Marker)
- Markdown-Format mit vollständiger Provenance (revision_id, sha256, retrieved_at, purpose, snippet_ref)

**Beispiel-Output für "Stephan Weil":**
```
Found 2 evidence references from Meilisearch (preferred)

- Evidence `98a37cb9-1cc5-51a1-a51e-5992856c4fa0`
  - **Source**: mediawiki
  - **Page**: Liste der Mitglieder des Niedersächsischen Landtages (17. Wahlperiode)
  - **Revision**: 256198867
  - **URL**: https://de.wikipedia.org/w/index.php?title=Liste_der_Mitglieder_des_Niedersächsischen_Landtages_(17._Wahlperiode)&oldid=256198867
  - **Retrieved**: 2024-01-15T10:30:00Z
  - **SHA256**: `a1b2c3d4e5f6...`
  - **Snippet**: "Stephan Weil | SPD | Wahlkreis Hannover-Linden | ..."
  - **Snippet Source**: table_row
  - **Purpose**: membership_row
  - **Snippet Ref**: ```json
    {
        "version": 1,
        "type": "table_row",
        "table_index": 0,
        "row_index": 5,
        "row_kind": "data",
        "title_hint": "Liste_der_Mitglieder_des_Niedersächsischen_Landtages_(17._Wahlperiode)",
        "match": {
            "person_title": "Stephan_Weil",
            "name_cell": "Stephan Weil"
        }
    }
    ```

- Evidence `b2c3d4e5-f6a7-89b0-c1d2-e3f4a5b6c7d8`
  - **Source**: mediawiki
  - **Page**: Stephan Weil
  - **Revision**: 245123456
  - **URL**: https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=245123456
  - **Retrieved**: 2024-01-15T10:31:00Z
  - **SHA256**: `b2c3d4e5f6a7...`
  - **Snippet**: "Stephan Weil (* 15. März 1958 in Hamburg) ist ein deutscher Politiker (SPD). Seit 2013 ist er Ministerpräsident von Niedersachsen..."
  - **Snippet Source**: lead_paragraph
  - **Purpose**: person_page_intro
```

**Wichtig:**
- Die **Mitgliederlisten-Evidence** zeigt jetzt die **korrekte Tabellenzeile** von Stephan Weil (nicht die letzte verarbeitete Zeile)
- `snippet_source: table_row` bedeutet, dass die spezifische Zeile aus der Tabelle extrahiert wurde
- `purpose: membership_row` zeigt, dass dies die Mitgliedschafts-Referenz ist
- `snippet_ref` enthält die vollständige Row-Level-Referenz (table_index, row_index, match-Informationen)

