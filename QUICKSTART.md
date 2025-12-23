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

### Schritt 2: Bundestag + Landtag Daten laden

```bash
# Environment-Variablen prüfen (falls noch nicht geschehen)
# DIP_API_KEY muss in .env gesetzt sein

# Pipeline mit Bundestag (alle Wahlperioden) + Landtag (alle Seeds) ausführen
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages \
  --force
```

**Was passiert:**
1. **DIP Ingest**: Lädt alle Bundestags-Personen (Wahlperioden 1-50, konfigurierbar via `DIP_MAX_WAHLPERIODE` in `.env`)
2. **Wikipedia Scraping**: Lädt alle Landtags-Mitgliederlisten aus Wikipedia (basierend auf Seeds)
3. **Reconciliation**: Führt Wikipedia- und DIP-Personen zusammen (Identity Resolution)
4. **Sinks**: Speichert in Neo4j und Meilisearch

**Ohne `--force` (idempotent, nutzt Cache):**
```bash
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

**Hinweis:** `--fetch-person-pages` (default: aktiviert) lädt auch einzelne Personenseiten für Intro, Geburtsdatum, etc. Ohne `--no-fetch-person-pages` ist es schneller, aber weniger Daten.

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

## Einzelne Schritte

### Nur Bundestag (ohne Landtag)

```bash
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

### Nur ein einzelner Landtag

```bash
# Z.B. Berlin Abgeordnetenhaus, 1. Wahlperiode
docker compose run --rm scraper scraper pipeline \
  --seed be_ah_1 \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

### Nur bestimmte Bundestags-Wahlperioden

```bash
docker compose run --rm scraper scraper pipeline \
  --seed be_ah_1 \
  --ingest-dip \
  --dip-wahlperiode "19,20" \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages
```

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

## Vollständiger Befehl zum Neuaufbau (alle Phasen)

**Wichtig:** Nach Code-Änderungen muss der Container neu gebaut werden!

```bash
# 1. Container neu bauen (wichtig nach Code-Änderungen!)
docker compose build scraper

# 2. Services starten
docker compose up -d neo4j meilisearch

# 3. Seeds entdecken (nutzt Cache)
docker compose run --rm scraper scraper seed --discover --landtage --pin-revisions

# 4. Alles laden (Phase 1 + 2 + 3)
docker compose run --rm scraper scraper pipeline \
  --ingest-dip \
  --reconcile \
  --write-neo4j \
  --write-meili \
  --fetch-person-pages

# 5. Evidence Resolver testen (Phase 3)
docker compose run --rm scraper scraper evidence --resolve-from-meili \
  --query "Stephan Weil" \
  --index persons \
  --limit 1 \
  --with-snippets \
  --format md
```

**Erwartete Ausgabe des Evidence Resolvers:**
- Zwei Evidence-IDs werden gefunden (Mitgliederliste + Personenseite)
- Canonical URLs mit `oldid` Parameter für Reproduzierbarkeit
- Snippets werden extrahiert und bereinigt (ohne Fußnoten-Marker)
- Markdown-Format mit vollständiger Provenance (revision_id, sha256, retrieved_at)

**Beispiel-Output:**
```
- Evidence `98a37cb9-1cc5-51a1-a51e-5992856c4fa0`
  - **Source**: mediawiki
  - **Page**: Liste der Mitglieder des Niedersächsischen Landtages (17. Wahlperiode)
  - **Revision**: 256198867
  - **URL**: https://de.wikipedia.org/w/index.php?title=...&oldid=256198867
  - **Snippet**: "Liste der Mitglieder des 17. niedersächsischen Landtages..."
```

