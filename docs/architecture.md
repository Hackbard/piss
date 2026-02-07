## PIS – Politisches Informations System

### Kontext & Ziele
PIS ist ein Daten- und Retrieval-Layer für **politische Akteure in Deutschland**. Das System sammelt Daten aus mehreren Quellen, normalisiert sie, konsolidiert sie in **genau 1 kanonischen Person-Datensatz pro realer Person** und stellt diese Datensätze anschließend **RAG-fähig** (strukturierte Facts + suchbarer Text + Filter/Metadaten) via **Meilisearch** bereit.

Nicht verhandelbar:
- **Keine stillen Duplikate** im kanonischen Person-Index (Dupe-Kandidaten werden reportet, nicht indexiert).
- **Provenance** ist immer nachvollziehbar (Quelle, IDs, fetched_at, Snapshot-Pfade).
- Reproduzierbarkeit: Pipeline ist **raw → normalized → canonical → indexed**.

### Datenfluss (ETL/ELT)
Wir nutzen eine deterministische, cache-basierte ETL-Pipeline:

1) **Ingest (Raw)**
- Fetcher pro Quelle lädt Rohdaten (JSON/HTML) mit Retries, Rate-Limit, Cache.
- Rohdaten werden als append-only Snapshots abgelegt (z.B. `data/raw/.../*.json` plus Metadaten).

2) **Normalize (Source → Canonical Shape)**
- Quelle-spezifische Mapper erzeugen Normalized Records im kanonischen Format (ohne Dedupe).
- Output als JSONL Snapshots (z.B. `data/normalized/{source}/persons.jsonl`).

3) **Reconcile (Entity Resolution / Dedupe / Merge)**
- Vereinheitlichung zu `Person` (kanonisch) mit deterministischen Merge-Regeln.
- Konflikte werden nicht “weggebügelt”, sondern als Konfliktobjekte/Reports persistiert.
- Output: `data/canonical/persons.jsonl` + Reports (Dupe-Kandidaten, Missing, Temporal Issues).

4) **Index (Meilisearch)**
- Kanonische Dokumente werden idempotent upserted.
- Index-Settings (filterable/sortable/searchable) werden beim Run gesetzt/validiert.

5) **RAG Dokumentbau**
- Pro Person wird ein RAG-Dokument erzeugt: `persona_summary` + `facts` + `provenance`.
- Retrieval liefert kompakte Kontext-Pakete für LLMs (Top-k + Filter + Evidence-Links).

### Komponentenübersicht
Die PIS-Schichten (neu) sind bewusst getrennt, um Quellen austauschbar zu halten:
- `pis/ingest/`: Fetcher/Clients je Quelle (Wikidata/Wikipedia, DIP/weitere Bundesquellen)
- `pis/normalize/`: Parser/Mapper je Quelle → Normalized Models
- `pis/reconcile/`: Entity Resolution + Merge + Reports
- `pis/index/`: Meilisearch Index Writer + Settings
- `pis/rag/`: Dokumentbau + Retrieval Helper
- `pis/models.py`: Kanonisches Datenmodell (Pydantic)

Das Repository enthält bereits Infrastruktur (Caching/Provenance, DIP-Connector, Meili-Sink). PIS baut darauf auf und führt eine **kanonische Person** als zentralen Aggregationspunkt ein.

### Konsistenz- und Dedup-Strategie
**Kanonische Identität**
- Primär: `wikidata_qid` (falls vorhanden) → deterministischer `pis_person_id`.
- Sekundär (wenn QID fehlt): mehrstufige Heuristik mit hoher Präzision:
  - Name (normalisiert) + Geburtsdatum (day-precision) + weitere Anker (Wikipedia pageid/url, DIP person id)
  - Nur **eindeutige** Matches werden automatisch gemerged, sonst `pending` Dupe-Kandidat.

**Merge-Regeln (High-level)**
- Quellen-Trust: Official Bundesquellen (z.B. DIP) > Wikidata > Wikipedia-Text.
- Fakten bleiben quellenspezifisch nachvollziehbar: jedes Feld bekommt mindestens eine Source-Referenz.
- Konflikte werden als Struktur persistiert (z.B. `conflicts[]` in Canonical Output und/oder `reports/conflicts.jsonl`).

**Validierungen / Reports**
- Dupe-Kandidaten (Score, Begründung, betroffene Quellen/IDs)
- Missing Coverage (z.B. “DIP Personen ohne Wikidata/Wikipedia Link” und umgekehrt)
- Zeitachsenprobleme (Überlappungen, Lücken, inverted intervals) für Mandate/Rollen

### Meilisearch Index-Strategie
Ziel: Keyword-Suche + Filter/Sort für RAG und Tooling.

**Index `pis_persons` (kanonisch, RAG-ready)**
- **Primary key**: `pis_person_id`
- **SearchableAttributes**:
  - `display_name`, `aliases[]`
  - `persona_summary`
  - `facts.party_names[]`, `facts.offices[]`, `facts.mandates[]` (textualized)
- **FilterableAttributes** (Beispiele):
  - `external_ids.wikidata_qid`
  - `external_ids.wikipedia_pageid`
  - `external_ids.dip_person_id`
  - `facts.parliament_types[]` (bund/land/bundesrat)
  - `facts.state_codes[]`
  - `facts.party_codes[]`
  - `facts.active_from`, `facts.active_to` (für Range-Filter; zusätzlich als ISO strings)
  - `provenance.sources[].source_system`
- **SortableAttributes**:
  - `display_name`
  - `facts.active_from`, `facts.active_to`
  - `meta.updated_at`

**Index `pis_memberships` (Mandate/Mitgliedschaften)**
- Primary key: `pis_membership_id`
- Filter: `pis_person_id`, `parliament_type`, `parliament_code`, `state_code`, `party_code`, `legislature_period_id`
- Sort: `start_date`, `end_date`

**Index `pis_legislature_periods`**
- Primary key: `pis_legislature_period_id`
- Filter: `parliament_type`, `parliament_code`, `state_code`, `term_number`
- Sort: `start_date`, `election_date`

Optional (später):
- `pis_office_roles` für exekutive Rollen als eigene indexierbare Einheiten
- `pis_conflicts` / `pis_reports` für QA-UI/Monitoring

# PIS – Politisches Informations System (Deutschland)

## Kontext & Ziele

PIS baut einen **Daten- und Retrieval-Layer** für politische Akteure in **Deutschland**. Daten werden aus mehreren Quellen ingestiert, normalisiert und zu **genau einem kanonischen Person-Datensatz pro realer Person** konsolidiert. Die kanonischen Datensätze werden anschließend in **Meilisearch** für Retrieval (RAG) indexiert.

**Nicht verhandelbar:**
- **1 Person = 1 kanonischer Datensatz** (Duplikate werden verhindert; Kandidaten werden reportet).
- **Provenance transparent**: Jede kanonische Aussage ist über Quellen/IDs/Timestamps nachvollziehbar.
- **Reproduzierbarkeit**: Pipeline ist deterministisch und wiederholbar (raw → normalized → canonical → indexed).

## Datenfluss (ETL/ELT)

**Stage 0 – Fetch (Ingestion):**
- Quelle-spezifische Fetcher laden Rohdaten (HTTP).
- Responses werden **dateibasiert gecacht** (inkl. Metadaten, Hash, fetched_at).
- Output: `data/raw/<source>/<run_id>/*.jsonl` (oder cache-basierte Ablage)

**Stage 1 – Normalize:**
- Quelle-spezifische Mapper erzeugen **Normalized Snapshots** im kanonischen Shape (noch nicht dedupliziert).
- Output: `data/normalized/<source>/<run_id>/*.jsonl`

**Stage 2 – Reconcile (Entity Resolution):**
- Normalized Snapshots werden zu **Canonical Persons** zusammengeführt.
- Output:
  - `data/canonical/<run_id>/persons.jsonl`
  - `data/reports/<run_id>/*` (Dupe-Kandidaten, Konflikte, Coverage-Gaps)

**Stage 3 – Index (Meilisearch):**
- Canonical Persons (plus abgeleitete RAG-Dokumente) werden idempotent in Meilisearch upserted.
- Output: Meili Indizes + Run-Manifest

## Komponentenübersicht

- `pis/ingest/`: Connectoren je Quelle (Wikidata/Wikipedia, DIP/Bundesquellen, …)
- `pis/normalize/`: Normalizer/Mapper (rohe Quellpayloads → Normalized Models)
- `pis/reconcile/`: Dedup + Merge + Konfliktmanagement + Reports
- `pis/index/`: Meilisearch Index Writer + Index Settings
- `pis/rag/`: Dokument-Bau (persona_summary, facts) + Retrieval Helper

## Konsistenz- & Dedup-Strategie

### Identitätsanker (Priority Order)
1. **Wikidata QID** (`wikidata_qid`) ⇒ starker Primäranker
2. **Bundesquelle/DIP Person-ID** (`dip_person_id` / `bundes_source_id`)
3. **Wikipedia Page ID/URL** (`wikipedia_pageid`, pinned revision wenn verfügbar)
4. **Heuristik**: normalisierter Name + Geburtsdatum + Funktionskontext (Mandate/Rollen)  

### Merge-Regeln (kanonisch)
- **Keine stille Überschreibung**: Bei Konflikten (z.B. verschiedene Geburtsdaten) werden
  - bevorzugte Quelle nach Policy gewählt (z.B. official > wikidata > wikipedia),
  - und der Konflikt als strukturierter Eintrag gespeichert (inkl. Quellenwerte).
- **Dupe Prevention**: vor Indexierung wird geprüft, ob ein `pis_person_id` bereits existiert.
  - Falls mehrere Canonical Candidates entstehen: **nicht indexieren**, sondern reporten.

### Validierungs-Reports (Minimum)
- Duplikate-Kandidaten (match score, Grund, betroffene IDs)
- Missing-by-source (z.B. in DIP vorhanden, aber nicht in Wikidata/Wikipedia – soweit vergleichbar)
- Zeitachsen-Probleme (Überlappungen/Lücken bei Mandaten/Rollen pro Person)

## Meilisearch Index-Strategie

### Indizes
- `pis_persons` (primär, RAG Entry Point)
- `pis_legislature_periods` (Filter- und Kontextdaten)
- optional später: `pis_mandates`, `pis_office_roles` (wenn separate Retrievalpfade nötig)

### `pis_persons` Dokument (high-level)
**Document ID**: `pis_person_id` (string, stabil)

**Searchable attributes (Vorschlag):**
- `display_name`
- `name_variants[]`
- `persona_summary`
- `facts_text` (flacher, gut suchbarer Text; aus strukturierten Facts generiert)
- `party_affiliations[]` (optional)

**Filterable attributes (Vorschlag):**
- `external_ids.wikidata_qid`
- `external_ids.wikipedia_pageid`
- `external_ids.dip_person_id`
- `active_in.parliament_types[]` (`BUND`, `LAND`, `BUNDESRAT`)
- `active_in.states[]` (nur bei LAND)
- `time_range.first_seen`, `time_range.last_seen` (ISO date)

**Sortable attributes (Vorschlag):**
- `display_name`
- `time_range.first_seen`
- `time_range.last_seen`

**Faceting (Vorschlag):**
- `active_in.parliament_types`
- `active_in.states`
- `party_affiliations`

### `pis_legislature_periods`
**Document ID**: stable key \(z.B. `${parliament_type}:${state}:${term}`\)

**Filterable attributes:**
- `parliament_type`
- `state` (nullable)
- `term_number`
- `start_date`, `end_date`, `election_date`

