# Übersicht: Personendatenstrukturen in allen Datenbanken

## Datenbanken im System

Das Projekt verwendet **drei Hauptspeicherorte** für Personendaten:

1. **Neo4j** (Graph-Datenbank) - Primäre persistente Datenbank
2. **MeiliSearch** (Such-Index) - Volltextsuche und Filterung
3. **File Cache** (Dateisystem) - Evidenz-Cache und Rohdaten

---

## 1. Neo4j Graph-Datenbank

### Person-bezogene Node-Typen

#### `Person` (Haupt-Entität)
**Constraints:** `Person.id IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Deterministische ID
- `name` (string) - Vollständiger Name
- `wikipedia_title` (string, optional) - Wikipedia-Seitentitel
- `wikipedia_url` (string, optional) - Wikipedia URL
- `birth_date` (date, optional) - Geburtsdatum (nur wenn aus harten Quellen extrahiert)
- `birth_date_status` (string) - Status: "unknown", "extracted", "not_present"
- `death_date` (date, optional) - Todesdatum
- `intro` (string, optional) - Einleitungstext von Wikipedia
- `evidence_ids` (List[string]) - Legacy Evidence-IDs
- `data_quality_flags` (List[string]) - Qualitätsflags, z.B. ['missing_birth_date']

**Beziehungen:**
- `(Person)-[:HELD]->(Mandate)` - Person hält Mandat
- `(Person)-[:SUPPORTED_BY {purpose, snippet_ref_json}]->(Evidence)` - Evidenz-Referenzen

**Cypher-Beispiel:**
```cypher
MATCH (p:Person {id: $person_id})
OPTIONAL MATCH (p)-[:SUPPORTED_BY]->(e:Evidence)
RETURN p, collect(e.url) as evidence_urls
```

---

#### `WikipediaPersonRecord` (Quellen-Record)
**Constraints:** `WikipediaPersonRecord.id IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Basierend auf `wikipedia_title + revision`
- `wikipedia_title` (string) - Wikipedia-Seitentitel
- `wikipedia_url` (string) - Wikipedia URL
- `page_id` (int) - Wikipedia Page ID
- `revision_id` (int) - Revision ID (für Reproduzierbarkeit)
- `name` (string) - Extrahierter Name
- `birth_date` (date, optional) - Geburtsdatum
- `death_date` (date, optional) - Todesdatum
- `intro` (string, optional) - Einleitungstext
- `evidence_ids` (List[string]) - Evidence-IDs

**Beziehungen:**
- `(CanonicalPerson)-[:HAS_SOURCE]->(WikipediaPersonRecord)`

**Zweck:** Hält Wikipedia-spezifische Daten mit Revision-Pinning für Provenance.

---

#### `DipPersonRecord` (Quellen-Record)
**Constraints:** `DipPersonRecord.id IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Basierend auf `dip_person_id + payload sha256`
- `dip_person_id` (int) - DIP Person ID (Bundestag)
- `vorname` (string, optional) - Vorname
- `nachname` (string, optional) - Nachname
- `namenszusatz` (string, optional) - Namenszusatz
- `titel` (string, optional) - Titel
- `fraktion` (string, optional) - Partei/Fraktion
- `wahlperiode` (List[int]) - Wahlperioden
- `person_roles` (List[Dict], optional) - Person-Rollen
- `evidence_ids` (List[string]) - Evidence-IDs

**Beziehungen:**
- `(CanonicalPerson)-[:HAS_SOURCE]->(DipPersonRecord)`

**Zweck:** Hält DIP (Deutscher Bundestag) API-Daten mit Provenance.

---

#### `CanonicalPerson` (Zusammengeführte Entität)
**Constraints:** `CanonicalPerson.id IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Deterministische ID
- `display_name` (string) - Anzeigename
- `wikipedia_title` (string, optional) - Aus `identifiers`
- `wikipedia_page_id` (string, optional) - Aus `identifiers`
- `dip_person_id` (string, optional) - Aus `identifiers`
- `created_at` (string, optional) - Erstellungszeitpunkt (UTC ISO)
- `updated_at` (string, optional) - Aktualisierungszeitpunkt (UTC ISO)
- `evidence_ids` (List[string]) - Evidence-IDs

**Beziehungen:**
- `(CanonicalPerson)-[:HAS_SOURCE]->(WikipediaPersonRecord)`
- `(CanonicalPerson)-[:HAS_SOURCE]->(DipPersonRecord)`

**Zweck:** Zusammenführung von Wikipedia- und DIP-Personen durch Identity Resolution.

---

#### `PersonLinkAssertion` (Matching-Assertion)
**Constraints:** `PersonLinkAssertion.id IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Basierend auf `wikipedia_ref + dip_ref + ruleset_version`
- `wikipedia_person_ref` (string) - Wikipedia Person Record ID oder Title
- `dip_person_ref` (string) - DIP Person ID (als String)
- `ruleset_version` (string) - Ruleset-Version (default: "ruleset_v1")
- `method` (string) - "override" oder "ruleset"
- `score` (float) - Match-Score 0..1
- `status` (string) - "accepted", "pending", oder "rejected"
- `reason` (string, optional) - Grund für Status
- `evidence_ids` (List[string]) - Evidence-IDs von beiden Seiten
- `created_at` (string) - Erstellungszeitpunkt (UTC ISO)

**Beziehungen:**
- `(PersonLinkAssertion)-[:LINKS]->(WikipediaPersonRecord)`
- `(PersonLinkAssertion)-[:LINKS]->(DipPersonRecord)`

**Zweck:** Auditierbare Verbindungen zwischen Wikipedia- und DIP-Personen.

---

#### `Mandate` (Mandat)
**Constraints:** `Mandate.id IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Deterministische ID
- `person_id` (string) - Person ID
- `parliament_id` (string) - Parliament-Code (z.B. "NI", "BY", "BT")
- `legislature_id` (string) - Legislature ID
- `party_code` (string, optional) - Parteikürzel
- `wahlkreis` (string, optional) - Wahlkreis
- `start_date` (date, optional) - Startdatum (day-only)
- `end_date` (date, optional) - Enddatum (day-only, NULL = offen)
- `start_date_raw` (string, optional) - Raw-Startwert
- `end_date_raw` (string, optional) - Raw-Endwert
- `start_date_precision` (string, optional) - Precision
- `end_date_precision` (string, optional) - Precision
- `start_date_source` (string, optional) - Quelle
- `end_date_source` (string, optional) - Quelle
- `role` (string, optional) - Rolle (z.B. "MdL", "MdB")
- `notes` (string, optional) - Notizen
- `evidence_ids` (List[string]) - Evidence-IDs

**Indexes:**
- `mandate_person_id` - Index auf `person_id`
- `mandate_legislature_id` - Index auf `legislature_id`
- `mandate_parliament_id` - Index auf `parliament_id`
- `mandate_party_code` - Index auf `party_code`
- `mandate_start_date` - Index auf `start_date`
- `mandate_end_date` - Index auf `end_date`

**Beziehungen:**
- `(Person)-[:HELD]->(Mandate)` - Person hält Mandat
- `(Mandate)-[:IN]->(Legislature)` - Mandat in Legislature
- `(Mandate)-[:AFFILIATED_WITH {start_date, end_date}]->(Party)` - Parteizugehörigkeit
- `(Mandate)-[:SUPPORTED_BY {purpose, snippet_ref_json}]->(Evidence)` - Evidenz

---

#### `Evidence` (Evidenz)
**Constraints:** 
- `Evidence.id IS UNIQUE`
- `Evidence.url IS UNIQUE`

**Felder:**
- `id` (string, UUID5) - Deterministische ID
- `url` (string) - Source URL (unique)
- `endpoint_kind` (string, optional) - Legacy: "parse" oder "query"
- `page_title` (string, optional) - Legacy: Page Title
- `page_id` (int, optional) - Legacy: Page ID
- `revision_id` (int, optional) - Legacy: Revision ID
- `source_url` (string, optional) - Legacy: Source URL
- `retrieved_at` (string, optional) - Retrieval Timestamp (UTC ISO)
- `sha256` (string, optional) - Legacy: SHA256 Hash
- `content_hash` (string, optional) - Content Hash (SHA256)

**Zweck:** Page-level Evidence für Provenance-Tracking.

---

## 2. MeiliSearch Such-Index

### Index: `persons`

**Dokument-Struktur:**
```json
{
  "_id": "person-uuid",
  "id": "person-uuid",
  "name": "Stephan Weil",
  "wikipedia_title": "Stephan_Weil",
  "wikipedia_url": "https://de.wikipedia.org/wiki/Stephan_Weil",
  "normalized_name": "stephan weil",
  "birth_date": "1958-03-15",
  "birth_date_status": "extracted",
  "death_date": null,
  "intro": "Stephan Weil ist ein deutscher Politiker...",
  "evidence_refs": [
    {
      "evidence_id": "evidence-123",
      "purpose": "person_page_intro",
      "snippet_ref": null,
      "confidence": null,
      "created_at": "2026-01-25T10:00:00Z"
    }
  ],
  "evidence_ids": ["evidence-123", "evidence-456"],
  "unstructured_evidence": null,
  "provenance": {
    "evidence_ids": ["evidence-123"],
    "source_page_title": "Stephan_Weil",
    "source_page_id": 12345,
    "revision_id": 67890,
    "retrieved_at": "2026-01-25T10:00:00Z",
    "sha256": "abc123..."
  },
  "data_quality_flags": []
}
```

**Index-Settings:**
- **Searchable Attributes:** `name`, `wikipedia_title`
- **Filterable Attributes:** 
  - `party_name`
  - `parliament`
  - `state`
  - `legislature_number`
  - `start_date`
  - `end_date`

**Zweck:** Volltextsuche nach Personen mit Filterung nach Mandaten.

---

### Index: `mandates`

**Dokument-Struktur:**
```json
{
  "_id": "mandate-uuid",
  "id": "mandate-uuid",
  "person_id": "person-uuid",
  "parliament_id": "NI",
  "legislature_id": "legislature-uuid",
  "party_code": "SPD",
  "wahlkreis": "Hannover-Mitte",
  "start_date": "2013-01-20",
  "end_date": "2017-11-14",
  "start_date_raw": null,
  "end_date_raw": null,
  "start_date_precision": "day",
  "end_date_precision": "day",
  "start_date_source": "legislature",
  "end_date_source": "legislature",
  "role": "MdL",
  "notes": null,
  "evidence_refs": [
    {
      "evidence_id": "evidence-789",
      "purpose": "membership_row",
      "snippet_ref": {
        "type": "table_row",
        "table_index": 0,
        "row_index": 5
      },
      "confidence": null,
      "created_at": "2026-01-25T10:00:00Z"
    }
  ],
  "evidence_ids": ["evidence-789"],
  "provenance": {...}
}
```

**Index-Settings:**
- **Searchable Attributes:** `party_name`, `wahlkreis`, `role`
- **Filterable Attributes:**
  - `person_id`
  - `legislature_id`
  - `party_name`
  - `wahlkreis`
  - `start_date`
  - `end_date`
  - `role`

**Zweck:** Suche und Filterung von Mandaten.

---

### Reconciliation-Daten in MeiliSearch

**CanonicalPerson-Dokumente** (in `persons` Index):
```json
{
  "_id": "canonical-person-uuid",
  "display_name": "Stephan Weil",
  "sources": {
    "wikipedia_title": "Stephan_Weil",
    "dip_person_id": "12345"
  },
  "match_status": "accepted",
  "evidence_ids": ["evidence-123", "evidence-456"],
  "provenance": {
    "revision_id": 67890,
    "page_id": 12345,
    "retrieved_at": "2026-01-25T10:00:00Z",
    "sha256": "abc123...",
    "source_url": "https://de.wikipedia.org/wiki/Stephan_Weil?oldid=67890"
  }
}
```

---

## 3. File Cache (Dateisystem)

### Cache-Struktur

```
data/cache/
├── mediawiki/
│   └── <safe_title>/
│       ├── <revision_id>/
│       │   └── parse/
│       │       ├── raw.json          # MediaWiki API Response
│       │       └── metadata.json     # CachedResponseMetadata
│       └── latest.json               # LatestCacheManifest
├── dip/
│   └── <safe_endpoint>/
│       ├── <params_hash>/
│       │   ├── raw.json              # DIP API Response
│       │   └── metadata.json         # Cache Metadata
│       └── latest.json
├── index/
│   └── evidence_index.jsonl          # Evidence ID → Cache Path Mapping
└── manifests/
    └── <run_id>.json                 # Pipeline Run Manifest
```

### Person-bezogene Cache-Dateien

#### MediaWiki Cache (`data/cache/mediawiki/<safe_title>/<revision_id>/parse/`)

**`raw.json`:** Vollständige MediaWiki API Response
```json
{
  "parse": {
    "pageid": 12345,
    "revid": 67890,
    "title": "Stephan_Weil",
    "text": {
      "*": "<html>...</html>"
    },
    "wikitext": {
      "*": "{{Infobox Person\n|NAME=Stephan Weil\n|GEBURTSDATUM=1958-03-15\n}}"
    },
    "displaytitle": "Stephan Weil"
  }
}
```

**`metadata.json`:** Cache-Metadaten
```json
{
  "request_params": {
    "action": "parse",
    "page": "Stephan_Weil",
    "oldid": 67890
  },
  "retrieved_at": "2026-01-25T10:00:00Z",
  "sha256": "abc123...",
  "url": "https://de.wikipedia.org/w/api.php?action=parse&page=Stephan_Weil&oldid=67890",
  "page_title": "Stephan_Weil",
  "page_id": 12345,
  "revision_id": 67890,
  "endpoint_kind": "parse"
}
```

---

#### DIP Cache (`data/cache/dip/person/<params_hash>/`)

**`raw.json`:** DIP API Response
```json
{
  "numFound": 100,
  "start": 0,
  "rows": [
    {
      "id": 12345,
      "vorname": "Stephan",
      "nachname": "Weil",
      "namenszusatz": null,
      "titel": "Dr.",
      "fraktion": "SPD",
      "wahlperiode": [18, 19, 20]
    }
  ],
  "nextCursor": "abc123..."
}
```

**`metadata.json`:** Cache-Metadaten
```json
{
  "request_params": {
    "f.wahlperiode": [18, 19, 20],
    "limit": 100,
    "cursor": null
  },
  "retrieved_at": "2026-01-25T10:00:00Z",
  "sha256": "def456...",
  "url": "https://search.dip.bundestag.de/api/v1/person",
  "endpoint": "/person",
  "page": 0,
  "cursor": null
}
```

---

#### Evidence Index (`data/cache/index/evidence_index.jsonl`)

**Format:** JSONL (eine Zeile pro Evidence)
```json
{"evidence_id": "evidence-123", "source_kind": "mediawiki", "cache_metadata_path": "data/cache/mediawiki/Stephan_Weil/67890/parse/metadata.json", "cache_raw_path": "data/cache/mediawiki/Stephan_Weil/67890/parse/raw.json", "page_title": "Stephan_Weil", "page_id": 12345, "revision_id": 67890, "sha256": "abc123..."}
{"evidence_id": "evidence-456", "source_kind": "dip", "cache_metadata_path": "data/cache/dip/person/xyz789/metadata.json", "cache_raw_path": "data/cache/dip/person/xyz789/raw.json", "endpoint": "/person", "sha256": "def456..."}
```

**Zweck:** Mapping von Evidence-IDs zu Cache-Pfaden für schnelle Evidenz-Auflösung.

---

## Datenfluss und Beziehungen

### 1. Person-Erstellung (Wikipedia)

```
Wikipedia API
  ↓
File Cache (raw.json + metadata.json)
  ↓
Parser (person_page.py)
  ↓
Person (Domain Model)
  ↓
Neo4j (Person Node)
  ↓
MeiliSearch (persons Index)
```

### 2. Person-Erstellung (DIP)

```
DIP API
  ↓
File Cache (raw.json + metadata.json)
  ↓
Ingest (dip/ingest.py)
  ↓
DipPersonRecord (Domain Model)
  ↓
Neo4j (DipPersonRecord Node)
```

### 3. Identity Resolution (Reconciliation)

```
WikipediaPersonRecord + DipPersonRecord
  ↓
Reconciliation (reconcile/wiki_dip.py)
  ↓
PersonLinkAssertion (Matching-Assertion)
  ↓
CanonicalPerson (Zusammengeführte Entität)
  ↓
Neo4j (CanonicalPerson Node + Relationships)
  ↓
MeiliSearch (persons Index mit match_status)
```

### 4. Mandat-Erstellung

```
Legislature Member List (Wikipedia)
  ↓
File Cache
  ↓
Parser (legislature_members.py)
  ↓
Person + Mandate (Domain Models)
  ↓
Neo4j (Person Node + Mandate Node + HELD Relationship)
  ↓
MeiliSearch (persons + mandates Indices)
```

---

## Wichtige Unterschiede zwischen Datenbanken

### Neo4j
- **Struktur:** Graph mit Nodes und Relationships
- **Zweck:** Beziehungen, Traversierung, komplexe Queries
- **Person-Daten:** Vollständig mit Relationships zu Mandaten, Evidence, etc.
- **Reconciliation:** CanonicalPerson mit HAS_SOURCE Relationships

### MeiliSearch
- **Struktur:** Dokumenten-Index (flach)
- **Zweck:** Volltextsuche, Filterung, Faceting
- **Person-Daten:** Flach mit embedded evidence_refs
- **Reconciliation:** CanonicalPerson als separate Dokumente mit match_status

### File Cache
- **Struktur:** Dateisystem mit JSON-Dateien
- **Zweck:** Evidenz-Speicherung, Reproduzierbarkeit, Offline-Tests
- **Person-Daten:** Rohdaten (MediaWiki/DIP API Responses) + Metadaten
- **Reconciliation:** Keine direkte Reconciliation-Daten, nur Source-Daten

---

## ID-Generierung

Alle IDs werden **deterministisch mit UUID5** generiert:

- **Person:** `generate_person_id(wikipedia_title)` oder `generate_person_id(name)`
- **WikipediaPersonRecord:** `generate_wikipedia_person_record_id(wikipedia_title, revision_id)`
- **DipPersonRecord:** `generate_dip_person_record_id(dip_person_id, payload_sha256)`
- **CanonicalPerson:** `generate_canonical_id(wikipedia_title, dip_person_id)`
- **PersonLinkAssertion:** `generate_link_assertion_id(wikipedia_ref, dip_ref, ruleset_version)`

**Vorteil:** Reproduzierbare IDs bei gleichen Inputs.

---

## Evidence-Referenzen

### EvidenceRef (Entity-Level)
```python
{
  "evidence_id": "evidence-123",
  "purpose": "person_page_intro" | "membership_row",
  "snippet_ref": {
    "type": "table_row",
    "table_index": 0,
    "row_index": 5
  },
  "confidence": 0.95,
  "created_at": "2026-01-25T10:00:00Z"
}
```

**In Neo4j:** Als Relationship-Property `snippet_ref_json` (JSON-String)
**In MeiliSearch:** Als embedded Array in Dokument
**In File Cache:** Via `evidence_index.jsonl` → Cache-Pfad → `raw.json`

---

## Zusammenfassung

| Aspekt | Neo4j | MeiliSearch | File Cache |
|--------|-------|-------------|------------|
| **Person-Entität** | `Person` Node | `persons` Index | - |
| **Wikipedia-Record** | `WikipediaPersonRecord` Node | - | `mediawiki/<title>/<revision>/parse/` |
| **DIP-Record** | `DipPersonRecord` Node | - | `dip/person/<hash>/` |
| **CanonicalPerson** | `CanonicalPerson` Node | `persons` Index (mit match_status) | - |
| **LinkAssertion** | `PersonLinkAssertion` Node | - | - |
| **Mandate** | `Mandate` Node | `mandates` Index | - |
| **Evidence** | `Evidence` Node | Embedded in Dokument | `evidence_index.jsonl` → Cache-Pfad |
| **Relationships** | Graph-Edges | - | - |
| **Suche** | Cypher-Queries | Volltextsuche | - |
| **Filterung** | WHERE-Clauses | Filterable Attributes | - |
