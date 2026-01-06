# Provenance & Evidence

## Evidence-Entity

Evidence repräsentiert eine unveränderliche, page-level Quelle:

- **id** (string, UUID, required): Deterministische UUID5-ID
- **url** (string, required): Source URL (mit `oldid` für Reproduzierbarkeit)
- **retrieved_at** (string, required): Retrieval-Timestamp (UTC ISO)
- **content_hash** (string, required): SHA256-Hash des Inhalts
- **source_type** (string, optional): Quelle (z.B. "wikipedia", "parliament_site", "dip")
- **locator** (string, optional): Locator (z.B. Abschnitt/Tabellenzeile/Selector)
- **snapshot_path** (string, optional): Pfad zu gecachtem HTML/JSON

**Beispiel:**
```json
{
  "id": "evidence-123",
  "url": "https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=256198867",
  "retrieved_at": "2024-01-15T10:30:00Z",
  "content_hash": "a1b2c3d4e5f6...",
  "source_type": "wikipedia",
  "locator": "table_row:0:5",
  "snapshot_path": "/data/cache/mediawiki/Stephan_Weil/256198867/parse/raw.json"
}
```

## EvidenceRef (Entity-Level)

EvidenceRef verknüpft fachliche Objekte mit Evidence und enthält Row-Level-Referenzen:

- **evidence_id** (string, required): Evidence-ID (page-level)
- **snippet_ref** (object, optional): Row-Level-Referenz (z.B. `table_row` mit `table_index`, `row_index`)
- **purpose** (string, optional): Zweck (z.B. "membership_row", "person_page_intro")
- **confidence** (float, optional): Confidence-Score (0..1)
- **created_at** (string, optional): Erstellungs-Timestamp

**Beispiel:**
```json
{
  "evidence_id": "evidence-123",
  "snippet_ref": {
    "version": 1,
    "type": "table_row",
    "table_index": 0,
    "row_index": 5,
    "row_kind": "data",
    "match": {
      "person_title": "Stephan_Weil",
      "name_cell": "Stephan Weil"
    }
  },
  "purpose": "membership_row",
  "confidence": 1.0
}
```

## Verknüpfungen

### Person ⟷ Evidence

```cypher
MATCH (p:Person)-[r:SUPPORTED_BY]->(e:Evidence)
WHERE p.id = "person-123"
RETURN p, r.purpose, e.url
```

### Mandate ⟷ Evidence

```cypher
MATCH (m:Mandate)-[r:SUPPORTED_BY]->(e:Evidence)
WHERE m.id = "mandate-123"
RETURN m, r.purpose, r.snippet_ref_json, e.url
```

### Legislature ⟷ Evidence

```cypher
MATCH (l:Legislature)-[r:SUPPORTED_BY]->(e:Evidence)
WHERE l.id = "legislature-nds-17"
RETURN l, e.url
```

## Query-Konvention

Jede fachliche Query, die Daten zurückgibt, muss Evidence liefern können:

### Mindestanforderung: evidence_urls

Jede Ergebniszeile muss `evidence_urls: string[]` enthalten:
- Dedupliziert
- Keine nulls
- Canonical URLs (mit `oldid` für Reproduzierbarkeit)

**Beispiel-Query:**
```cypher
MATCH (m:Mandate)-[r:SUPPORTED_BY]->(e:Evidence)
WHERE m.party_code = "SPD"
  AND m.parliament_id = "parliament-nds"
RETURN m.id, m.party_code, collect(DISTINCT e.url) as evidence_urls
```

### Erweiterte Query mit EvidenceRef

```cypher
MATCH (m:Mandate)-[r:SUPPORTED_BY]->(e:Evidence)
WHERE m.id = "mandate-123"
RETURN 
  m.id,
  m.party_code,
  e.url,
  r.purpose,
  r.snippet_ref_json
```

## Hashing

### Content Hash (SHA256)

Der `content_hash` wird über den vollständigen Response-Payload berechnet:

```python
from scraper.utils.hashing import sha256_hash_json

content_hash = sha256_hash_json(response_data)
```

**Zweck:**
- Integritätsprüfung
- Deduplikation
- Cache-Validierung

## Retrieval

### Evidence Index

Der Evidence Index (`/data/cache/index/evidence_index.jsonl`) wird automatisch aktualisiert:

- **MediaWiki**: Beim Cachen von `action=parse` Responses
- **DIP**: Beim Cachen von DIP API Responses

**Format:**
```json
{
  "evidence_id": "evidence-123",
  "source_kind": "mediawiki",
  "cache_metadata_path": "/data/cache/mediawiki/Stephan_Weil/256198867/parse/metadata.json",
  "cache_raw_path": "/data/cache/mediawiki/Stephan_Weil/256198867/parse/raw.json",
  "page_title": "Stephan_Weil",
  "page_id": 123456,
  "revision_id": 256198867,
  "sha256": "a1b2c3d4e5f6..."
}
```

### Offline-Verhalten

Der Evidence Resolver arbeitet vollständig offline:
- Liest aus dem Disk-Cache (keine HTTP-Requests)
- Nutzt den Evidence Index für schnelle Lookups
- Falls Index fehlt: Optionaler "best effort" Scan

## Snippet-Extraktion

Snippets werden aus dem gecachten HTML extrahiert:

- **Lead Paragraph**: Erster `<p>` mit ausreichend Inhalt (>= 80 Zeichen)
- **Table Row**: Falls `snippet_ref` vorhanden (z.B. `table_row:0:5`)
- **Cleaning**: Entfernt Fußnoten-Marker `[1]`, `[2]`, normalisiert Whitespace
- **Truncation**: Maximal konfigurierbare Länge (default: 500 Zeichen)

## Reproduzierbarkeit

### oldid URLs

Wikipedia-Seiten ändern sich. Eine URL ohne `oldid` zeigt immer die aktuelle Version. Mit `oldid=<revision_id>` ist die URL reproduzierbar:

**Format:**
- Mit `oldid`: `https://de.wikipedia.org/w/index.php?title=Stephan_Weil&oldid=256198867`
- Ohne `oldid`: `https://de.wikipedia.org/wiki/Stephan_Weil` (zeigt aktuelle Version)

### Deterministische IDs

Alle Evidence-IDs werden deterministisch generiert:

```python
from scraper.utils.ids import generate_evidence_id

evidence_id = generate_evidence_id(
    page_id=123456,
    revision_id=256198867,
    endpoint_kind="parse",
    sha256="a1b2c3d4e5f6...",
)
```

**Vorteil:** Gleiche Quelle → gleiche ID (idempotent)

