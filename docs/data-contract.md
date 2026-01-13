# Data Contract

## Entities und Pflichtfelder

### Person

- **id** (string, UUID, required): Stabile, deterministische UUID5-ID
- **name** (string, required): Vollständiger Name
- **wikipedia_title** (string, optional): Wikipedia-Seitentitel
- **normalized_name** (string, optional): Normalisierter Name für Deduplikation
- **birth_date** (string, optional): Geburtsdatum (ISO-Format: YYYY-MM-DD)
- **death_date** (string, optional): Todesdatum (ISO-Format)
- **intro** (string, optional): Einleitungstext von Wikipedia
- **evidence_refs** (List[EvidenceRef], optional): Entity-Level Evidence-Referenzen
- **evidence_ids** (List[string], optional): Legacy Evidence-IDs

**Beispiel:**
```json
{
  "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "name": "Stephan Weil",
  "wikipedia_title": "Stephan_Weil",
  "normalized_name": "stephan weil",
  "evidence_refs": [
    {
      "evidence_id": "evidence-123",
      "purpose": "person_page_intro"
    }
  ]
}
```

### Mandate

- **id** (string, UUID, required): Stabile, deterministische UUID5-ID
- **person_id** (string, required): Person-ID
- **parliament_id** (string, required): Parliament-Code (z.B. `NI`, `BY`, `BT`, `BR`)
- **legislature_id** (string, required): Legislature-ID
- **party_code** (string, optional): Parteikürzel (z.B. "SPD", "CDU")
- **start_date** (string, optional, nullable): Startdatum (ISO-Format: YYYY-MM-DD, day-only oder `null`)
- **end_date** (string, optional, nullable): Enddatum (ISO-Format: YYYY-MM-DD, day-only oder `null` = offen/unbekannt)
- **start_date_raw** (string, optional, nullable): Raw-Startwert wenn keine day-Precision vorliegt
- **end_date_raw** (string, optional, nullable): Raw-Endwert wenn keine day-Precision vorliegt
- **start_date_source** (string, optional, nullable): Quelle (z.B. `"legislature"`)
- **end_date_source** (string, optional, nullable): Quelle (z.B. `"legislature"`)
- **role** (string, optional): Rolle (z.B. "MdL", "MdB")
- **evidence_refs** (List[EvidenceRef], optional): Entity-Level Evidence-Referenzen

**QA-Hinweis:** Der Validator (`scraper validate`) behandelt `Mandate.start_date` aktuell als Pflichtfeld und liefert bei `null` einen ERROR.

**Beispiel:**
```json
{
  "id": "mandate-123",
  "person_id": "person-456",
  "parliament_id": "NI",
  "legislature_id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "party_code": "SPD",
  "start_date": "2013-01-20",
  "end_date": "2017-11-14",
  "role": "MdL",
  "evidence_refs": [
    {
      "evidence_id": "evidence-789",
      "purpose": "membership_row",
      "snippet_ref": {
        "type": "table_row",
        "table_index": 0,
        "row_index": 5
      }
    }
  ]
}
```

### Legislature

- **id** (string, UUID, required): Stabile, deterministische UUID5-ID
- **parliament_id** (string, required): Parliament-Code (z.B. `NI`, `BY`, `BT`, `BR`)
- **term_number** (int, optional, nullable): Wahlperiode/Term-Nummer (z.B. `17`)
- **name** (string, required): Name (z.B. "17. Landtag Niedersachsen")
- **start_date** (string, optional, nullable): Startdatum (ISO, day-only) der **konstituierenden Sitzung / ersten Sitzung**, sonst `null`
- **end_date** (string, optional, nullable): Enddatum (ISO, day-only) oder `null`
- **start_date_raw** (string, optional, nullable): Raw-Startwert (z.B. "November 1986") wenn day-Precision fehlt
- **end_date_raw** (string, optional, nullable): Raw-Endwert wenn day-Precision fehlt
- **start_date_precision** (string, optional, nullable): `"day"|"month"|"year"|"unknown"` (MUSS `"day"` sein, wenn `start_date` gesetzt ist)
- **end_date_precision** (string, optional, nullable): `"day"|"month"|"year"|"unknown"` (MUSS `"day"` sein, wenn `end_date` gesetzt ist)
- **start_date_source** (string, optional, nullable): `"official"|"wikidata"|"wikipedia"` (oder projektspezifischer Source-Key)
- **end_date_source** (string, optional, nullable): `"official"|"wikidata"|"wikipedia"` (oder projektspezifischer Source-Key)
- **source_url** (string, optional, nullable): Canonical Wikipedia-URL mit `oldid` (Reproduzierbarkeit)
- **wikipedia_title** (string, optional, nullable): Wikipedia-Seitentitel
- **evidence_ids** (List[string], optional): Evidence-IDs

**Beispiel:**
```json
{
  "id": "7219e8b8-3d63-59ae-823e-df5a7a0d2253",
  "parliament_id": "NI",
  "term_number": 17,
  "name": "17. Landtag Niedersachsen",
  "start_date": "2013-01-20",
  "start_date_precision": "day",
  "start_date_source": "official",
  "end_date": "2017-11-14",
  "end_date_precision": "day"
}
```

### LegislatureTerm

`LegislatureTerm` hält term-spezifische Daten pro Quelle getrennt und wird anschließend in `Legislature` propagiert.

- **id** (string, required): Deterministische ID (z.B. `"official:NI:17"` oder `"wikidata:Q123:123456"`)
- **qid** (string, optional, nullable): Wikidata-QID (falls Quelle Wikidata)
- **parliament_id** (string, required)
- **term_number** (int, required)
- **name** (string, optional, nullable)
- **start_date** (string, optional, nullable): ISO day-only oder `null`
- **start_date_raw** (string, optional, nullable)
- **start_date_precision** (string, optional, nullable): `"day"|"month"|"year"|"unknown"`
- **end_date** (string, optional, nullable): ISO day-only oder `null`
- **end_date_raw** (string, optional, nullable)
- **end_date_precision** (string, optional, nullable): `"day"|"month"|"year"|"unknown"`
- **source_primary** (string, required): `"official"|"wikidata"|"wikipedia"`
- **source_meta_json** (string, optional, nullable): Canonical JSON string mit Revision/Snapshot-Meta
- **evidence_urls** (List[string], optional): Liste von Evidence-URLs (z.B. revision-pinned Wikidata EntityData URL)

**Beziehungen:**
- `(l:Legislature)-[:HAS_TERM]->(t:LegislatureTerm)`

### Parliament

- **id** (string, UUID, required): Stabile, deterministische UUID5-ID
- **name** (string, required): Name (z.B. "Niedersächsischer Landtag")
- **level** (string, required): Level ("federal" oder "state")
- **state_code** (string, optional): Bundesland-Code (z.B. "NI" für Niedersachsen)
- **evidence_ids** (List[string], optional): Evidence-IDs

**Beispiel:**
```json
{
  "id": "3b1b2c6e-3b8f-5d7c-9c44-4ff0d2d4c2fb",
  "name": "Niedersächsischer Landtag",
  "level": "state",
  "state_code": "NI"
}
```

### Party

- **id** (string, UUID, required): Stabile, deterministische UUID5-ID
- **code** (string, required): Parteikürzel (z.B. "SPD", "CDU") - Primary Key
- **name** (string, required): Vollständiger Parteiname
- **evidence_ids** (List[string], optional): Evidence-IDs

**Beispiel:**
```json
{
  "id": "party-spd",
  "code": "SPD",
  "name": "Sozialdemokratische Partei Deutschlands"
}
```

## Zeitlogik

### interval_overlaps

Zentrale Funktion zur Überprüfung von Zeitintervall-Überschneidungen:

```python
from scraper.utils.intervals import interval_overlaps
from datetime import date

# Überlappung prüfen
overlaps = interval_overlaps(
    a_start=date(2020, 1, 1),
    a_end=date(2020, 12, 31),
    b_start=date(2020, 6, 1),
    b_end=date(2020, 6, 30),
)
# Returns: True
```

**Regeln:**
- `None` als `end_date` bedeutet "offen" (unbegrenzt)
- Zwei offene Intervalle überlappen immer
- Berührung am Rand gilt als Überlappung

### Filter Mandate by Overlap

Query-Hilfe zum Filtern von Mandaten nach Zeitbereich:

```python
from scraper.utils.intervals import filter_mandates_by_overlap
from datetime import date

filtered = filter_mandates_by_overlap(
    mandates=mandates,
    from_date=date(2014, 1, 1),
    to_date=date(2020, 12, 31),
)
```

## Constraints und Indexes

### Neo4j Constraints

**Unique Constraints:**
- `Person.id` IS UNIQUE
- `Parliament.id` IS UNIQUE
- `Legislature.id` IS UNIQUE
- `Mandate.id` IS UNIQUE
- `Party.id` IS UNIQUE
- `Party.code` IS UNIQUE
- `Evidence.id` IS UNIQUE

**Indexes:**
- `Mandate(person_id)` - Index für Person-Lookups
- `Mandate(legislature_id)` - Index für Legislature-Lookups
- `Mandate(parliament_id)` - Index für Parliament-Lookups
- `Mandate(party_code)` - Index für Party-Lookups
- `Mandate(start_date)` - Index für Zeitbereich-Queries
- `Mandate(end_date)` - Index für Zeitbereich-Queries

### Dedupe-Regel

Mandate-Deduplikation basierend auf:
- `(person_id, legislature_id, start_date, end_date, party_code)`

Diese Kombination darf nicht doppelt existieren.

## ID-Generierung

Alle IDs werden deterministisch mit UUID5 generiert:

```python
from scraper.utils.ids import (
    generate_person_id,
    generate_parliament_id,
    generate_legislature_id,
    generate_party_id,
    generate_mandate_id,
)

person_id = generate_person_id("Stephan_Weil")
parliament_node_id = generate_parliament_id("Niedersächsischer Landtag", "state", "NI")
legislature_id = generate_legislature_id("NI", 17)
party_id = generate_party_id("SPD")
mandate_id = generate_mandate_id(
    person_id,
    legislature_id,
    "2013-01-20",
    "2017-11-14",
    role="MdL",
    party_code="SPD",
)
```

## Abfrage-Beispiele

### Alle SPD-Mandate im Landtag NDS 2014-2020

```cypher
MATCH (m:Mandate)
WHERE m.party_code = "SPD"
  AND m.parliament_id = "NI"
  AND m.start_date <= "2020-12-31"
  AND (m.end_date IS NULL OR m.end_date >= "2014-01-01")
RETURN m
```

### Personen mit überlappenden Mandaten

```cypher
MATCH (m1:Mandate)-[:HELD]-(p:Person)-[:HELD]-(m2:Mandate)
WHERE m1.id <> m2.id
  AND m1.person_id = m2.person_id
  AND m1.legislature_id = m2.legislature_id
  AND m1.start_date <= COALESCE(m2.end_date, "9999-12-31")
  AND m2.start_date <= COALESCE(m1.end_date, "9999-12-31")
RETURN p, m1, m2
```

