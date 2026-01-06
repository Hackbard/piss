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
- **parliament_id** (string, required): Parliament-ID
- **legislature_id** (string, required): Legislature-ID
- **party_code** (string, optional): Parteikürzel (z.B. "SPD", "CDU")
- **start_date** (string, required): Startdatum (ISO-Format: YYYY-MM-DD)
- **end_date** (string, optional, nullable): Enddatum (ISO-Format, nullable = offen)
- **role** (string, optional): Rolle (z.B. "MdL", "MdB")
- **evidence_refs** (List[EvidenceRef], optional): Entity-Level Evidence-Referenzen

**Beispiel:**
```json
{
  "id": "mandate-123",
  "person_id": "person-456",
  "parliament_id": "parliament-nds",
  "legislature_id": "legislature-nds-17",
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
- **parliament_id** (string, required): Parliament-ID
- **name** (string, required): Name (z.B. "17. Landtag Niedersachsen")
- **start_date** (string, required): Startdatum (ISO-Format)
- **end_date** (string, required): Enddatum (ISO-Format)
- **evidence_ids** (List[string], optional): Evidence-IDs

**Beispiel:**
```json
{
  "id": "legislature-nds-17",
  "parliament_id": "parliament-nds",
  "name": "17. Landtag Niedersachsen",
  "start_date": "2013-01-20",
  "end_date": "2017-11-14"
}
```

### Parliament

- **id** (string, UUID, required): Stabile, deterministische UUID5-ID
- **name** (string, required): Name (z.B. "Niedersächsischer Landtag")
- **level** (string, required): Level ("federal" oder "state")
- **state_code** (string, optional): Bundesland-Code (z.B. "NI" für Niedersachsen)
- **evidence_ids** (List[string], optional): Evidence-IDs

**Beispiel:**
```json
{
  "id": "parliament-nds",
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
parliament_id = generate_parliament_id("Niedersächsischer Landtag", "state", "NI")
legislature_id = generate_legislature_id(parliament_id, "17. Landtag Niedersachsen")
party_id = generate_party_id("SPD")
mandate_id = generate_mandate_id(person_id, legislature_id, "2013-01-20", "2017-11-14", "MdL")
```

## Abfrage-Beispiele

### Alle SPD-Mandate im Landtag NDS 2014-2020

```cypher
MATCH (m:Mandate)
WHERE m.party_code = "SPD"
  AND m.parliament_id = "parliament-nds"
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

