# QA-Gates (Validator & Reports)

## Validator-Regeln

### ERROR-Level Validierungen

#### MANDATE_MISSING_START_DATE
- **Bedingung**: `start_date` fehlt
- **Aktion**: **WARNING** (default) oder **ERROR** (mit `--strict-completeness`)
- **Beispiel**: Mandate ohne `start_date`
- **Hinweis**: Im Default-Modus ist Missing `start_date` ein Completeness-Gap (kein Hard-Error), da noch nicht alle Term-Startdaten verfügbar sein müssen

#### MANDATE_END_BEFORE_START
- **Bedingung**: `end_date < start_date`
- **Aktion**: ERROR
- **Beispiel**: `start_date="2020-12-31"`, `end_date="2020-01-01"`

#### MANDATE_DUPLICATE
- **Bedingung**: Doppelte Mandate nach Dedupe-Key `(person_id, legislature_id, start_date, end_date, party_code)`
- **Aktion**: ERROR
- **Beispiel**: Zwei identische Mandate für dieselbe Person/Legislature/Partei/Zeitraum

#### MANDATE_OVERLAP_SAME_PARTY
- **Bedingung**: Überlappende Mandate für gleiche Person/Legislature mit gleicher Partei
- **Aktion**: ERROR (nicht erklärbar)
- **Beispiel**: Zwei SPD-Mandate überlappen sich

#### MANDATE_MISSING_EVIDENCE (strict mode)
- **Bedingung**: Mandate ohne Evidence-Referenzen
- **Aktion**: ERROR (nur im strict mode)
- **Beispiel**: Mandate ohne `evidence_refs` und ohne `evidence_ids`

### WARN-Level Validierungen

#### MANDATE_UNKNOWN_PARTY_CODE
- **Bedingung**: `party_code` nicht in Party-Tabelle/Knoten
- **Aktion**: WARN
- **Beispiel**: `party_code="UNKNOWN"` existiert nicht in Party-Liste

#### MANDATE_OVERLAP_DIFFERENT_PARTY
- **Bedingung**: Überlappende Mandate für gleiche Person/Legislature mit unterschiedlicher Partei
- **Aktion**: WARN (möglicher Parteiwechsel)
- **Beispiel**: SPD-Mandat und CDU-Mandat überlappen sich

#### MANDATE_MISSING_EVIDENCE (non-strict mode)
- **Bedingung**: Mandate ohne Evidence-Referenzen
- **Aktion**: WARN (default)
- **Beispiel**: Mandate ohne `evidence_refs` und ohne `evidence_ids`

## CLI Command

### Basis-Usage

```bash
scraper validate
```

### Mit Filtern

```bash
# Filter nach Datum
scraper validate --from 2014-01-01 --to 2020-12-31

# Filter nach Parliament
scraper validate --parliament NI

# Kombiniert
scraper validate --from 2014-01-01 --to 2020-12-31 --parliament NI
```

### Strict Mode

```bash
# Missing Evidence wird zu ERROR
scraper validate --strict

# Strict Completeness: Missing start_date wird zu ERROR (default: WARNING)
scraper validate --strict-completeness

# Kombiniert: Strict Evidence + Strict Completeness
scraper validate --strict --strict-completeness
```

### JSON Output

```bash
# Output als JSON (für CI/Programmierung)
scraper validate --json
```

### Exit Codes

- **0**: Keine Errors (Warnings sind OK)
- **1**: Mindestens ein ERROR

**CI-tauglich:** Exit Code != 0 bei ERRORs

## Output-Format

### Human-Readable (Default)

```
✗ Validation failed: 2 errors, 3 warnings
  ERROR [MANDATE_MISSING_START_DATE]: Mandate mandate-123 is missing required start_date
  ERROR [MANDATE_END_BEFORE_START]: Mandate mandate-456 has end_date (2020-01-01) before start_date (2020-12-31)
  WARN [MANDATE_UNKNOWN_PARTY_CODE]: Mandate mandate-789 has unknown party_code: UNKNOWN
  WARN [MANDATE_MISSING_EVIDENCE]: Mandate mandate-101 has no evidence references
  WARN [MANDATE_OVERLAP_DIFFERENT_PARTY]: Mandates mandate-111 and mandate-222 overlap with different party_codes (SPD vs CDU) - possible party switch
```

### JSON Output

```json
{
  "errors": [
    {
      "code": "MANDATE_MISSING_START_DATE",
      "message": "Mandate mandate-123 is missing required start_date",
      "entity_id": "mandate-123",
      "entity_type": "Mandate"
    }
  ],
  "warnings": [
    {
      "code": "MANDATE_UNKNOWN_PARTY_CODE",
      "message": "Mandate mandate-789 has unknown party_code: UNKNOWN",
      "entity_id": "mandate-789",
      "entity_type": "Mandate"
    }
  ],
  "error_count": 1,
  "warning_count": 1
}
```

## Nutzung in CI/CD

### GitHub Actions Beispiel

```yaml
- name: Validate Data
  run: |
    docker compose run --rm scraper scraper validate --json > validation.json
    if [ $? -ne 0 ]; then
      echo "Validation failed"
      cat validation.json
      exit 1
    fi
```

### Pre-Commit Hook

```bash
#!/bin/bash
scraper validate --strict
if [ $? -ne 0 ]; then
  echo "Validation failed. Please fix errors before committing."
  exit 1
fi
```

## Import-Run Meta (Optional)

Für nachvollziehbare Import-Runs kann eine Metadatenstruktur angelegt werden:

```json
{
  "run_id": "run-2024-01-15-10-30-00",
  "started_at": "2024-01-15T10:30:00Z",
  "finished_at": "2024-01-15T10:45:00Z",
  "source": "pipeline",
  "counts": {
    "persons": 150,
    "mandates": 200,
    "legislatures": 5,
    "parties": 10
  },
  "diff": {
    "persons_added": 5,
    "mandates_added": 10,
    "mandates_updated": 3
  },
  "validation": {
    "errors": 0,
    "warnings": 2
  }
}
```

**Speicherung:**
- SQL: Tabelle `import_runs`
- Neo4j: Knoten `ImportRun` mit Relationships zu Entities
- JSON: `/data/exports/<run_id>/manifest.json`

## Validator-API

### Programmatische Nutzung

```python
from scraper.validation.validator import DataValidator
from scraper.models.domain import Mandate, Party
from datetime import date

validator = DataValidator(strict_mode=False)
result = validator.validate_all(
    mandates=mandates,
    parties=parties,
    from_date=date(2014, 1, 1),
    to_date=date(2020, 12, 31),
    parliament_id="NI",
)

if result.has_errors():
    print(f"Validation failed: {len(result.errors)} errors")
    for error in result.errors:
        print(f"  {error['code']}: {error['message']}")
else:
    print(f"Validation passed: {len(result.warnings)} warnings")
```

## Best Practices

1. **Nach jedem Import**: Validator ausführen
2. **In CI/CD**: Validator als Gate vor Merge
3. **Strict Mode**: In Production verwenden
4. **JSON Output**: Für automatisierte Reports
5. **Date Filtering**: Für inkrementelle Validierung

