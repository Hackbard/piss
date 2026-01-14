# Import Governance

## Overview

The Neo4j sink now enforces governance-compatible data shape from the initial import, ensuring that all ingested nodes follow the "Know or NULL" policy without requiring post-import fixes.

## Governance Rules

### Date Fields

For any governed date field (e.g., `start_date`, `end_date`):

1. **If precision != day:**
   - Canonical date field (`start_date`) MUST be `NULL`
   - `start_date_raw` and `start_date_precision` are set
   - `start_date_source` is optional (set if available)

2. **If precision == day:**
   - Canonical date field (`start_date`) MUST be set
   - `start_date_precision` = `'day'`
   - `start_date_raw` can be the same ISO string
   - `start_date_source` MUST be set
   - Evidence must include the source URL

### Evidence

All entities with dates use consistent evidence representation:
- Evidence nodes created with `MERGE (e:Evidence {url: $url})`
- Relationships: `(entity)-[:SUPPORTED_BY]->(e:Evidence)`
- Evidence URLs are deduplicated via unique constraint on `Evidence.url`

## Implementation

### Normalizer

The `langgraph_app/governance/normalize.py` module provides:
- `normalize_date_field()`: Normalizes a single date field
- `normalize_legislature_record()`: Normalizes Legislature records
- `normalize_mandate_record()`: Normalizes Mandate records

### Sink Changes

The Neo4j sink (`src/scraper/sinks/neo4j.py`) now:
1. Uses the normalizer to prepare governance-compatible properties
2. Writes Evidence nodes with URLs (not just IDs)
3. Creates `SUPPORTED_BY` relationships consistently
4. Uses `CASE WHEN ... THEN NULL ELSE date(...) END` for safe date conversion

### Import Audit (Optional)

Set `PISS_IMPORT_AUDIT=true` to enable batch-level audit events:
- One `AuditEvent` per import batch (not per row)
- Contains: batch ID, entity counts, sample entity IDs
- Disabled by default for performance

## Validation

After import, run:
```bash
docker compose run --rm --build scraper scraper validate --json | jq '{error_count, warning_count, meta: .meta}'
```

Expect:
- No `DATE_CANONICAL_WITHOUT_EVIDENCE` errors from import
- Completeness gaps (missing dates) appear as warnings, not errors

## Spot Checks

```cypher
MATCH (l:Legislature)
WHERE l.start_date IS NOT NULL
RETURN l.start_date, l.start_date_precision, l.start_date_source
LIMIT 20;
```

```cypher
MATCH (:Legislature)-[:SUPPORTED_BY]->(:Evidence)
RETURN count(*) AS rels;
```
