## [Unreleased] (2026-02-07)
### Added
- `docs/architecture.md`: PIS architecture overview (ETL → canonical → Meilisearch/RAG).
- `docs/assumptions.md`: explicit assumptions for federal/official sources and identity/time modeling.
- `src/pis/`: initial PIS package skeleton with canonical Pydantic models in `src/pis/models.py`.

### Fixed
- Restored offline/backward-compatible members-list parsing helpers in `langgraph_app/graph.py` to keep MVP runner tests working.
- Clamp `evidence_urls` to `max_sources` in `_merge_member_rows` even when only a single row exists.

### Added
- PIS CLI entrypoint (`pis`) with `health` and `schema` commands.
- `.env.example` for local development.
- `docker-compose.pis.yml` for a Meilisearch-only local setup.
- Basic unit tests for `pis.models`.

### Added
- Deterministic PIS ID helpers in `src/pis/ids.py`.
- JSONL snapshot writer in `src/pis/io/jsonl.py`.
- Reusable file-based HTTP JSON cache in `src/pis/utils/http_cache.py`.

### Added
- Wikidata/Wikipedia PoC pipeline: `pis poc wikidata-persons` (SPARQL fetch + optional MediaWiki intro enrichment + JSONL snapshots + optional Meilisearch index `pis_persons`).
- Canonical uniqueness guard: duplicates are separated and written as `*.dupes.persons.jsonl` instead of silently indexing duplicates.
- DIP (Bundestag) connector PoC: `pis poc dip-persons` (DIP `/person` ingestion + snapshots + optional indexing into `pis_person_sources`).
- Reconciliation PoC: `pis poc reconcile-wikidata-dip` (conservative 1:1 name-based merge + reports + optional indexing into canonical `pis_persons`).
 - RAG helpers: `pis rag retrieve` and `src/pis/rag/retrieve.py` for compact JSON context packages.

### Changed
- PIS codebase aligned to Ruff typing/import conventions (StrEnum + modern type hints).

### Added
- Temporal QA helper (`src/pis/qa/temporal.py`) to detect overlapping role intervals (basis for time-axis reports).

