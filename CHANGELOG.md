## [Unreleased] (2026-02-07)
### Added
- `docs/architecture.md`: PIS architecture overview (ETL → canonical → Meilisearch/RAG).
- `docs/assumptions.md`: explicit assumptions for federal/official sources and identity/time modeling.
- `src/pis/`: initial PIS package skeleton with canonical Pydantic models in `src/pis/models.py`.

### Fixed
- Restored offline/backward-compatible members-list parsing helpers in `langgraph_app/graph.py` to keep MVP runner tests working.
- Clamp `evidence_urls` to `max_sources` in `_merge_member_rows` even when only a single row exists.

