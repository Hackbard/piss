from __future__ import annotations

from typing import Any, Iterable

import meilisearch

from pis.settings import PisSettings


class PisMeiliIndexer:
    def __init__(self, settings: PisSettings, *, index_name: str = "pis_persons"):
        self.settings = settings
        self.index_name = index_name
        self.client = meilisearch.Client(settings.meili_url, settings.meili_master_key)

    def init(self) -> None:
        idx = self.client.index(self.index_name)
        idx.update_settings(
            {
                "searchableAttributes": [
                    "display_name",
                    "aliases",
                    "persona_summary",
                    "facts",
                    "external_ids.wikidata_qid",
                    "external_ids.wikipedia_title",
                ],
                "filterableAttributes": [
                    "pis_person_id",
                    "external_ids.wikidata_qid",
                    "external_ids.wikipedia_pageid",
                    "external_ids.dip_person_id",
                    "external_ids.wikipedia_title",
                    "provenance.sources.source_system",
                ],
                "sortableAttributes": [
                    "display_name",
                    "meta.updated_at",
                ],
            }
        )

    def upsert_persons(self, docs: Iterable[dict[str, Any]]) -> None:
        idx = self.client.index(self.index_name)
        idx.update_documents(list(docs), primary_key="pis_person_id")

