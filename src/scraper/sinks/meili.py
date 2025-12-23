from typing import Any, Dict

from meilisearch import Client

from scraper.config import Settings


class MeiliSink:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = Client(settings.meili_url, settings.meili_master_key)

    def init(self) -> None:
        persons_index = self.client.index("persons")
        persons_index.update_settings(
            {
                "filterableAttributes": [
                    "party_name",
                    "parliament",
                    "state",
                    "legislature_number",
                    "start_date",
                    "end_date",
                ],
                "searchableAttributes": ["name", "wikipedia_title"],
            }
        )

        mandates_index = self.client.index("mandates")
        mandates_index.update_settings(
            {
                "filterableAttributes": [
                    "person_id",
                    "legislature_id",
                    "party_name",
                    "wahlkreis",
                    "start_date",
                    "end_date",
                    "role",
                ],
                "searchableAttributes": ["party_name", "wahlkreis", "role"],
            }
        )

    def upsert(self, normalized_data: Dict[str, Any]) -> None:
        persons_docs = []
        for person in normalized_data.get("persons", []):
            person_dict = person.model_dump()
            person_dict["_id"] = person.id
            persons_docs.append(person_dict)

        if persons_docs:
            persons_index = self.client.index("persons")
            persons_index.update_documents(persons_docs, primary_key="_id")

        mandates_docs = []
        for mandate in normalized_data.get("mandates", []):
            mandate_dict = mandate.model_dump()
            mandate_dict["_id"] = mandate.id
            mandates_docs.append(mandate_dict)

        if mandates_docs:
            mandates_index = self.client.index("mandates")
            mandates_index.update_documents(mandates_docs, primary_key="_id")

