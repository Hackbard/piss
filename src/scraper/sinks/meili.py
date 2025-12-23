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
            
            # Ensure evidence_ids is derived from evidence_refs if not set
            if not person_dict.get("evidence_ids") and person.evidence_refs:
                person_dict["evidence_ids"] = list(set([ref.evidence_id for ref in person.evidence_refs]))
            
            # Serialize evidence_refs for Meilisearch (JSON-serializable)
            if person.evidence_refs:
                person_dict["evidence_refs"] = [ref.model_dump() for ref in person.evidence_refs]
            
            # Validate: if intro is present, must have at least 2 evidence IDs
            if person_dict.get("intro") and len(person_dict.get("evidence_ids", [])) < 2:
                import sys
                print(f"  ⚠ Warning: Person {person.wikipedia_title} in Meili has intro but only {len(person_dict.get('evidence_ids', []))} evidence ID(s). Expected at least 2.", file=sys.stderr)
            
            persons_docs.append(person_dict)

        if persons_docs:
            persons_index = self.client.index("persons")
            persons_index.update_documents(persons_docs, primary_key="_id")

        mandates_docs = []
        for mandate in normalized_data.get("mandates", []):
            mandate_dict = mandate.model_dump()
            mandate_dict["_id"] = mandate.id
            
            # Ensure evidence_ids is derived from evidence_refs if not set
            if not mandate_dict.get("evidence_ids") and mandate.evidence_refs:
                mandate_dict["evidence_ids"] = list(set([ref.evidence_id for ref in mandate.evidence_refs]))
            
            # Serialize evidence_refs for Meilisearch (JSON-serializable)
            if mandate.evidence_refs:
                mandate_dict["evidence_refs"] = [ref.model_dump() for ref in mandate.evidence_refs]
            
            mandates_docs.append(mandate_dict)

        if mandates_docs:
            mandates_index = self.client.index("mandates")
            mandates_index.update_documents(mandates_docs, primary_key="_id")

    def upsert_reconciliation(self, normalized_data: Dict[str, Any]) -> None:
        canonical_docs = []
        for canonical in normalized_data.get("canonical_persons", []):
            # Build provenance summary from canonical person
            provenance = None
            if canonical.provenance:
                provenance = canonical.provenance
            elif canonical.identifiers.get("wikipedia_title"):
                # Try to get provenance from Wikipedia source
                from scraper.cache.mediawiki_cache import get_cached_metadata
                from urllib.parse import quote
                
                metadata = get_cached_metadata(canonical.identifiers["wikipedia_title"])
                if metadata:
                    page_title_encoded = quote(canonical.identifiers["wikipedia_title"].replace("_", " "), safe="")
                    source_url = f"https://de.wikipedia.org/wiki/{page_title_encoded}"
                    if metadata.revision_id:
                        source_url_canonical = f"{source_url}?oldid={metadata.revision_id}"
                    else:
                        source_url_canonical = source_url
                    
                    provenance = {
                        "revision_id": metadata.revision_id,
                        "page_id": metadata.page_id,
                        "retrieved_at": metadata.retrieved_at,
                        "sha256": metadata.sha256,
                        "source_url": source_url_canonical,
                    }
            
            doc = {
                "_id": canonical.id,
                "display_name": canonical.display_name,
                "sources": {
                    "wikipedia_title": canonical.identifiers.get("wikipedia_title"),
                    "dip_person_id": canonical.identifiers.get("dip_person_id"),
                },
                "match_status": "accepted",
                "evidence_ids": canonical.evidence_ids,
                "provenance": provenance,
            }
            canonical_docs.append(doc)

        if canonical_docs:
            persons_index = self.client.index("persons")
            persons_index.update_documents(canonical_docs, primary_key="_id")

