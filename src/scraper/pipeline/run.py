import json
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from scraper.cache.mediawiki_cache import get_cached_parse_response, get_seed, load_seeds
from scraper.config import Settings
from scraper.models.domain import Evidence, Legislature, Party
from scraper.parsers.legislature_members import parse_legislature_members
from scraper.sinks.json_export import export_json
from scraper.sinks.meili import MeiliSink
from scraper.sinks.neo4j import Neo4jSink
from scraper.utils.time import utc_now_iso


class PipelineRunner:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.neo4j_sink: Neo4jSink | None = None
        self.meili_sink: MeiliSink | None = None

    def run_single(
        self,
        seed_key: str,
        write_neo4j: bool = False,
        write_meili: bool = False,
        force: bool = False,
        revalidate: bool = False,
    ) -> bool:
        run_id = str(uuid4())
        manifest: Dict[str, Any] = {
            "run_id": run_id,
            "started_at": utc_now_iso(),
            "seed_key": seed_key,
            "requests": [],
            "cache_hits": [],
            "cache_misses": [],
            "outputs": {},
            "errors": [],
        }

        try:
            from scraper.cache.mediawiki_cache import fetch_legislature_page

            fetch_legislature_page(seed_key=seed_key, run_id=run_id, force=force, revalidate=revalidate)
            manifest["cache_misses"].append({"seed": seed_key, "type": "legislature"})

            response = get_cached_parse_response(seed_key=seed_key)
            if not response:
                manifest["errors"].append(f"No cached response for {seed_key}")
                return False

            legislature_data = parse_legislature_members(response, seed_key=seed_key)

            seed_data = get_seed(seed_key)
            normalized = self._normalize(legislature_data, seed_data, response)

            export_dir = self.settings.scraper_export_dir / run_id
            export_dir.mkdir(parents=True, exist_ok=True)
            normalized["exported_at"] = utc_now_iso()
            export_json(normalized, export_dir, run_id=run_id)
            manifest["outputs"]["json_export"] = str(export_dir)

            if write_neo4j:
                if not self.neo4j_sink:
                    self.neo4j_sink = Neo4jSink(self.settings)
                    self.neo4j_sink.init()
                self.neo4j_sink.upsert(normalized)
                manifest["outputs"]["neo4j"] = "upserted"

            if write_meili:
                if not self.meili_sink:
                    self.meili_sink = MeiliSink(self.settings)
                    self.meili_sink.init()
                self.meili_sink.upsert(normalized)
                manifest["outputs"]["meilisearch"] = "upserted"

            manifest["completed_at"] = utc_now_iso()
            manifest["status"] = "success"

        except Exception as e:
            manifest["errors"].append(str(e))
            manifest["status"] = "error"
            manifest["completed_at"] = utc_now_iso()
            return False
        finally:
            manifest_path = self.settings.scraper_cache_dir / "manifests" / f"{run_id}.json"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        return True

    def run_all(
        self,
        write_neo4j: bool = False,
        write_meili: bool = False,
        force: bool = False,
        revalidate: bool = False,
    ) -> bool:
        seeds = load_seeds()
        all_success = True
        for seed_key in seeds.keys():
            success = self.run_single(
                seed_key=seed_key,
                write_neo4j=write_neo4j,
                write_meili=write_meili,
                force=force,
                revalidate=revalidate,
            )
            if not success:
                all_success = False
        return all_success

    def _normalize(
        self, legislature_data: Any, seed_data: Dict[str, Any], response: Any
    ) -> Dict[str, Any]:
        parties = {}
        legislatures = {}
        persons = {}
        mandates = []
        evidence_list = []

        parliament = seed_data.get("hints", {}).get("parliament", "")
        state = seed_data.get("hints", {}).get("state", "")
        legislature_number = seed_data.get("hints", {}).get("legislature_number")
        time_range = seed_data.get("expected_time_range", {})

        if parliament and state and legislature_number:
            from scraper.utils.ids import generate_legislature_id

            legislature_id = generate_legislature_id(parliament, state, legislature_number)
            legislature = Legislature(
                id=legislature_id,
                parliament=parliament,
                state=state,
                number=legislature_number,
                start_date=time_range.get("start", ""),
                end_date=time_range.get("end", ""),
                evidence_ids=[legislature_data.evidence_id],
            )
            legislatures[legislature_id] = legislature

        for person, mandate in legislature_data.members:
            persons[person.id] = person

            if mandate.party_name:
                from scraper.utils.ids import generate_party_id

                party_id = generate_party_id(mandate.party_name)
                if party_id not in parties:
                    parties[party_id] = Party(
                        id=party_id,
                        name=mandate.party_name,
                        evidence_ids=mandate.evidence_ids,
                    )

            mandates.append(mandate)

        evidence = Evidence(
            id=legislature_data.evidence_id,
            endpoint_kind="parse",
            page_title=response.page_title,
            page_id=response.page_id,
            revision_id=response.revision_id,
            source_url=f"https://de.wikipedia.org/wiki/{response.page_title}",
            retrieved_at=utc_now_iso(),
            sha256="",
        )
        evidence_list.append(evidence)

        return {
            "persons": list(persons.values()),
            "parties": list(parties.values()),
            "legislatures": list(legislatures.values()),
            "mandates": mandates,
            "evidence": evidence_list,
        }

