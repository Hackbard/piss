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
from typing import Optional, List, Any
from uuid import uuid5
from scraper.utils.ids import NAMESPACE_PERSON


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
        ingest_dip: bool = False,
        reconcile: bool = False,
        dip_wahlperiode: Optional[List[int]] = None,
        fetch_person_pages: bool = True,
    ) -> bool:
        run_id = str(uuid4())
        manifest: Dict[str, Any] = {
            "run_id": run_id,
            "started_at": utc_now_iso(),
            "seed_key": seed_key,
            "requests": [],
            "cache_hits": [],
            "cache_misses": [],
            "dip_requests": [],
            "reconcile_summary": {},
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
            normalized = self._normalize(legislature_data, seed_data, response, run_id=run_id, fetch_person_pages=fetch_person_pages, force=force)
            
            if not normalized:
                manifest["errors"].append("Normalization returned None")
                return False

            export_dir = self.settings.scraper_export_dir / run_id
            export_dir.mkdir(parents=True, exist_ok=True)
            normalized["exported_at"] = utc_now_iso()
            export_json(normalized, export_dir, run_id=run_id)
            manifest["outputs"]["json_export"] = str(export_dir)

            dip_records: List[Any] = []
            if ingest_dip or reconcile:
                from scraper.sources.dip.ingest import ingest_person_list_sync
                from scraper.models.domain import DipPersonRecord
                from scraper.utils.ids import generate_evidence_id
                from scraper.utils.hashing import sha256_hash_json

                if not self.settings.dip_api_key:
                    manifest["errors"].append(
                        "DIP_API_KEY not set. DIP ingest/reconcile requires API key."
                    )
                    if ingest_dip:
                        return False
                else:
                    # Default: Load ALL Wahlperioden dynamically (1-50 covers all current + future)
                    # If specific WPs are given, use those; otherwise load everything
                    if dip_wahlperiode:
                        wahlperiode_list = dip_wahlperiode
                    else:
                        # Load all Wahlperioden - from 1 to DIP_MAX_WAHLPERIODE (configurable via env)
                        # Non-existent WPs will return empty results, which we handle gracefully
                        max_wp = self.settings.dip_max_wahlperiode
                        wahlperiode_list = list(range(1, max_wp + 1))
                    
                    # Process each Wahlperiode individually for better cache granularity
                    all_dip_persons = []
                    for wp in wahlperiode_list:
                        try:
                            wp_persons = ingest_person_list_sync([wp], run_id, force=force)
                            if wp_persons:  # Only add if we got results
                                all_dip_persons.extend(wp_persons)
                                manifest["dip_requests"].append(
                                    {"wahlperiode": wp, "count": len(wp_persons)}
                                )
                        except Exception as e:
                            # If WP doesn't exist or API error, skip it silently (for future WPs)
                            # Only log if it's a real error (not just "WP doesn't exist")
                            error_str = str(e).lower()
                            if "401" in error_str or "unauthorized" in error_str:
                                manifest["errors"].append(f"DIP API authentication failed for WP {wp}")
                                if ingest_dip:
                                    return False
                            # For other errors (like non-existent WP), just continue silently
                    
                    dip_persons = all_dip_persons

                for dip_person in dip_persons:
                    evidence_id = generate_evidence_id(
                        0,
                        0,
                        "dip_person",
                        sha256_hash_json(dip_person.model_dump()),
                    )
                    dip_record = DipPersonRecord(
                        id=str(uuid5(NAMESPACE_PERSON, f"dip:{dip_person.id}")),
                        dip_person_id=dip_person.id,
                        vorname=dip_person.vorname,
                        nachname=dip_person.nachname,
                        namenszusatz=dip_person.namenszusatz,
                        titel=dip_person.titel,
                        fraktion=dip_person.fraktion,
                        wahlperiode=dip_person.wahlperiode,
                        person_roles=dip_person.person_roles,
                        evidence_ids=[evidence_id],
                    )
                    dip_records.append(dip_record)

            if reconcile:
                from scraper.models.domain import WikipediaPersonRecord
                from scraper.reconcile.wiki_dip import reconcile_wiki_dip

                wiki_records = []
                for person in normalized.get("persons", []):
                    # Build provenance from person data and metadata
                    provenance = None
                    if person.wikipedia_title:
                        from scraper.cache.mediawiki_cache import get_cached_metadata
                        from urllib.parse import quote
                        
                        metadata = get_cached_metadata(person.wikipedia_title)
                        if metadata:
                            page_title_encoded = quote(person.wikipedia_title.replace("_", " "), safe="")
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
                    
                    wiki_record = WikipediaPersonRecord(
                        id=person.id,
                        wikipedia_title=person.wikipedia_title,
                        wikipedia_url=person.wikipedia_url,
                        page_id=person.provenance.source_page_id if person.provenance else (provenance.get("page_id") if provenance else 0),
                        revision_id=person.provenance.revision_id if person.provenance else (provenance.get("revision_id") if provenance else 0),
                        name=person.name,
                        birth_date=person.birth_date,
                        death_date=person.death_date,
                        intro=person.intro,
                        evidence_ids=person.evidence_ids,
                        provenance=provenance,
                    )
                    
                    # Validate: if intro is present, must have at least 2 evidence IDs
                    if wiki_record.intro and len(wiki_record.evidence_ids) < 2:
                        import sys
                        print(f"  ⚠ Warning: WikipediaPersonRecord {wiki_record.wikipedia_title} has intro but only {len(wiki_record.evidence_ids)} evidence ID(s). Expected at least 2.", file=sys.stderr)
                    wiki_records.append(wiki_record)
                
                normalized["wikipedia_person_records"] = wiki_records

                canonical_persons, assertions = reconcile_wiki_dip(
                    wiki_records, dip_records, use_overrides=True
                )

                accepted = sum(1 for a in assertions if a.status == "accepted")
                pending = sum(1 for a in assertions if a.status == "pending")
                rejected = sum(1 for a in assertions if a.status == "rejected")

                manifest["reconcile_summary"] = {
                    "accepted": accepted,
                    "pending": pending,
                    "rejected": rejected,
                    "canonical_persons_count": len(canonical_persons),
                    "assertions_count": len(assertions),
                }

                normalized["canonical_persons"] = canonical_persons
                normalized["link_assertions"] = assertions
                normalized["dip_person_records"] = dip_records

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
            import traceback
            import sys
            error_msg = str(e)
            traceback_str = traceback.format_exc()
            manifest["errors"].append(error_msg)
            manifest["error_traceback"] = traceback_str
            manifest["status"] = "error"
            manifest["completed_at"] = utc_now_iso()
            print(f"✗ Pipeline error: {error_msg}", file=sys.stderr)
            print(f"Traceback:\n{traceback_str}", file=sys.stderr)
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
        ingest_dip: bool = False,
        reconcile: bool = False,
        dip_wahlperiode: Optional[List[int]] = None,
        fetch_person_pages: bool = True,
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
                ingest_dip=ingest_dip,
                reconcile=reconcile,
                dip_wahlperiode=dip_wahlperiode,
                fetch_person_pages=fetch_person_pages,
            )
            if not success:
                all_success = False
        return all_success

    def _normalize(
        self, legislature_data: Any, seed_data: Dict[str, Any], response: Any, run_id: str, fetch_person_pages: bool = True, force: bool = False
    ) -> Dict[str, Any]:
        parties = {}
        legislatures = {}
        persons = {}
        mandates = []
        evidence_list = []

        hints = seed_data.get("hints") or {}
        parliament = hints.get("parliament", "")
        state = hints.get("state", "")
        legislature_number = hints.get("legislature_number")
        time_range = seed_data.get("expected_time_range") or {}

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

        # Update evidence index (page-level, no snippet_ref)
        from scraper.cache.evidence_index import update_evidence_index
        from scraper.cache.mediawiki_cache import get_cache_path
        
        member_list_evidence_id = legislature_data.evidence_id
        cache_path = get_cache_path(response.page_title, response.revision_id, "parse")
        metadata_path = cache_path / "metadata.json"
        raw_path = cache_path / "raw.json"
        
        # Load metadata for sha256
        metadata_sha256 = None
        if metadata_path.exists():
            try:
                import json as json_module
                with open(metadata_path, "r", encoding="utf-8") as f:
                    metadata = json_module.load(f)
                    metadata_sha256 = metadata.get("sha256")
            except (IOError, json_module.JSONDecodeError):
                pass
        
        # Update evidence index once (page-level, no snippet_ref)
        if metadata_path.exists() and raw_path.exists():
            update_evidence_index(
                evidence_id=member_list_evidence_id,
                source_kind="mediawiki",
                cache_metadata_path=metadata_path,
                cache_raw_path=raw_path,
                page_title=response.page_title,
                page_id=response.page_id,
                revision_id=response.revision_id,
                sha256=metadata_sha256,
            )

        person_enrichment_stats = {"total": 0, "cached": 0, "fetched": 0, "failed": 0, "enriched": 0}
        
        for person, mandate in legislature_data.members:
            # Fetch individual person page to get intro, birth_date, etc.
            if fetch_person_pages and person.wikipedia_title:
                person_enrichment_stats["total"] += 1
                try:
                    from scraper.cache.mediawiki_cache import (
                        fetch_and_cache_parse,
                        get_latest_manifest_path,
                    )
                    from scraper.parsers.person_page import parse_person_page
                    import asyncio
                    import logging
                    import json
                    
                    import sys
                    
                    # Check if cached
                    latest_path = get_latest_manifest_path(person.wikipedia_title)
                    was_cached = latest_path.exists() and not force
                    
                    # Fetch person page (uses cache if available, unless force=True)
                    # fetch_and_cache_parse handles cache automatically
                    person_response = asyncio.run(
                        fetch_and_cache_parse(
                            page_title=person.wikipedia_title,
                            run_id=run_id,
                            force=force,
                            revalidate=False
                        )
                    )
                    
                    if person_response:
                        if was_cached:
                            person_enrichment_stats["cached"] += 1
                            print(f"✓ Person page cached: {person.wikipedia_title}", file=sys.stderr)
                        else:
                            person_enrichment_stats["fetched"] += 1
                            print(f"✓ Person page fetched: {person.wikipedia_title}", file=sys.stderr)
                        
                        # Parse person page to get intro, birth_date, etc.
                        parsed_person = parse_person_page(person_response)
                        
                        # Check if we got new data
                        has_new_data = False
                        if parsed_person.birth_date:
                            person.birth_date = parsed_person.birth_date
                            person.birth_date_status = parsed_person.birth_date_status
                            has_new_data = True
                        elif parsed_person.birth_date_status != "unknown":
                            person.birth_date_status = parsed_person.birth_date_status
                        
                        if parsed_person.death_date:
                            person.death_date = parsed_person.death_date
                            has_new_data = True
                        if parsed_person.intro:
                            person.intro = parsed_person.intro
                            has_new_data = True
                        if parsed_person.unstructured_evidence:
                            person.unstructured_evidence = parsed_person.unstructured_evidence
                            has_new_data = True
                        
                        # Merge data quality flags
                        person.data_quality_flags = list(set(person.data_quality_flags + parsed_person.data_quality_flags))
                        
                        # Merge evidence_refs (preferred) and evidence_ids (legacy)
                        # Deduplicate by evidence_id + purpose + snippet_ref hash
                        existing_refs = {(ref.evidence_id, ref.purpose, str(ref.snippet_ref)): ref for ref in person.evidence_refs}
                        for ref in parsed_person.evidence_refs:
                            key = (ref.evidence_id, ref.purpose, str(ref.snippet_ref))
                            if key not in existing_refs:
                                existing_refs[key] = ref
                        person.evidence_refs = list(existing_refs.values())
                        
                        # Update legacy evidence_ids from merged evidence_refs
                        all_evidence_ids = set(person.evidence_ids)  # Keep existing legacy IDs
                        for ref in person.evidence_refs:
                            all_evidence_ids.add(ref.evidence_id)
                        person.evidence_ids = list(all_evidence_ids)
                        
                        # Validate: if intro is present, we must have at least 2 evidence IDs
                        # (one from member list, one from person page)
                        if person.intro and len(person.evidence_ids) < 2:
                            import sys
                            print(f"  ⚠ Warning: Person {person.wikipedia_title} has intro but only {len(person.evidence_ids)} evidence ID(s). Expected at least 2 (member list + person page).", file=sys.stderr)
                        
                        if has_new_data:
                            person_enrichment_stats["enriched"] += 1
                            import sys
                            print(f"  → Enriched: birth_date={parsed_person.birth_date is not None} (status={parsed_person.birth_date_status}), intro={len(parsed_person.intro) if parsed_person.intro else 0} chars, evidence_ids={len(person.evidence_ids)}", file=sys.stderr)
                    else:
                        person_enrichment_stats["failed"] += 1
                        import sys
                        print(f"✗ Person page fetch returned None: {person.wikipedia_title}", file=sys.stderr)
                except Exception as e:
                    person_enrichment_stats["failed"] += 1
                    # If person page fetch/parse fails, continue with basic person data
                    # (name and wikipedia_title from table are still available)
                    import sys
                    import traceback
                    print(f"✗ Failed to fetch/enrich person page {person.wikipedia_title}: {e}", file=sys.stderr)
                    print(f"  Traceback: {traceback.format_exc()}", file=sys.stderr)
                    pass
            
            persons[person.id] = person
            
            # Note: snippet_refs are stored in EvidenceRef on Mandate (membership_row), not in Evidence index
            # Evidence index is page-level only, row-level references are entity-level (EvidenceRef)

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
        
        # Log person enrichment stats
        if fetch_person_pages and person_enrichment_stats["total"] > 0:
            import sys
            print(f"\nPerson enrichment stats: {person_enrichment_stats['total']} total, "
                  f"{person_enrichment_stats['cached']} cached, "
                  f"{person_enrichment_stats['fetched']} fetched, "
                  f"{person_enrichment_stats['enriched']} enriched, "
                  f"{person_enrichment_stats['failed']} failed", file=sys.stderr)

        # Get metadata to retrieve sha256 and retrieved_at
        from scraper.cache.mediawiki_cache import get_cached_metadata
        from urllib.parse import quote
        
        metadata = get_cached_metadata(response.page_title)
        sha256 = metadata.sha256 if metadata else ""
        retrieved_at = metadata.retrieved_at if metadata else utc_now_iso()
        
        # Normalize source_url: URL-encode and add oldid parameter for reproducibility
        page_title_encoded = quote(response.page_title.replace("_", " "), safe="")
        source_url = f"https://de.wikipedia.org/wiki/{page_title_encoded}"
        if response.revision_id:
            source_url_canonical = f"{source_url}?oldid={response.revision_id}"
        else:
            source_url_canonical = source_url
        
        evidence = Evidence(
            id=legislature_data.evidence_id,
            endpoint_kind="parse",
            page_title=response.page_title,
            page_id=response.page_id,
            revision_id=response.revision_id,
            source_url=source_url_canonical,
            retrieved_at=retrieved_at,
            sha256=sha256,
        )
        evidence_list.append(evidence)

        return {
            "persons": list(persons.values()),
            "parties": list(parties.values()),
            "legislatures": list(legislatures.values()),
            "mandates": mandates,
            "evidence": evidence_list,
        }

