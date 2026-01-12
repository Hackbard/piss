import sys
from pathlib import Path
import re
from typing import Any, Optional
from uuid import uuid4

import typer
from typer import Option

from scraper.config import get_settings
from scraper.logging import setup_logging
from scraper.pipeline.run import PipelineRunner

app = typer.Typer(help="Wikipedia Parliament Scraper")
settings = get_settings()
setup_logging()


@app.command()
def seed(
    validate: bool = Option(False, "--validate", help="Validate seed configuration"),
    discover: bool = Option(False, "--discover", help="Discover seeds for landtage"),
    landtage: bool = Option(False, "--landtage", help="Discover landtage seeds"),
    registry: Optional[Path] = Option(None, "--registry", help="Path to landtage registry"),
    output: Optional[Path] = Option(None, "--output", help="Output path for discovered seeds"),
    pin_revisions: bool = Option(True, "--pin-revisions/--no-pin-revisions", help="Pin page_id and revision_id in seeds"),
    force: bool = Option(False, "--force", help="Force refetch, ignore cache"),
) -> None:
    """Seed management commands."""
    if discover or landtage:
        import asyncio
        from scraper.seeds.discover_landtage import discover_landtage_seeds

        try:
            typer.echo("Discovering landtage seeds...", err=True)
            manifest = asyncio.run(
                discover_landtage_seeds(
                    registry_path=registry,
                    output_path=output,
                    pin_revisions=pin_revisions,
                    force=force,
                )
            )
            
            typer.echo(f"✓ Discovery complete:", err=True)
            typer.echo(f"  Found: {len(manifest['found_titles'])} titles", err=True)
            typer.echo(f"  Validated: {len(manifest['validated'])} seeds", err=True)
            typer.echo(f"  Rejected: {len(manifest['rejected'])} titles", err=True)
            typer.echo(f"  Output: {manifest['output_file']}", err=True)
            
            if manifest["errors"]:
                typer.echo(f"  Errors: {len(manifest['errors'])}", err=True)
            
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Discovery failed: {e}", err=True)
            sys.exit(1)
    elif validate:
        from scraper.cache.mediawiki_cache import validate_seeds

        try:
            validate_seeds()
            typer.echo("✓ Seeds validation passed", err=True)
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Seeds validation failed: {e}", err=True)
            sys.exit(2)
    else:
        typer.echo("Error: Must specify --validate or --discover --landtage", err=True)
        sys.exit(2)


@app.command()
def fetch(
    legislature: bool = Option(False, "--legislature", help="Fetch legislature page"),
    person: bool = Option(False, "--person", help="Fetch person page"),
    seed: Optional[str] = Option(None, "--seed", help="Seed key"),
    title: Optional[str] = Option(None, "--title", help="Wikipedia page title"),
    force: bool = Option(False, "--force", help="Force refetch, ignore cache"),
    revalidate: bool = Option(False, "--revalidate", help="Revalidate revision"),
) -> None:
    """Fetch pages from MediaWiki API."""
    from scraper.cache.mediawiki_cache import fetch_legislature_page, fetch_person_page

    run_id = str(uuid4())

    if legislature and seed:
        try:
            fetch_legislature_page(seed_key=seed, run_id=run_id, force=force, revalidate=revalidate)
            typer.echo(f"✓ Fetched legislature for seed: {seed}", err=True)
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Fetch failed: {e}", err=True)
            sys.exit(1)
    elif person and title:
        try:
            fetch_person_page(page_title=title, run_id=run_id, force=force, revalidate=revalidate)
            typer.echo(f"✓ Fetched person page: {title}", err=True)
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Fetch failed: {e}", err=True)
            sys.exit(1)
    else:
        typer.echo("Error: Must specify --legislature --seed or --person --title", err=True)
        sys.exit(2)


@app.command()
def parse(
    legislature: bool = Option(False, "--legislature", help="Parse legislature page"),
    seed: Optional[str] = Option(None, "--seed", help="Seed key"),
) -> None:
    """Parse fetched pages."""
    from scraper.cache.mediawiki_cache import get_cached_parse_response
    from scraper.parsers.legislature_members import parse_legislature_members

    if legislature and seed:
        try:
            response = get_cached_parse_response(seed_key=seed)
            if not response:
                typer.echo(f"✗ No cached data found for seed: {seed}", err=True)
                sys.exit(1)
            result = parse_legislature_members(response, seed_key=seed)
            typer.echo(f"✓ Parsed {len(result.members)} members", err=True)
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Parse failed: {e}", err=True)
            sys.exit(1)
    else:
        typer.echo("Error: Must specify --legislature --seed", err=True)
        sys.exit(2)


@app.command()
def dip(
    ingest: bool = Option(False, "--ingest", help="Ingest DIP persons"),
    persons: bool = Option(False, "--persons", help="Ingest persons"),
    from_wp: Optional[int] = Option(None, "--from-wp", help="From Wahlperiode"),
    to_wp: Optional[int] = Option(None, "--to-wp", help="To Wahlperiode"),
    detail: bool = Option(False, "--detail", help="Fetch person details"),
    force: bool = Option(False, "--force", help="Force refetch"),
) -> None:
    """DIP API operations."""
    from scraper.sources.dip.ingest import ingest_person_list_sync
    from uuid import uuid4

    if ingest and persons:
        run_id = str(uuid4())
        from_wp_val = from_wp or 1
        to_wp_val = to_wp or 20
        wahlperiode = list(range(from_wp_val, to_wp_val + 1))

        try:
            dip_persons = ingest_person_list_sync(wahlperiode, run_id, force=force)
            typer.echo(f"✓ Ingested {len(dip_persons)} DIP persons for WP {from_wp_val}-{to_wp_val}", err=True)
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ DIP ingest failed: {e}", err=True)
            sys.exit(1)
    else:
        typer.echo("Error: Must specify --ingest --persons", err=True)
        sys.exit(2)


@app.command()
def reconcile(
    wiki_dip: bool = Option(False, "--wiki-dip", help="Reconcile Wikipedia and DIP"),
    seed: Optional[str] = Option(None, "--seed", help="Seed key"),
    use_overrides: bool = Option(True, "--use-overrides/--no-overrides", help="Use link overrides"),
    write_neo4j: bool = Option(False, "--write-neo4j", help="Write to Neo4j"),
    write_meili: bool = Option(False, "--write-meili", help="Write to Meilisearch"),
) -> None:
    """Reconcile data sources."""
    if wiki_dip and seed:
        from scraper.cache.mediawiki_cache import get_cached_parse_response, get_seed
        from scraper.parsers.legislature_members import parse_legislature_members
        from scraper.models.domain import WikipediaPersonRecord, DipPersonRecord
        from scraper.reconcile.wiki_dip import reconcile_wiki_dip
        from scraper.sources.dip.ingest import ingest_person_list_sync
        from uuid import uuid4

        try:
            response = get_cached_parse_response(seed_key=seed)
            if not response:
                typer.echo(f"✗ No cached Wikipedia data for seed: {seed}", err=True)
                sys.exit(1)

            legislature_data = parse_legislature_members(response, seed_key=seed)
            seed_data = get_seed(seed_key)

            wiki_records = []
            for person, _ in legislature_data.members:
                wiki_record = WikipediaPersonRecord(
                    id=person.id,
                    wikipedia_title=person.wikipedia_title,
                    wikipedia_url=person.wikipedia_url,
                    page_id=0,
                    revision_id=0,
                    name=person.name,
                    birth_date=person.birth_date,
                    death_date=person.death_date,
                    intro=person.intro,
                    evidence_ids=person.evidence_ids,
                )
                wiki_records.append(wiki_record)

            run_id = str(uuid4())
            wahlperiode = [19]
            dip_persons = ingest_person_list_sync(wahlperiode, run_id, force=False)

            from scraper.models.domain import DipPersonRecord
            from scraper.utils.ids import generate_evidence_id, NAMESPACE_PERSON
            from scraper.utils.hashing import sha256_hash_json
            from uuid import uuid5 as uuid5_func

            dip_records = []
            for dip_person in dip_persons:
                evidence_id = generate_evidence_id(
                    0, 0, "dip_person", sha256_hash_json(dip_person.model_dump())
                )
                dip_record = DipPersonRecord(
                    id=str(uuid5_func(NAMESPACE_PERSON, f"dip:{dip_person.id}")),
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

            canonical_persons, assertions = reconcile_wiki_dip(
                wiki_records, dip_records, use_overrides=use_overrides
            )

            accepted = sum(1 for a in assertions if a.status == "accepted")
            pending = sum(1 for a in assertions if a.status == "pending")
            rejected = sum(1 for a in assertions if a.status == "rejected")

            typer.echo(f"✓ Reconciliation complete:", err=True)
            typer.echo(f"  Accepted: {accepted}", err=True)
            typer.echo(f"  Pending: {pending}", err=True)
            typer.echo(f"  Rejected: {rejected}", err=True)
            typer.echo(f"  Canonical persons: {len(canonical_persons)}", err=True)

            if write_neo4j or write_meili:
                normalized = {
                    "canonical_persons": canonical_persons,
                    "link_assertions": assertions,
                    "dip_person_records": dip_records,
                }
                if write_neo4j:
                    from scraper.sinks.neo4j import Neo4jSink
                    sink = Neo4jSink(settings)
                    sink.init()
                    sink.upsert_reconciliation(normalized)
                if write_meili:
                    from scraper.sinks.meili import MeiliSink
                    sink = MeiliSink(settings)
                    sink.init()
                    sink.upsert_reconciliation(normalized)

            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Reconciliation failed: {e}", err=True)
            sys.exit(1)
    else:
        typer.echo("Error: Must specify --wiki-dip --seed", err=True)
        sys.exit(2)


@app.command()
def pipeline(
    seed: Optional[str] = Option(None, "--seed", help="Seed key (if not provided, runs all)"),
    write_neo4j: bool = Option(False, "--write-neo4j", help="Write to Neo4j"),
    write_meili: bool = Option(False, "--write-meili", help="Write to Meilisearch"),
    force: bool = Option(False, "--force", help="Force refetch"),
    revalidate: bool = Option(False, "--revalidate", help="Revalidate revisions"),
    ingest_dip: bool = Option(False, "--ingest-dip", help="Ingest DIP data"),
    reconcile: bool = Option(False, "--reconcile", help="Reconcile Wikipedia and DIP"),
    dip_wahlperiode: Optional[str] = Option(None, "--dip-wahlperiode", help="DIP Wahlperiode (comma-separated)"),
    fetch_person_pages: bool = Option(True, "--fetch-person-pages/--no-fetch-person-pages", help="Fetch individual person pages for intro, birth_date, etc."),
) -> None:
    """Run the complete pipeline."""
    runner = PipelineRunner(settings)

    dip_wp_list = None
    if dip_wahlperiode:
        dip_wp_list = [int(x.strip()) for x in dip_wahlperiode.split(",")]

    try:
        if seed:
            success = runner.run_single(
                seed_key=seed,
                write_neo4j=write_neo4j,
                write_meili=write_meili,
                force=force,
                revalidate=revalidate,
                ingest_dip=ingest_dip,
                reconcile=reconcile,
                dip_wahlperiode=dip_wp_list,
                fetch_person_pages=fetch_person_pages,
            )
        else:
            success = runner.run_all(
                write_neo4j=write_neo4j,
                write_meili=write_meili,
                force=force,
                revalidate=revalidate,
                ingest_dip=ingest_dip,
                reconcile=reconcile,
                dip_wahlperiode=dip_wp_list,
                fetch_person_pages=fetch_person_pages,
            )
        sys.exit(0 if success else 1)
    except Exception as e:
        typer.echo(f"✗ Pipeline failed: {e}", err=True)
        sys.exit(1)


@app.command()
def evidence(
    resolve: bool = Option(False, "--resolve", help="Resolve evidence IDs"),
    ids: Optional[str] = Option(None, "--ids", help="Comma-separated evidence IDs"),
    format: str = Option("json", "--format", help="Output format: json, yaml, md"),
    with_snippets: bool = Option(False, "--with-snippets", help="Include snippets"),
    max_len: int = Option(500, "--max-len", help="Maximum snippet length"),
    prefer: str = Option("table_row", "--prefer", help="Preferred snippet type: table_row or lead_paragraph"),
    resolve_from_meili: bool = Option(False, "--resolve-from-meili", help="Resolve from Meilisearch query"),
    query: Optional[str] = Option(None, "--query", help="Meilisearch query string"),
    index: str = Option("persons", "--index", help="Meilisearch index name"),
    limit: int = Option(5, "--limit", help="Limit results from Meilisearch"),
) -> None:
    """Evidence resolver commands."""
    from scraper.evidence.resolver import EvidenceResolver
    from scraper.evidence.formatters import (
        format_resolved_evidence_json,
        format_resolved_evidence_yaml,
        format_resolved_evidence_markdown,
    )
    
    resolver = EvidenceResolver(backend="file_cache")
    evidence_ids = []
    evidence_refs = []
    
    if resolve_from_meili:
        if not query:
            typer.echo("Error: --query required when using --resolve-from-meili", err=True)
            sys.exit(1)
        
        # Query Meilisearch
        from scraper.sinks.meili import MeiliSink
        from scraper.models.domain import EvidenceRef
        meili = MeiliSink(settings)
        meili.init()
        
        search_index = meili.client.index(index)
        search_results = search_index.search(query, {"limit": limit})
        
        # Prefer evidence_refs (new approach), fallback to evidence_snippet_refs (old format), then evidence_ids (legacy)
        for hit in search_results.get("hits", []):
            # Try to get evidence_refs first (preferred)
            hit_evidence_refs = hit.get("evidence_refs", [])
            if isinstance(hit_evidence_refs, list) and hit_evidence_refs:
                for ref_dict in hit_evidence_refs:
                    try:
                        evidence_ref = EvidenceRef(**ref_dict)
                        evidence_refs.append(evidence_ref)
                    except Exception:
                        pass
            
            # Fallback: evidence_snippet_refs (old format) - convert to EvidenceRef
            if (not hit_evidence_refs or not isinstance(hit_evidence_refs, list) or not hit_evidence_refs):
                hit_evidence_snippet_refs = hit.get("evidence_snippet_refs", {})
                if isinstance(hit_evidence_snippet_refs, dict) and hit_evidence_snippet_refs:
                    for evidence_id, snippet_ref in hit_evidence_snippet_refs.items():
                        if snippet_ref and isinstance(snippet_ref, dict):
                            try:
                                evidence_ref = EvidenceRef(
                                    evidence_id=evidence_id,
                                    snippet_ref=snippet_ref,
                                    purpose="membership_row" if snippet_ref.get("type") == "table_row" else None,
                                )
                                evidence_refs.append(evidence_ref)
                            except Exception:
                                pass
            
            # Final fallback: legacy evidence_ids (if no evidence_refs or evidence_snippet_refs found)
            if (not hit_evidence_refs or not isinstance(hit_evidence_refs, list) or not hit_evidence_refs) and \
               (not hit.get("evidence_snippet_refs") or not isinstance(hit.get("evidence_snippet_refs"), dict) or not hit.get("evidence_snippet_refs")):
                hit_evidence_ids = hit.get("evidence_ids", [])
                if isinstance(hit_evidence_ids, list):
                    evidence_ids.extend(hit_evidence_ids)
        
        # Deduplicate evidence_ids (legacy fallback)
        if evidence_ids:
            evidence_ids = list(set(evidence_ids))
            typer.echo(f"Found {len(evidence_ids)} unique evidence IDs from Meilisearch (legacy)", err=True)
        
        if evidence_refs:
            typer.echo(f"Found {len(evidence_refs)} evidence references from Meilisearch", err=True)
        
        if not evidence_refs and not evidence_ids:
            typer.echo(f"No evidence_refs or evidence_ids found in Meilisearch results for query: {query}", err=True)
            sys.exit(1)
    
    elif resolve:
        if not ids:
            typer.echo("Error: --ids required when using --resolve", err=True)
            sys.exit(1)
        
        evidence_ids = [eid.strip() for eid in ids.split(",")]
    
    else:
        typer.echo("Error: Must specify --resolve or --resolve-from-meili", err=True)
        sys.exit(1)
    
    if not evidence_refs and not evidence_ids:
        typer.echo("Error: No evidence references or evidence IDs to resolve", err=True)
        sys.exit(1)
    
    # Validate prefer option (only used for legacy evidence_ids)
    if prefer not in ["table_row", "lead_paragraph"]:
        typer.echo(f"Error: --prefer must be 'table_row' or 'lead_paragraph', got: {prefer}", err=True)
        sys.exit(1)
    
    # Resolve evidence: prefer evidence_refs (new approach), fallback to evidence_ids (legacy)
    resolved = []
    if evidence_refs:
        resolved = resolver.resolve_refs(
            evidence_refs=evidence_refs,
            with_snippets=with_snippets,
            snippet_max_len=max_len,
        )
    elif evidence_ids:
        resolved = resolver.resolve(
            evidence_ids=evidence_ids,
            with_snippets=with_snippets,
            snippet_max_len=max_len,
            prefer_snippet=prefer,
        )
    
    if not resolved:
        typer.echo(f"Warning: No evidence resolved for {len(evidence_ids)} IDs", err=True)
        sys.exit(0)  # Exit 0, but warn
    
    # Format output
    if format == "json":
        output = format_resolved_evidence_json(resolved)
    elif format == "yaml":
        output = format_resolved_evidence_yaml(resolved)
    elif format == "md":
        output = format_resolved_evidence_markdown(resolved)
    else:
        typer.echo(f"Error: Unknown format: {format}", err=True)
        sys.exit(1)
    
    print(output)


@app.command()
def export(
    json: bool = Option(False, "--json", help="Export as JSON"),
    out: Optional[Path] = Option(None, "--out", help="Output directory"),
    run_id: Optional[str] = Option(None, "--run-id", help="Run ID to export"),
) -> None:
    """Export data."""
    from scraper.sinks.json_export import export_json

    if json and out:
        try:
            export_json(output_dir=Path(out), run_id=run_id)
            typer.echo(f"✓ Exported to {out}", err=True)
            sys.exit(0)
        except Exception as e:
            typer.echo(f"✗ Export failed: {e}", err=True)
            sys.exit(1)
    else:
        typer.echo("Error: Must specify --json --out", err=True)
        sys.exit(2)


@app.command()
def validate(
    from_date: Optional[str] = Option(None, "--from", help="Filter from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Option(None, "--to", help="Filter to date (YYYY-MM-DD)"),
    parliament: Optional[str] = Option(None, "--parliament", help="Filter by parliament ID"),
    strict: bool = Option(False, "--strict", help="Strict mode: missing evidence is ERROR"),
    json_output: bool = Option(False, "--json", help="Output as JSON"),
) -> None:
    """Validate data quality."""
    from datetime import date as date_type
    from scraper.validation.validator import DataValidator
    from scraper.sinks.neo4j import Neo4jSink
    from scraper.models.domain import Mandate, Party
    
    try:
        from_date_obj = date_type.fromisoformat(from_date) if from_date else None
        to_date_obj = date_type.fromisoformat(to_date) if to_date else None
    except ValueError as e:
        typer.echo(f"✗ Invalid date format: {e}", err=True)
        sys.exit(1)
    
    sink = Neo4jSink(settings)
    sink.init()
    
    with sink.driver.session() as session:
        mandates_result = session.run("""
            MATCH (m:Mandate)
            RETURN m.id as id, m.person_id as person_id, m.parliament_id as parliament_id,
                   m.legislature_id as legislature_id, m.party_code as party_code,
                   m.start_date as start_date, m.end_date as end_date, m.role as role,
                   m.evidence_ids as evidence_ids
        """)
        
        mandates = []
        skipped_count = 0
        for record in mandates_result:
            parliament_id = record.get("parliament_id")
            if not parliament_id:
                skipped_count += 1
                continue
            
            mandate = Mandate(
                id=record["id"],
                person_id=record["person_id"],
                parliament_id=parliament_id,
                legislature_id=record["legislature_id"],
                party_code=record.get("party_code"),
                start_date=record.get("start_date"),
                end_date=record.get("end_date"),
                role=record.get("role"),
                evidence_ids=record.get("evidence_ids", []),
            )
            mandates.append(mandate)
        
        if skipped_count > 0:
            typer.echo(f"⚠ Skipped {skipped_count} mandate(s) without parliament_id (legacy data)", err=True)
        
        parties_result = session.run("""
            MATCH (p:Party)
            RETURN p.id as id, p.code as code, p.name as name
        """)
        
        parties = []
        skipped_parties_count = 0
        for record in parties_result:
            code = record.get("code")
            if not code:
                skipped_parties_count += 1
                continue
            
            party = Party(
                id=record["id"],
                code=code,
                name=record.get("name", ""),
            )
            parties.append(party)
        
        if skipped_parties_count > 0:
            typer.echo(f"⚠ Skipped {skipped_parties_count} party/parties without code (legacy data)", err=True)
    
    validator = DataValidator(strict_mode=strict)
    result = validator.validate_all(
        mandates=mandates,
        parties=parties,
        from_date=from_date_obj,
        to_date=to_date_obj,
        parliament_id=parliament,
    )
    
    if json_output:
        import json
        print(json.dumps(result.to_dict(), indent=2))
    else:
        if result.errors:
            typer.echo(f"✗ Validation failed: {len(result.errors)} errors, {len(result.warnings)} warnings", err=True)
            for error in result.errors:
                typer.echo(f"  ERROR [{error['code']}]: {error['message']}", err=True)
        else:
            typer.echo(f"✓ Validation passed: {len(result.warnings)} warnings", err=True)
        
        if result.warnings:
            for warning in result.warnings:
                typer.echo(f"  WARN [{warning['code']}]: {warning['message']}", err=True)
    
    sys.exit(1 if result.has_errors() else 0)


@app.command()
def mandates(
    parliament: Optional[str] = Option(None, "--parliament", help="Parliament ID filter"),
    legislature: Optional[str] = Option(None, "--legislature", help="Legislature ID filter"),
    party: Optional[str] = Option(None, "--party", help="Party code filter (e.g. 'SPD', 'CDU')"),
    from_date: Optional[str] = Option(None, "--from", help="Start date filter (YYYY-MM-DD)"),
    to_date: Optional[str] = Option(None, "--to", help="End date filter (YYYY-MM-DD)"),
    person_id: Optional[str] = Option(None, "--person-id", help="Person ID filter"),
    person_name: Optional[str] = Option(None, "--person-name", help="Person name contains filter"),
    limit: int = Option(200, "--limit", help="Maximum results (1-1000)"),
    offset: int = Option(0, "--offset", help="Offset for pagination"),
    sort: str = Option("person_name", "--sort", help="Sort field: person_name, start_date, end_date, party_code"),
    sort_direction: str = Option("ASC", "--sort-direction", help="Sort direction: ASC or DESC"),
    json_output: bool = Option(False, "--json", help="Output as JSON"),
) -> None:
    """Query mandates with evidence-by-default."""
    from datetime import date
    
    from scraper.models.query import MandateQueryFilter, SortDirection, SortField
    from scraper.services.neo4j_query import Neo4jMandateQueryService
    
    try:
        from_date_obj = date.fromisoformat(from_date) if from_date else None
        to_date_obj = date.fromisoformat(to_date) if to_date else None
    except ValueError as e:
        typer.echo(f"✗ Invalid date format: {e}", err=True)
        sys.exit(1)
    
    try:
        sort_field = SortField(sort)
    except ValueError:
        typer.echo(f"✗ Invalid sort field: {sort}. Valid: person_name, start_date, end_date, party_code", err=True)
        sys.exit(1)
    
    try:
        sort_dir = SortDirection(sort_direction.upper())
    except ValueError:
        typer.echo(f"✗ Invalid sort direction: {sort_direction}. Valid: ASC, DESC", err=True)
        sys.exit(1)
    
    filter_obj = MandateQueryFilter(
        parliament_id=parliament,
        legislature_id=legislature,
        party_code=party,
        from_date=from_date_obj,
        to_date=to_date_obj,
        person_id=person_id,
        person_name_contains=person_name,
        limit=limit,
        offset=offset,
        sort=sort_field,
        sort_direction=sort_dir,
    )
    
    try:
        service = Neo4jMandateQueryService(settings)
        result = service.search(filter_obj)
        service.close()
        
        if json_output:
            import json
            print(json.dumps(result.model_dump(mode="json"), indent=2, default=str))
        else:
            typer.echo(f"Found {len(result.rows)} mandate(s)", err=True)
            if result.total is not None:
                typer.echo(f"Total: {result.total}", err=True)
            typer.echo("", err=True)
            
            for row in result.rows:
                end_date_str = row.end_date.isoformat() if row.end_date else "open"
                evidence_count = len(row.evidence_urls)
                typer.echo(f"  {row.person_name} ({row.person_id})", err=False)
                typer.echo(f"    Mandate: {row.mandate_id}", err=False)
                typer.echo(f"    Legislature: {row.legislature_name or row.legislature_id}", err=False)
                typer.echo(f"    Party: {row.party_code or 'N/A'}", err=False)
                typer.echo(f"    Period: {row.start_date.isoformat()} - {end_date_str}", err=False)
                typer.echo(f"    Evidence: {evidence_count} URL(s)", err=False)
                if row.evidence_urls:
                    for url in row.evidence_urls[:3]:
                        typer.echo(f"      - {url}", err=False)
                    if len(row.evidence_urls) > 3:
                        typer.echo(f"      ... and {len(row.evidence_urls) - 3} more", err=False)
                typer.echo("", err=False)
        
        sys.exit(0)
    except Exception as e:
        typer.echo(f"✗ Query failed: {e}", err=True)
        import traceback
        if settings.scraper_cache_dir:
            typer.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@app.command()
def legislature_stats(
    legislature_id: str = Option(..., "--legislature-id", help="Legislature ID"),
    json_output: bool = Option(False, "--json", help="Output as JSON"),
) -> None:
    """Get statistics for a legislature."""
    from scraper.services.neo4j_query import Neo4jLegislatureStatsService
    
    try:
        service = Neo4jLegislatureStatsService(settings)
        stats = service.get_legislature_stats(legislature_id)
        service.close()
        
        if json_output:
            import json
            print(json.dumps(stats.model_dump(mode="json"), indent=2, default=str))
        else:
            typer.echo(f"Legislature: {stats.legislature_name} ({stats.legislature_id})", err=False)
            typer.echo(f"Total Seats: {stats.total_seats or 'N/A'}", err=False)
            typer.echo("", err=False)
            typer.echo("Party Seats:", err=False)
            for party_code, seats in sorted(stats.party_seats.items()):
                typer.echo(f"  {party_code}: {seats}", err=False)
            typer.echo("", err=False)
            typer.echo(f"Evidence: {len(stats.evidence_urls)} URL(s)", err=False)
            if stats.evidence_urls:
                for url in stats.evidence_urls[:3]:
                    typer.echo(f"  - {url}", err=False)
                if len(stats.evidence_urls) > 3:
                    typer.echo(f"  ... and {len(stats.evidence_urls) - 3} more", err=False)
        
        sys.exit(0)
    except Exception as e:
        typer.echo(f"✗ Query failed: {e}", err=True)
        import traceback
        typer.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@app.command()
def person(
    person_id: Optional[str] = Option(None, "--id", help="Person ID"),
    name: Optional[str] = Option(None, "--name", help="Search by name (contains)"),
    limit: int = Option(20, "--limit", help="Maximum results for name search (1-100)"),
    json_output: bool = Option(False, "--json", help="Output as JSON"),
) -> None:
    """Lookup person by ID or search by name."""
    from scraper.services.neo4j_query import Neo4jPersonLookupService
    
    if not person_id and not name:
        typer.echo("✗ Must specify either --id or --name", err=True)
        sys.exit(1)
    
    if person_id and name:
        typer.echo("✗ Cannot specify both --id and --name", err=True)
        sys.exit(1)
    
    try:
        service = Neo4jPersonLookupService(settings)
        
        if person_id:
            person = service.find_by_id(person_id)
            service.close()
            
            if not person:
                typer.echo(f"✗ Person not found: {person_id}", err=True)
                sys.exit(1)
            
            if json_output:
                import json
                print(json.dumps(person.model_dump(mode="json"), indent=2, default=str))
            else:
                typer.echo(f"Person: {person.name} ({person.person_id})", err=False)
                if person.wikipedia_title:
                    typer.echo(f"Wikipedia: {person.wikipedia_title}", err=False)
                if person.birth_date:
                    typer.echo(f"Birth Date: {person.birth_date.isoformat()}", err=False)
                if person.death_date:
                    typer.echo(f"Death Date: {person.death_date.isoformat()}", err=False)
                if person.intro:
                    intro_preview = person.intro[:200] + "..." if len(person.intro) > 200 else person.intro
                    typer.echo(f"Intro: {intro_preview}", err=False)
                typer.echo(f"Evidence: {len(person.evidence_urls)} URL(s)", err=False)
                if person.evidence_urls:
                    for url in person.evidence_urls[:3]:
                        typer.echo(f"  - {url}", err=False)
                    if len(person.evidence_urls) > 3:
                        typer.echo(f"  ... and {len(person.evidence_urls) - 3} more", err=False)
        else:
            persons = service.search_by_name(name, limit=limit)
            service.close()
            
            if json_output:
                import json
                print(json.dumps([p.model_dump(mode="json") for p in persons], indent=2, default=str))
            else:
                typer.echo(f"Found {len(persons)} person(s)", err=False)
                for p in persons:
                    typer.echo(f"  {p.name} ({p.person_id})", err=False)
        
        sys.exit(0)
    except Exception as e:
        typer.echo(f"✗ Query failed: {e}", err=True)
        import traceback
        typer.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@app.command()
def reset_db(
    neo4j: bool = Option(False, "--neo4j", help="Reset Neo4j database (delete all nodes/relationships)"),
    meili: bool = Option(False, "--meili", help="Reset Meilisearch indexes (delete all indexes)"),
    yes: bool = Option(False, "--yes", help="Skip confirmation prompt"),
) -> None:
    """Reset database: delete all nodes/relationships in Neo4j and/or all indexes in Meilisearch."""
    if not neo4j and not meili:
        typer.echo("Error: Must specify at least --neo4j or --meili", err=True)
        sys.exit(2)
    
    if not yes:
        if neo4j:
            typer.echo("⚠ WARNING: This will delete ALL nodes and relationships in Neo4j!", err=True)
        if meili:
            typer.echo("⚠ WARNING: This will delete ALL indexes in Meilisearch!", err=True)
        typer.echo("", err=True)
        confirm = typer.prompt("Type 'yes' to confirm", default="no")
        if confirm.lower() != "yes":
            typer.echo("Aborted.", err=True)
            sys.exit(0)
    
    if neo4j:
        try:
            from scraper.sinks.neo4j import Neo4jSink
            
            sink = Neo4jSink(settings)
            with sink.driver.session() as session:
                labels = [
                    "Person", "Parliament", "Party", "Legislature", "Mandate", "Evidence",
                    "CanonicalPerson", "WikipediaPersonRecord", "DipPersonRecord", "PersonLinkAssertion"
                ]
                
                for label in labels:
                    result = session.run(f"MATCH (n:{label}) DETACH DELETE n RETURN count(n) as count")
                    record = result.single()
                    count = record["count"] if record else 0
                    if count > 0:
                        typer.echo(f"✓ Deleted {count} {label} node(s)", err=True)
                
                typer.echo("✓ Neo4j reset complete", err=True)
            sink.close()
        except Exception as e:
            typer.echo(f"✗ Neo4j reset failed: {e}", err=True)
            import traceback
            typer.echo(traceback.format_exc(), err=True)
            sys.exit(1)
    
    if meili:
        try:
            from scraper.sinks.meili import MeiliSink
            
            sink = MeiliSink(settings)
            indexes = ["persons", "mandates"]
            
            for index_name in indexes:
                try:
                    index = sink.client.get_index(index_name)
                    if index:
                        sink.client.delete_index(index_name)
                        typer.echo(f"✓ Deleted Meilisearch index: {index_name}", err=True)
                except Exception:
                    typer.echo(f"  Index {index_name} does not exist (skipping)", err=True)
            
            typer.echo("✓ Meilisearch reset complete", err=True)
        except Exception as e:
            typer.echo(f"✗ Meilisearch reset failed: {e}", err=True)
            import traceback
            typer.echo(traceback.format_exc(), err=True)
            sys.exit(1)
    
    typer.echo("✓ Database reset complete", err=True)
    sys.exit(0)


@app.command()
def repair_dates(
    dry_run: bool = Option(False, "--dry-run", help="Show what would be changed without making changes"),
) -> None:
    """Repair mandate dates: remove invalid strings and backfill from legislature."""
    from neo4j import GraphDatabase
    
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    
    try:
        with driver.session() as session:
            typer.echo("Step 1: Cleaning invalid date strings...", err=True)
            
            cleanup_query = """
            MATCH (m:Mandate)
            WHERE m.start_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"] 
               OR m.end_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"]
            WITH m,
                 CASE WHEN m.start_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"] THEN null ELSE m.start_date END as clean_start,
                 CASE WHEN m.end_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"] THEN null ELSE m.end_date END as clean_end
            SET m.start_date = clean_start,
                m.end_date = clean_end
            RETURN count(m) AS touched
            """
            
            if dry_run:
                count_query = """
                MATCH (m:Mandate)
                WHERE m.start_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"] 
                   OR m.end_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"]
                RETURN count(m) AS touched
                """
                result = session.run(count_query)
                touched = result.single()["touched"]
                typer.echo(f"  Would clean {touched} mandates", err=True)
            else:
                result = session.run(cleanup_query)
                touched = result.single()["touched"]
                typer.echo(f"  ✓ Cleaned {touched} mandates", err=True)
            
            typer.echo("Step 2: Normalizing legislature dates...", err=True)
            
            legislature_normalize_query = """
            MATCH (l:Legislature)
            WHERE l.start_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"]
               OR l.end_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"]
            WITH l,
                 CASE WHEN l.start_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"] THEN null ELSE l.start_date END as clean_start,
                 CASE WHEN l.end_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"] THEN null ELSE l.end_date END as clean_end
            SET l.start_date = clean_start,
                l.end_date = clean_end
            RETURN count(l) AS touched
            """
            
            if dry_run:
                count_query = """
                MATCH (l:Legislature)
                WHERE l.start_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"]
                   OR l.end_date IN ["unknown", "", "—", "–", "?", "n/a", "na", "-"]
                RETURN count(l) AS touched
                """
                result = session.run(count_query)
                touched = result.single()["touched"]
                typer.echo(f"  Would normalize {touched} legislatures", err=True)
            else:
                result = session.run(legislature_normalize_query)
                touched = result.single()["touched"]
                typer.echo(f"  ✓ Normalized {touched} legislatures", err=True)
            
            typer.echo("Step 2.5: Deriving legislature dates from mandates (if missing)...", err=True)
            
            derive_legislature_dates_query = """
            MATCH (l:Legislature)
            WHERE l.start_date IS NULL OR l.end_date IS NULL
            WITH l
            OPTIONAL MATCH (m:Mandate)
            WHERE m.legislature_id = l.id AND (m.start_date IS NOT NULL OR m.end_date IS NOT NULL)
            WITH l,
                 min(m.start_date) as min_start,
                 max(m.end_date) as max_end
            WHERE min_start IS NOT NULL OR max_end IS NOT NULL
            WITH l, min_start, max_end
            SET l.start_date = CASE WHEN l.start_date IS NULL THEN min_start ELSE l.start_date END,
                l.end_date = CASE WHEN l.end_date IS NULL THEN max_end ELSE l.end_date END
            RETURN count(l) AS updated
            """
            
            if dry_run:
                count_query = """
                MATCH (l:Legislature)
                WHERE l.start_date IS NULL OR l.end_date IS NULL
                WITH l
                OPTIONAL MATCH (m:Mandate)
                WHERE m.legislature_id = l.id AND (m.start_date IS NOT NULL OR m.end_date IS NOT NULL)
                WITH l,
                     min(m.start_date) as min_start,
                     max(m.end_date) as max_end
                WHERE min_start IS NOT NULL OR max_end IS NOT NULL
                RETURN count(l) AS would_update
                """
                result = session.run(count_query)
                updated = result.single()["would_update"]
                typer.echo(f"  Would derive dates for {updated} legislatures", err=True)
            else:
                result = session.run(derive_legislature_dates_query)
                updated = result.single()["updated"]
                typer.echo(f"  ✓ Derived dates for {updated} legislatures", err=True)
            
            typer.echo("Step 2.6: Creating missing IN_LEGISLATURE relationships...", err=True)
            
            create_relationships_query = """
            MATCH (m:Mandate), (l:Legislature)
            WHERE m.legislature_id = l.id AND NOT (m)-[:IN_LEGISLATURE]->(l)
            MERGE (m)-[:IN_LEGISLATURE]->(l)
            RETURN count(m) AS created
            """
            
            if dry_run:
                count_query = """
                MATCH (m:Mandate), (l:Legislature)
                WHERE m.legislature_id = l.id AND NOT (m)-[:IN_LEGISLATURE]->(l)
                RETURN count(m) AS would_create
                """
                result = session.run(count_query)
                created = result.single()["would_create"]
                typer.echo(f"  Would create {created} relationships", err=True)
            else:
                result = session.run(create_relationships_query)
                created = result.single()["created"]
                typer.echo(f"  ✓ Created {created} relationships", err=True)
            
            typer.echo("Step 3: Backfilling start_date from legislature...", err=True)
            
            diagnostic_query = """
            MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
            WHERE m.start_date IS NULL
            RETURN 
                count(m) as mandates_without_start,
                count(CASE WHEN l.start_date IS NOT NULL THEN 1 END) as legislatures_with_start
            """
            result = session.run(diagnostic_query)
            diag = result.single()
            mandates_without = diag["mandates_without_start"]
            legislatures_with = diag["legislatures_with_start"]
            
            if mandates_without > 0 or legislatures_with > 0:
                typer.echo(f"  [Diagnostic] Mandates ohne start_date: {mandates_without}, Legislatures mit start_date: {legislatures_with}", err=True)
            
            backfill_start_query = """
            MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
            WHERE m.start_date IS NULL AND l.start_date IS NOT NULL
            SET m.start_date = l.start_date,
                m.start_date_source = "legislature"
            RETURN count(m) AS backfilled
            """
            
            if dry_run:
                count_query = """
                MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                WHERE m.start_date IS NULL AND l.start_date IS NOT NULL
                RETURN count(m) AS backfilled
                """
                result = session.run(count_query)
                backfilled = result.single()["backfilled"]
                typer.echo(f"  Would backfill {backfilled} mandates", err=True)
            else:
                result = session.run(backfill_start_query)
                backfilled = result.single()["backfilled"]
                typer.echo(f"  ✓ Backfilled {backfilled} mandates", err=True)
            
            typer.echo("Step 4: Backfilling end_date from legislature...", err=True)
            
            diagnostic_query = """
            MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
            WHERE m.end_date IS NULL
            WITH m, l
            RETURN 
                count(m) as mandates_without_end,
                count(CASE WHEN l.end_date IS NOT NULL THEN 1 END) as legislatures_with_end
            """
            result = session.run(diagnostic_query)
            diag = result.single()
            mandates_without = diag["mandates_without_end"]
            legislatures_with = diag["legislatures_with_end"]
            
            total_legislatures_query = """
            MATCH (l:Legislature)
            WHERE l.end_date IS NOT NULL
            RETURN count(l) as total_with_end
            """
            result = session.run(total_legislatures_query)
            total_with_end = result.single()["total_with_end"]
            
            typer.echo(f"  [Diagnostic] Mandates ohne end_date: {mandates_without}, Legislatures mit end_date (total): {total_with_end}, Legislatures mit end_date (connected): {legislatures_with}", err=True)
            
            backfill_end_query = """
            MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
            WHERE m.end_date IS NULL AND l.end_date IS NOT NULL
            SET m.end_date = l.end_date,
                m.end_date_source = "legislature"
            RETURN count(m) AS backfilled
            """
            
            if dry_run:
                count_query = """
                MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                WHERE m.end_date IS NULL AND l.end_date IS NOT NULL
                RETURN count(m) AS backfilled
                """
                result = session.run(count_query)
                backfilled = result.single()["backfilled"]
                typer.echo(f"  Would backfill {backfilled} mandates", err=True)
            else:
                result = session.run(backfill_end_query)
                backfilled = result.single()["backfilled"]
                typer.echo(f"  ✓ Backfilled {backfilled} mandates", err=True)
            
            typer.echo("✓ Repair complete!", err=True)
    
    finally:
        driver.close()


def _extract_oldid_from_url(url: Optional[str]) -> Optional[int]:
    if not url:
        return None

    try:
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        oldid = q.get("oldid", [None])[0]
        return int(oldid) if oldid else None
    except (ValueError, TypeError):
        return None


def _extract_title_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    try:
        from urllib.parse import parse_qs, unquote, urlparse

        parsed = urlparse(url)

        q = parse_qs(parsed.query)
        title = q.get("title", [None])[0]
        if title:
            return unquote(title).replace(" ", "_")

        m = re.search(r"/wiki/(?P<title>[^?#]+)", parsed.path)
        if m:
            return unquote(m.group("title")).replace(" ", "_")

        return None
    except Exception:
        return None


@app.command()
def repair_legislature_dates(
    parliament_id: Optional[str] = Option(None, "--parliament-id", help="Filter by parliament_id"),
    limit: int = Option(50, "--limit", help="Max legislatures to process per run"),
    dry_run: bool = Option(False, "--dry-run", help="Show what would be changed without making changes"),
    force_fetch: bool = Option(False, "--force-fetch", help="Force refetch from Wikipedia API (ignore cache)"),
    sleep_ms: int = Option(150, "--sleep-ms", help="Sleep between API calls (politeness)"),
) -> None:
    """Repair Legislature.start_date/end_date by re-parsing the Wikipedia list page (oldid-pinned), then backfill Mandates."""
    import asyncio
    import time
    from uuid import uuid4

    from neo4j import GraphDatabase

    from scraper.cache.mediawiki_cache import fetch_and_cache_parse
    from scraper.parsers.legislature_dates import extract_legislature_dates

    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    run_id = str(uuid4())

    def log(msg: str) -> None:
        typer.echo(msg, err=True)

    def get_fallback_source(session: Any, legislature_id: str) -> tuple[Optional[str], Optional[int], Optional[str]]:
        result = session.run(
            """
            MATCH (l:Legislature {id: $legislature_id})
            OPTIONAL MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l)
            WITH l, collect(m) AS mandates
            WITH l, head([m IN mandates WHERE m IS NOT NULL | m]) AS m
            WITH l, head(coalesce(m.evidence_ids, [])) AS evidence_id
            OPTIONAL MATCH (e:Evidence {id: evidence_id})
            RETURN e.page_title AS page_title, e.revision_id AS revision_id, e.url AS url
            """,
            legislature_id=legislature_id,
        ).single()

        if not result:
            return None, None, None

        return result.get("page_title"), result.get("revision_id"), result.get("url")

    updated = 0
    skipped_no_source = 0
    skipped_no_dates = 0
    errored = 0

    try:
        with driver.session() as session:
            if not dry_run:
                where_src = "WHERE l.source_url IS NULL OR l.wikipedia_title IS NULL"
                if parliament_id:
                    where_src += " AND l.parliament_id = $parliament_id"

                session.run(
                    f"""
                    MATCH (l:Legislature)
                    {where_src}
                    OPTIONAL MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l)
                    WITH l, collect(m) AS mandates
                    WITH l, head([m IN mandates WHERE m IS NOT NULL | m]) AS m
                    WITH l, head(coalesce(m.evidence_ids, [])) AS evidence_id
                    OPTIONAL MATCH (e:Evidence {{id: evidence_id}})
                    SET l.source_url = coalesce(l.source_url, e.url),
                        l.wikipedia_title = coalesce(l.wikipedia_title, e.page_title)
                    """,
                    parliament_id=parliament_id,
                )

                session.run(
                    """
                    MATCH (l:Legislature)
                    WHERE (l.start_date_raw IS NOT NULL AND (l.start_date_raw CONTAINS "<" OR l.start_date_raw CONTAINS ">" OR l.start_date_raw CONTAINS "href" OR l.start_date_raw CONTAINS '"'))
                       OR (l.end_date_raw IS NOT NULL AND (l.end_date_raw CONTAINS "<" OR l.end_date_raw CONTAINS ">" OR l.end_date_raw CONTAINS "href" OR l.end_date_raw CONTAINS '"'))
                    SET l.start_date_raw = CASE
                        WHEN l.start_date_raw IS NOT NULL AND (l.start_date_raw CONTAINS "<" OR l.start_date_raw CONTAINS ">" OR l.start_date_raw CONTAINS "href" OR l.start_date_raw CONTAINS '"')
                        THEN null
                        ELSE l.start_date_raw
                    END,
                    l.end_date_raw = CASE
                        WHEN l.end_date_raw IS NOT NULL AND (l.end_date_raw CONTAINS "<" OR l.end_date_raw CONTAINS ">" OR l.end_date_raw CONTAINS "href" OR l.end_date_raw CONTAINS '"')
                        THEN null
                        ELSE l.end_date_raw
                    END
                    """,
                )

            where = "WHERE (l.start_date IS NULL OR l.end_date IS NULL)"
            if parliament_id:
                where += " AND l.parliament_id = $parliament_id"

            rows = session.run(
                f"""
                MATCH (l:Legislature)
                {where}
                RETURN l.id AS id,
                       l.parliament_id AS parliament_id,
                       l.name AS name,
                       l.source_url AS source_url,
                       l.wikipedia_title AS wikipedia_title
                ORDER BY l.parliament_id, l.id
                LIMIT $limit
                """,
                parliament_id=parliament_id,
                limit=limit,
            ).data()

            log(f"Found {len(rows)} legislature(s) missing start/end dates")

            for row in rows:
                legislature_id = row["id"]
                source_url = row.get("source_url")
                title = row.get("wikipedia_title") or _extract_title_from_url(source_url)
                oldid = _extract_oldid_from_url(source_url)

                if not title or not oldid:
                    fb_title, fb_revision, fb_url = get_fallback_source(session, legislature_id)
                    title = title or fb_title or _extract_title_from_url(fb_url)
                    oldid = oldid or (int(fb_revision) if fb_revision else _extract_oldid_from_url(fb_url))
                    source_url = source_url or fb_url

                if not title or not oldid:
                    skipped_no_source += 1
                    log(f"- [{legislature_id}] skipped: no source_url/evidence oldid")
                    continue

                try:
                    response = asyncio.run(
                        fetch_and_cache_parse(
                            page_title=title,
                            run_id=run_id,
                            force=force_fetch,
                            revalidate=False,
                            revision_id=oldid,
                        )
                    )
                    if not response:
                        raise ValueError("fetch_and_cache_parse returned None")

                    dates = extract_legislature_dates(response)
                    if not dates.start_date and not dates.end_date and not dates.start_date_raw and not dates.end_date_raw:
                        skipped_no_dates += 1
                        log(f"- [{legislature_id}] skipped: no dates detected on page {title} oldid={oldid}")
                        continue

                    if dry_run:
                        log(
                            f"- [{legislature_id}] would update: start={dates.start_date or None} end={dates.end_date or None} raw_start={dates.start_date_raw or None} raw_end={dates.end_date_raw or None}"
                        )
                    else:
                        session.run(
                            """
                            MATCH (l:Legislature {id: $id})
                            WITH l,
                                 (l.start_date IS NULL) AS missing_start,
                                 (l.end_date IS NULL) AS missing_end
                            SET l.wikipedia_title = coalesce(l.wikipedia_title, $title),
                                l.source_url = coalesce(l.source_url, $source_url),
                                l.start_date = CASE WHEN missing_start AND $start_date IS NOT NULL THEN $start_date ELSE l.start_date END,
                                l.end_date = CASE WHEN missing_end AND $end_date IS NOT NULL THEN $end_date ELSE l.end_date END,
                                l.start_date_source = CASE WHEN missing_start AND $start_date IS NOT NULL THEN $start_date_source ELSE l.start_date_source END,
                                l.end_date_source = CASE WHEN missing_end AND $end_date IS NOT NULL THEN $end_date_source ELSE l.end_date_source END,
                                l.start_date_raw = CASE WHEN missing_start AND $start_date IS NULL AND $start_date_raw IS NOT NULL THEN $start_date_raw ELSE l.start_date_raw END,
                                l.end_date_raw = CASE WHEN missing_end AND $end_date IS NULL AND $end_date_raw IS NOT NULL THEN $end_date_raw ELSE l.end_date_raw END
                            """,
                            id=legislature_id,
                            title=title,
                            source_url=source_url,
                            start_date=dates.start_date,
                            end_date=dates.end_date,
                            start_date_raw=dates.start_date_raw,
                            end_date_raw=dates.end_date_raw,
                            start_date_source="wikipedia_list" if dates.start_date else None,
                            end_date_source="wikipedia_list" if dates.end_date else None,
                        )
                        updated += 1
                        log(
                            f"✓ [{legislature_id}] updated from {title} oldid={oldid} "
                            f"(start={dates.start_date or dates.start_date_raw or 'null'}, end={dates.end_date or dates.end_date_raw or 'null'})"
                        )

                    if sleep_ms > 0:
                        time.sleep(sleep_ms / 1000.0)
                except Exception as e:
                    errored += 1
                    log(f"! [{legislature_id}] error: {e}")

            log(f"Repair summary: updated={updated}, skipped_no_source={skipped_no_source}, skipped_no_dates={skipped_no_dates}, errored={errored}")

            log("Backfilling Mandate dates from Legislature...")

            if dry_run:
                result = session.run(
                    """
                    MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                    WHERE m.start_date IS NULL AND l.start_date IS NOT NULL
                    RETURN count(m) AS backfilled
                    """
                )
                log(f"- would backfill start_date for {result.single()['backfilled']} mandates")
            else:
                result = session.run(
                    """
                    MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                    WHERE m.start_date IS NULL AND l.start_date IS NOT NULL
                    SET m.start_date = l.start_date,
                        m.start_date_source = "legislature"
                    RETURN count(m) AS backfilled
                    """
                )
                log(f"✓ backfilled start_date for {result.single()['backfilled']} mandates")

            if dry_run:
                result = session.run(
                    """
                    MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                    WHERE m.end_date IS NULL AND l.end_date IS NOT NULL
                    RETURN count(m) AS backfilled
                    """
                )
                log(f"- would backfill end_date for {result.single()['backfilled']} mandates")
            else:
                result = session.run(
                    """
                    MATCH (m:Mandate)-[:IN_LEGISLATURE]->(l:Legislature)
                    WHERE m.end_date IS NULL AND l.end_date IS NOT NULL
                    SET m.end_date = l.end_date,
                        m.end_date_source = "legislature"
                    RETURN count(m) AS backfilled
                    """
                )
                log(f"✓ backfilled end_date for {result.single()['backfilled']} mandates")
    finally:
        driver.close()


@app.command()
def api(
    host: str = Option("0.0.0.0", "--host", help="Host to bind"),
    port: int = Option(8000, "--port", help="Port to bind"),
    reload: bool = Option(False, "--reload", help="Enable auto-reload"),
) -> None:
    """Start FastAPI tool gateway server."""
    import uvicorn
    
    from scraper.api.app import create_app
    
    api_app = create_app()
    uvicorn.run(api_app, host=host, port=port, reload=reload)


if __name__ == "__main__":
    app()

