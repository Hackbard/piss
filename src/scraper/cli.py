import sys
from pathlib import Path
from typing import Optional
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
        
        # Prefer evidence_refs (new approach), fallback to evidence_ids (legacy)
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
            
            # Fallback: legacy evidence_ids (if no evidence_refs found for this hit)
            if not hit_evidence_refs or not isinstance(hit_evidence_refs, list) or not hit_evidence_refs:
                hit_evidence_ids = hit.get("evidence_ids", [])
                if isinstance(hit_evidence_ids, list):
                    evidence_ids.extend(hit_evidence_ids)
        
        # Deduplicate evidence_ids (legacy fallback)
        if evidence_ids:
            evidence_ids = list(set(evidence_ids))
            typer.echo(f"Found {len(evidence_ids)} unique evidence IDs from Meilisearch (legacy)", err=True)
        
        if evidence_refs:
            typer.echo(f"Found {len(evidence_refs)} evidence references from Meilisearch (preferred)", err=True)
        
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


if __name__ == "__main__":
    app()

