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
def seed() -> None:
    """Validate seed configuration."""
    from scraper.cache.mediawiki_cache import validate_seeds

    try:
        validate_seeds()
        typer.echo("✓ Seeds validation passed", err=True)
        sys.exit(0)
    except Exception as e:
        typer.echo(f"✗ Seeds validation failed: {e}", err=True)
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
def pipeline(
    seed: Optional[str] = Option(None, "--seed", help="Seed key (if not provided, runs all)"),
    write_neo4j: bool = Option(False, "--write-neo4j", help="Write to Neo4j"),
    write_meili: bool = Option(False, "--write-meili", help="Write to Meilisearch"),
    force: bool = Option(False, "--force", help="Force refetch"),
    revalidate: bool = Option(False, "--revalidate", help="Revalidate revisions"),
) -> None:
    """Run the complete pipeline."""
    runner = PipelineRunner(settings)

    try:
        if seed:
            success = runner.run_single(
                seed_key=seed,
                write_neo4j=write_neo4j,
                write_meili=write_meili,
                force=force,
                revalidate=revalidate,
            )
        else:
            success = runner.run_all(
                write_neo4j=write_neo4j,
                write_meili=write_meili,
                force=force,
                revalidate=revalidate,
            )
        sys.exit(0 if success else 1)
    except Exception as e:
        typer.echo(f"✗ Pipeline failed: {e}", err=True)
        sys.exit(1)


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

