from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import meilisearch
import typer

from pis.index.meili import PisMeiliIndexer
from pis.ingest.mediawiki import fetch_intro
from pis.ingest.wikidata import fetch_politicians_de
from pis.io.jsonl import write_jsonl
from pis.models import Person
from pis.normalize.wikidata import person_to_index_doc, wikidata_row_to_person
from pis.settings import PisSettings
from pis.utils.time import utc_now


app = typer.Typer(add_completion=False, help="PIS – Politisches Informations System (CLI)")
poc_app = typer.Typer(add_completion=False, help="Proof-of-concept pipelines (Wikidata/Wikipedia)")
app.add_typer(poc_app, name="poc")


@app.command()
def health() -> None:
    """Check connectivity to Meilisearch (for local dev)."""
    settings = PisSettings()
    client = meilisearch.Client(settings.meili_url, settings.meili_master_key)
    try:
        healthy = bool(client.is_healthy())
        version = client.get_version()
    except Exception as e:  # noqa: BLE001
        raise typer.Exit(code=2) from e
    typer.echo(json.dumps({"meili_url": settings.meili_url, "healthy": healthy, "version": version}, default=str))


@app.command()
def schema(model: str = typer.Argument("person", help="Model name: person")) -> None:
    """Print JSON schema for canonical PIS models."""
    model = model.strip().lower()
    schemas: dict[str, Any] = {
        "person": Person.model_json_schema(),
    }
    if model not in schemas:
        typer.echo(f"Unknown model: {model}. Available: {', '.join(sorted(schemas))}", err=True)
        raise typer.Exit(code=2)
    typer.echo(json.dumps(schemas[model], ensure_ascii=False, indent=2))


@app.command()
def pipeline() -> None:
    """Placeholder for upcoming raw→normalized→canonical→indexed pipeline."""
    typer.echo("Not implemented yet. Next steps will add ingestion + normalization + reconcile + indexing.")
    raise typer.Exit(code=1)


@poc_app.command("wikidata-persons")
def poc_wikidata_persons(
    limit: int = typer.Option(50, help="Page size for Wikidata SPARQL LIMIT"),
    offset: int = typer.Option(0, help="Wikidata SPARQL OFFSET (pagination)"),
    with_wikipedia_intro: bool = typer.Option(True, help="Fetch Wikipedia intro via MediaWiki API when dewiki title exists"),
    write_meili: bool = typer.Option(False, help="Index into Meilisearch (pis_persons)"),
    force: bool = typer.Option(False, help="Ignore HTTP cache and refetch"),
    out_dir: Path | None = typer.Option(None, help="Override output dir (defaults to settings.pis_*_dir)"),
) -> None:
    """End-to-end PoC: Wikidata SPARQL → canonical Person → JSONL snapshots → (optional) Meilisearch."""
    settings = PisSettings()
    settings.ensure_dirs()

    cached, rows = fetch_politicians_de(settings=settings, limit=limit, offset=offset, force=force)

    fetched_at = utc_now()
    raw_url = cached.url
    raw_snapshot_path = str(cached.raw_path)

    base_out = out_dir or settings.pis_data_dir
    raw_dir = (base_out / "raw").resolve()
    norm_dir = (base_out / "normalized").resolve()
    canonical_dir = (base_out / "canonical").resolve()
    for d in (raw_dir, norm_dir, canonical_dir):
        d.mkdir(parents=True, exist_ok=True)

    run_key = f"wikidata_politicians_de_limit{limit}_offset{offset}_{cached.cache_key[:8]}"
    raw_out = raw_dir / f"{run_key}.sparql.json"
    norm_out = norm_dir / f"{run_key}.persons.jsonl"
    canonical_out = canonical_dir / f"{run_key}.persons.jsonl"

    # Store raw query response as a run snapshot too (in addition to cache).
    raw_out.write_text(json.dumps(cached.data, ensure_ascii=False, indent=2), encoding="utf-8")

    persons: list[Person] = []
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        p = wikidata_row_to_person(
            row=row,
            fetched_at=fetched_at,
            raw_snapshot_path=raw_snapshot_path,
            normalized_snapshot_path=str(norm_out),
            source_url=raw_url,
        )
        if with_wikipedia_intro and row.dewiki_title:
            mw_cached, intro = fetch_intro(settings=settings, title=row.dewiki_title, force=force)
            if intro:
                p.external_ids.wikipedia_pageid = intro.pageid
                p.external_ids.wikipedia_title = intro.title
                p.facts["wikipedia_intro"] = intro.extract
                # Attach an additional provenance record for Wikipedia intro.
                from pis.models import PersonSource, SourceSystem

                p.sources.append(
                    PersonSource(
                        source_system=SourceSystem.WIKIPEDIA,
                        source_person_id=str(intro.pageid),
                        fetched_at=fetched_at,
                        source_urls=[intro.url] if intro.url else [],
                        raw_snapshot_path=str(mw_cached.raw_path),
                        normalized_snapshot_path=str(norm_out),
                        extra={"title": intro.title},
                    )
                )

        persons.append(p)
        normalized_rows.append(p.model_dump(mode="json"))

    # Write normalized + canonical snapshots (canonical == persons for this PoC; reconcile comes later).
    write_jsonl(norm_out, normalized_rows)
    write_jsonl(canonical_out, normalized_rows)

    if write_meili:
        indexer = PisMeiliIndexer(settings)
        indexer.init()
        indexer.upsert_persons([person_to_index_doc(p) for p in persons])

    typer.echo(
        json.dumps(
            {
                "count": len(persons),
                "raw_cache_key": cached.cache_key,
                "raw_cache_path": str(cached.raw_path),
                "run_raw_snapshot": str(raw_out),
                "normalized_jsonl": str(norm_out),
                "canonical_jsonl": str(canonical_out),
                "meili_indexed": bool(write_meili),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

