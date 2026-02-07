from __future__ import annotations

import json
from typing import Any

import meilisearch
import typer

from pis.models import Person
from pis.settings import PisSettings


app = typer.Typer(add_completion=False, help="PIS – Politisches Informations System (CLI)")


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

