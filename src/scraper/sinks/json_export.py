import json
from pathlib import Path
from typing import Any, Dict


def export_json(data: Dict[str, Any], output_dir: Path, run_id: str | None = None) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    skip_keys = {"exported_at"}
    for entity_type, entities in data.items():
        if entity_type in skip_keys or not entities:
            continue

        output_file = output_dir / f"{entity_type}.json"
        serialized = [entity.model_dump() if hasattr(entity, "model_dump") else entity for entity in entities]
        output_file.write_text(
            json.dumps(serialized, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )

    manifest_file = output_dir / "manifest.json"
    manifest = {
        "exported_at": data.get("exported_at"),
        "entity_counts": {k: len(v) for k, v in data.items() if isinstance(v, list)},
    }
    if run_id:
        manifest["run_id"] = run_id
    manifest_file.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

