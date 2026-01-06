import json
from pathlib import Path

import jsonschema
from jsonschema import Draft7Validator, RefResolver


def load_schema(schema_path: str) -> dict:
    """Load JSON schema from file."""
    path = Path(__file__).parent.parent.parent.parent / schema_path
    with open(path, "r") as f:
        return json.load(f)


def assert_json_matches_schema(json_data: dict, schema_path: str) -> None:
    """
    Assert that JSON data matches the schema.
    
    Raises jsonschema.ValidationError if validation fails.
    """
    schema = load_schema(schema_path)
    
    base_path = Path(__file__).parent.parent.parent.parent / "contracts" / "tools"
    resolver = RefResolver(
        base_uri=f"file://{base_path}/",
        referrer=schema,
    )
    
    validator = Draft7Validator(schema, resolver=resolver)
    validator.validate(json_data)


def validate_response(response_data: dict, tool_name: str) -> None:
    """Validate response against tool-specific schema."""
    schema_map = {
        "mandates.search": "contracts/tools/mandates.search.response.schema.json",
        "legislature.stats": "contracts/tools/legislature.stats.response.schema.json",
        "person.lookup": "contracts/tools/person.lookup.response.schema.json",
    }
    
    schema_path = schema_map.get(tool_name)
    if not schema_path:
        raise ValueError(f"Unknown tool: {tool_name}")
    
    assert_json_matches_schema(response_data, schema_path)

