from typing import Dict, Any
import json
from pathlib import Path
from jsonschema import validate, ValidationError, SchemaError
import sys

# Cache for loaded schemas
_SCHEMA_CACHE = {}

def load_schema(schema_path: Path) -> Dict[str, Any]:
    schema_key = str(schema_path)
    if schema_key in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[schema_key]
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
        _SCHEMA_CACHE[schema_key] = schema
        return schema

def validate_config(config: Dict[str, Any], schema_path: Path, source_name: str) -> None:
    try:
        schema = load_schema(schema_path)
        validate(instance=config, schema=schema)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except SchemaError as e:
        print(f"Error: Invalid schema at '{schema_path}': {e.message}")
        sys.exit(1)
    except ValidationError as e:
        print(f"Error: Validation failed for {source_name}")
        print(f"  Path: {' -> '.join(str(p) for p in e.path) if e.path else 'root'}")
        print(f"  Message: {e.message}")
        sys.exit(1)
