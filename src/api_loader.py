import json
from pathlib import Path
from utils.schema_utils import validate_config

def load_api_sources(sources_dir: Path) -> list:
    apis = []
    apis_dir = sources_dir / "apis"
    api_schema_path = apis_dir / "schema.json"
    if apis_dir.exists():
        for file_path in apis_dir.glob("*.json"):
            if file_path.name == "schema.json":
                continue
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    api_config = json.load(f)
                    validate_config(api_config, api_schema_path, file_path.name)
                    api_config['_source_file'] = file_path.name
                    apis.append(api_config)
            except Exception as e:
                print(f"Error loading API config {file_path.name}: {e}")
                continue
    return apis
