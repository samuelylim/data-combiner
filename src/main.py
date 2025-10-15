from typing import Union, Dict, Any
import json
import sys
import asyncio
from pathlib import Path
from transformers import get_transformer_class
from jsonschema import validate, ValidationError, SchemaError
from http_client import make_request, close_session
from import_loader import load_all_imports


# Cache for loaded schemas
_SCHEMA_CACHE = {}


def load_schema(schema_name: str) -> Dict[str, Any]:
    """
    Load and cache a JSON schema from the schemas directory.
    
    Args:
        schema_name: Name of the schema file (without .json extension)
                    e.g., 'api', 'dataset', or 'import'
    
    Returns:
        Dictionary containing the JSON schema
        
    Raises:
        FileNotFoundError: If schema file doesn't exist
        json.JSONDecodeError: If schema file contains invalid JSON
    """
    if schema_name in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[schema_name]
    
    # Get the project root directory (parent of src/)
    project_root = Path(__file__).parent.parent
    schema_path = project_root / "schemas" / f"{schema_name}.json"
    
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
        _SCHEMA_CACHE[schema_name] = schema
        return schema


def validate_config(config: Dict[str, Any], schema_name: str, source_name: str) -> None:
    """
    Validate a configuration against its JSON schema.
    
    Args:
        config: The configuration dictionary to validate
        schema_name: Name of the schema to validate against ('api', 'dataset', or 'import')
        source_name: Name of the source file/folder for error reporting
        
    Raises:
        SystemExit: If validation fails
    """
    try:
        schema = load_schema(schema_name)
        validate(instance=config, schema=schema)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except SchemaError as e:
        print(f"Error: Invalid schema '{schema_name}.json': {e.message}")
        sys.exit(1)
    except ValidationError as e:
        print(f"Error: Validation failed for {source_name}")
        print(f"  Path: {' -> '.join(str(p) for p in e.path) if e.path else 'root'}")
        print(f"  Message: {e.message}")
        sys.exit(1)

def load_sources() -> Dict[str, list]:
    """
    Load data source configurations from the sources directory.
    
    Loads configuration files according to the data-combiner structure:
    - APIs: JSON files from sources/apis/
    - Datasets: Folders with structure.json from sources/datasets/
    - Imports: JSON files from sources/imports/
    
    Returns:
        Dictionary with keys 'apis', 'datasets', and 'imports', each containing
        a list of loaded configurations.
    """
    sources = {
        "apis": [],
        "datasets": [],
        "imports": []
    }
    
    # Get the project root directory (parent of src/)
    project_root = Path(__file__).parent.parent
    sources_dir = project_root / "sources"
    
    # Load API definitions from sources/apis/
    apis_dir = sources_dir / "apis"
    if apis_dir.exists():
        for file_path in apis_dir.glob("*.json"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    api_config = json.load(f)
                    # Validate against API schema
                    validate_config(api_config, 'api', file_path.name)
                    api_config['_source_file'] = file_path.name
                    sources["apis"].append(api_config)
                    print(f"Loaded API config: {file_path.name}")
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON in {file_path.name}: {e}")
                sys.exit(1)
            except Exception as e:
                print(f"Error loading {file_path.name}: {e}")
                sys.exit(1)
    
    # Load dataset definitions from sources/datasets/
    datasets_dir = sources_dir / "datasets"
    if datasets_dir.exists():
        for dataset_folder in datasets_dir.iterdir():
            if dataset_folder.is_dir():
                structure_file = dataset_folder / "structure.json"
                if structure_file.exists():
                    try:
                        with open(structure_file, 'r', encoding='utf-8') as f:
                            dataset_config = json.load(f)
                            # Validate against dataset schema
                            validate_config(dataset_config, 'dataset', f"{dataset_folder.name}/structure.json")
                            dataset_config['_source_folder'] = dataset_folder.name
                            dataset_config['_folder_path'] = str(dataset_folder)
                            sources["datasets"].append(dataset_config)
                            print(f"Loaded dataset config: {dataset_folder.name}")
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in {structure_file}: {e}")
                        sys.exit(1)
                    except Exception as e:
                        print(f"Error loading {structure_file}: {e}")
                        sys.exit(1)
                else:
                    print(f"Warning: Dataset folder '{dataset_folder.name}' missing structure.json")
                    sys.exit(1)
    
    # Load import definitions from sources/imports/
    imports_dir = sources_dir / "imports"
    if imports_dir.exists():
        for file_path in imports_dir.glob("*.json"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    import_config = json.load(f)
                    # Validate against import schema
                    validate_config(import_config, 'import', file_path.name)
                    import_config['_source_file'] = file_path.name
                    sources["imports"].append(import_config)
                    print(f"Loaded import config: {file_path.name}")
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON in {file_path.name}: {e}")
                sys.exit(1)
            except Exception as e:
                print(f"Error loading {file_path.name}: {e}")
                sys.exit(1)
    
    return sources


if __name__ == "__main__":
    # Load sources from disk
    sources = load_sources()
    
    # Print summary
    print(f"\n=== Sources Loaded ===")
    print(f"APIs: {len(sources['apis'])}")
    print(f"Datasets: {len(sources['datasets'])}")
    print(f"Imports: {len(sources['imports'])}")
    print(f"Total: {sum(len(v) for v in sources.values())}")
    
    # Process imports
    async def main():
        try:
            await load_all_imports(sources)
        finally:
            # Clean up HTTP session
            await close_session()
    
    # Run async main
    asyncio.run(main())

