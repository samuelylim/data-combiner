"""
Import loader for bulk data downloads.

This module handles downloading and processing bulk data files (CSV, TSV, XLSX)
according to import configurations defined in sources/imports/.
"""

import json
from typing import Dict, Any
from pathlib import Path
from utils.http_client import make_request
from utils.schema_utils import validate_config
from utils.file_utils import detect_file_format, parse_csv_tsv, parse_xlsx
from utils.data_utils import process_column_value


async def output_record(record: Dict[str, Any], source_name: str) -> None:
    """
    Output a single record to the destination.
    
    This is a temporary implementation that prints records.
    In the future, this will be replaced with SQL INSERT/UPDATE queries.
    
    Args:
        record: Dictionary mapping database column names to values
        source_name: Name of the import source for logging
    """
    print(f"[{source_name}] Record: {record}")


async def load_import(config: Dict[str, Any], source_name: str) -> None:
    """
    Load data from an import configuration.
    
    Downloads a file, parses it, maps columns, applies transformations,
    and outputs records.
    
    Args:
        config: The import configuration dictionary (validated against import.json schema)
        source_name: Name of the import source for logging
        
    Raises:
        KeyError: If required fields are missing or columns not found
        ValueError: If transformations fail
        aiohttp.ClientError: If download fails
    """
    print(f"\n=== Processing Import: {source_name} ===")
    
    # Extract configuration
    endpoint = config["endpoint"]
    headers = config.get("headers")
    column_map = config["column_map"]
    has_header = config.get("has_header", False)
    
    # Download the file
    print(f"Downloading from: {endpoint}")
    content = await make_request(
        endpoint=endpoint,
        headers=headers,
        method="GET"
    )
    
    # Ensure we have bytes
    if isinstance(content, str):
        content = content.encode('utf-8')
    elif isinstance(content, dict):
        raise ValueError("Expected file content, got JSON response")
    
    # Detect file format
    file_format = detect_file_format(content, config)
    print(f"Detected format: {file_format}")
    
    # Parse file based on format
    if file_format == "xlsx":
        rows = parse_xlsx(content, config)
    else:
        rows = parse_csv_tsv(content, config)
    
    print(f"Parsed {len(rows)} rows")
    
    # Process each row
    records_processed = 0
    for row_index, row_data in enumerate(rows):
        try:
            # Build record by mapping columns
            record = {}
            for db_column, column_def in column_map.items():
                value = process_column_value(row_data, column_def, db_column, has_header)
                record[db_column] = value
            
            # Output the record
            await output_record(record, source_name)
            records_processed += 1
            
        except Exception as e:
            print(f"Error processing row {row_index + 1}: {e}")
            # Continue processing other rows
            continue
    
    print(f"Successfully processed {records_processed} records")




async def load_all_imports(sources: Dict[str, list]) -> None:
    """
    Load data from all import configurations.
    
    Args:
        sources: Dictionary with 'imports' key containing list of import configs
    """
    import_configs = sources.get("imports", [])
    
    if not import_configs:
        print("No import configurations found")
        return
    
    print(f"\n{'='*60}")
    print(f"Loading {len(import_configs)} import source(s)")
    print(f"{'='*60}")
    
    for import_config in import_configs:
        source_name = import_config.get("_source_file", "unknown")
        try:
            await load_import(import_config, source_name)
        except Exception as e:
            print(f"Error loading import '{source_name}': {e}")
            # Continue with other imports
            continue
    
    print(f"\n{'='*60}")
    print("Import loading complete")
    print(f"{'='*60}")

def load_import_sources(sources_dir: Path) -> list:
    imports = []
    imports_dir = sources_dir / "imports"
    import_schema_path = imports_dir / "schema.json"
    if imports_dir.exists():
        for file_path in imports_dir.glob("*.json"):
            if file_path.name == "schema.json":
                continue
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    import_config = json.load(f)
                    validate_config(import_config, import_schema_path, file_path.name)
                    import_config['_source_file'] = file_path.name
                    imports.append(import_config)
            except Exception as e:
                print(f"Error loading import config {file_path.name}: {e}")
                continue
    return imports
