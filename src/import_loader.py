"""
Import loader for bulk data downloads.

This module handles downloading and processing bulk data files (CSV, TSV, XLSX)
according to import configurations defined in sources/imports/.
"""

import io
import csv
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import pandas as pd
from http_client import make_request
from transformers import get_transformer_class


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


def _apply_transformation(value: Any, transform_spec: Dict[str, Any]) -> Any:
    """
    Apply a transformation to a value.
    
    Args:
        value: The value to transform
        transform_spec: Dictionary with 'type' and transformation parameters
        
    Returns:
        The transformed value
        
    Raises:
        ValueError: If transformer type is not recognized
        KeyError: If required parameters are missing
        TypeError: If parameters have wrong type
    """
    transformer_type = transform_spec.get("type")
    if not transformer_type:
        raise KeyError("Transform specification must include 'type' field")
    
    transformer_class = get_transformer_class(transformer_type)
    transformer = transformer_class.from_dict(transform_spec)
    return transformer.transform(value)


def _process_column_value(
    row_data: Union[Dict[str, Any], List[Any]],
    column_def: Union[str, Dict[str, Any]],
    db_column: str,
    has_header: bool
) -> Any:
    """
    Extract and optionally transform a value from row data.
    
    Args:
        row_data: Either a dict (if file has headers) or list (if no headers)
        column_def: Either a string (column name/index) or dict with 'column' and optional 'transform'
        db_column: The database column name (for error messages)
        has_header: Whether the file has a header row
        
    Returns:
        The processed value (possibly transformed)
        
    Raises:
        KeyError: If column not found
        ValueError: If transformation fails
    """
    # Determine the source column and optional transform
    if isinstance(column_def, str):
        source_column = column_def
        transform_spec = None
    elif isinstance(column_def, dict):
        source_column = column_def.get("column")
        if source_column is None:
            raise KeyError(f"Column definition for '{db_column}' must include 'column' field")
        transform_spec = column_def.get("transform")
    else:
        raise TypeError(f"Invalid column definition type for '{db_column}': {type(column_def)}")
    
    # Extract the value
    if isinstance(row_data, dict):
        # File has headers - use column name
        if source_column not in row_data:
            raise KeyError(f"Column '{source_column}' not found in row data")
        value = row_data[source_column]
    else:
        # File has no headers - use index
        try:
            index = int(source_column) if isinstance(source_column, str) else source_column
            value = row_data[index]
        except (ValueError, IndexError, TypeError) as e:
            raise KeyError(f"Cannot access column '{source_column}' in row data: {e}")
    
    # Apply transformation if specified
    if transform_spec:
        value = _apply_transformation(value, transform_spec)
    
    return value


def _detect_file_format(content: bytes, config: Dict[str, Any]) -> str:
    """
    Detect the file format from content and configuration.
    
    Args:
        content: The file content as bytes
        config: The import configuration
        
    Returns:
        File format: 'csv', 'tsv', or 'xlsx'
    """
    # Check if XLSX is configured
    if "sheet" in config:
        return "xlsx"
    
    # Check separator to distinguish CSV from TSV
    separator = config.get("separator", ",")
    if separator == "\t":
        return "tsv"
    else:
        return "csv"


def _parse_csv_tsv(
    content: bytes,
    config: Dict[str, Any]
) -> List[Union[Dict[str, Any], List[Any]]]:
    """
    Parse CSV or TSV file content.
    
    Args:
        content: The file content as bytes
        config: Import configuration with separator, encoding, has_header, etc.
        
    Returns:
        List of rows - each row is either a dict (with headers) or list (without headers)
    """
    encoding = config.get("file_encoding", "utf-8")
    separator = config.get("separator", ",")
    has_header = config.get("has_header", False)
    null_values = config.get("null_values", [])
    
    # Decode content to string
    text_content = content.decode(encoding)
    
    # Parse CSV
    reader = csv.reader(io.StringIO(text_content), delimiter=separator)
    rows = list(reader)
    
    if not rows:
        return []
    
    # Process based on header presence
    if has_header:
        headers = rows[0]
        data_rows = rows[1:]
        # Convert to list of dicts
        result = []
        for row in data_rows:
            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(row):
                    value = row[i]
                    # Handle null values
                    if value in null_values:
                        value = None
                    row_dict[header] = value
            result.append(row_dict)
        return result
    else:
        # Return as list of lists, handling null values
        result = []
        for row in rows:
            processed_row = [None if val in null_values else val for val in row]
            result.append(processed_row)
        return result


def _parse_xlsx(
    content: bytes,
    config: Dict[str, Any]
) -> List[Union[Dict[str, Any], List[Any]]]:
    """
    Parse XLSX file content.
    
    Args:
        content: The file content as bytes
        config: Import configuration with sheet, has_header, etc.
        
    Returns:
        List of rows - each row is either a dict (with headers) or list (without headers)
    """
    sheet_name = config.get("sheet", 0)  # Default to first sheet
    has_header = config.get("has_header", False)
    null_values = config.get("null_values", [])
    
    # Read Excel file
    df = pd.read_excel(
        io.BytesIO(content),
        sheet_name=sheet_name,
        header=0 if has_header else None,
        na_values=null_values,
        keep_default_na=False  # Only use our null_values
    )
    
    # Convert to list of dicts or lists
    if has_header:
        # Convert to list of dicts
        return df.to_dict('records')
    else:
        # Convert to list of lists
        return df.values.tolist()


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
    file_format = _detect_file_format(content, config)
    print(f"Detected format: {file_format}")
    
    # Parse file based on format
    if file_format == "xlsx":
        rows = _parse_xlsx(content, config)
    else:
        rows = _parse_csv_tsv(content, config)
    
    print(f"Parsed {len(rows)} rows")
    
    # Process each row
    records_processed = 0
    for row_index, row_data in enumerate(rows):
        try:
            # Build record by mapping columns
            record = {}
            for db_column, column_def in column_map.items():
                value = _process_column_value(row_data, column_def, db_column, has_header)
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
