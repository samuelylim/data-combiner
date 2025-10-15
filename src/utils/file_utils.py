"""
File parsing utilities for various data formats.

This module provides functions to detect and parse CSV, TSV, and XLSX files.
"""

import io
import csv
from typing import Dict, Any, List, Union
import pandas as pd


def detect_file_format(content: bytes, config: Dict[str, Any]) -> str:
    """
    Detect the file format from content and configuration.
    
    Args:
        content: The file content as bytes
        config: Configuration dict that may contain 'sheet' or 'separator'
        
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


def parse_csv_tsv(
    content: bytes,
    config: Dict[str, Any]
) -> List[Union[Dict[str, Any], List[Any]]]:
    """
    Parse CSV or TSV file content.
    
    Args:
        content: The file content as bytes
        config: Configuration with separator, encoding, has_header, null_values, etc.
        
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


def parse_xlsx(
    content: bytes,
    config: Dict[str, Any]
) -> List[Union[Dict[str, Any], List[Any]]]:
    """
    Parse XLSX file content.
    
    Args:
        content: The file content as bytes
        config: Configuration with sheet, has_header, null_values, etc.
        
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
