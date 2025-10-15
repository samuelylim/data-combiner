"""
File parsing utilities for various data formats.

This module provides functions to detect and parse CSV, TSV, XLSX, and HTML table files.
"""

import io
import csv
from typing import Dict, Any, List, Union
import pandas as pd
from bs4 import BeautifulSoup


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


def parse_html_table(
    content: Union[str, bytes],
    config: Dict[str, Any]
) -> List[Union[Dict[str, Any], List[Any]]]:
    """
    Parse HTML table content.
    
    Args:
        content: The HTML content as string or bytes
        config: Configuration with html_table settings (selector, has_header, etc.)
        
    Returns:
        List of rows - each row is either a dict (with headers) or list (without headers)
        
    Raises:
        ValueError: If table cannot be found or parsed
    """
    # Decode if bytes
    if isinstance(content, bytes):
        html_content = content.decode('utf-8')
    else:
        html_content = content
    
    # Get HTML table configuration
    html_config = config.get("html_table", {})
    selector = html_config.get("selector", "table")
    table_index = html_config.get("table_index", 0)
    has_header = html_config.get("has_header", True)
    skip_rows = html_config.get("skip_rows", 0)
    row_selector = html_config.get("row_selector")
    cell_selector = html_config.get("cell_selector")
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find table
    tables = soup.select(selector)
    if not tables:
        raise ValueError(f"No table found with selector '{selector}'")
    if table_index >= len(tables):
        raise ValueError(f"Table index {table_index} out of range (found {len(tables)} tables)")
    
    table = tables[table_index]
    
    # Extract rows
    if row_selector:
        rows = table.select(row_selector)
    else:
        # Default: get rows from tbody if it exists, otherwise from table
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
        else:
            # Get all tr elements, but exclude those in thead
            thead = table.find('thead')
            if thead:
                # Get all tr in table, then filter out thead rows
                all_rows = table.find_all('tr')
                thead_rows = thead.find_all('tr')
                rows = [row for row in all_rows if row not in thead_rows]
            else:
                rows = table.find_all('tr')
    
    if not rows:
        return []
    
    # Determine cell selector
    if not cell_selector:
        cell_selector = 'td'
    
    # Extract header if present
    headers = None
    data_start_index = 0
    
    if has_header:
        # Try to find header in thead first
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                header_cells = header_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True) for cell in header_cells]
                # Data rows start from first row in rows list (tbody)
                data_start_index = 0
        
        # If no thead, assume first row is header
        if headers is None and rows:
            header_cells = rows[0].find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) for cell in header_cells]
            data_start_index = 1
    
    # Extract data rows
    data_rows = rows[data_start_index + skip_rows:]
    result = []
    
    for row in data_rows:
        cells = row.select(cell_selector) if cell_selector != 'td' else row.find_all('td')
        if not cells:
            # Try 'th' if no 'td' found
            cells = row.find_all('th')
        
        # Skip rows with colspan cells (number of cells != number of columns)
        # This is common for pagination links or summary rows
        if has_header and headers and len(cells) != len(headers):
            continue
        
        cell_values = [cell.get_text(strip=True) for cell in cells]
        
        if has_header and headers:
            # Return as dict
            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(cell_values):
                    row_dict[header] = cell_values[i]
                else:
                    row_dict[header] = None
            result.append(row_dict)
        else:
            # Return as list
            result.append(cell_values)
    
    return result
