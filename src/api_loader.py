"""
API loader for live data sources.

This module handles loading and processing data from JSON and HTML table APIs
according to API configurations defined in sources/apis/.

Supports:
- JSON APIs with nested record extraction
- HTML table scraping
- Multiple pagination strategies (next_page_url, page_num, offset)
- Rate limiting (per-endpoint and shared)
- Column mapping and transformations
"""

import json
from typing import Dict, Any, List, Union, Optional
from pathlib import Path
from http_client import make_request
from utils.schema_utils import validate_config
from utils.file_utils import parse_html_table
from utils.data_utils import process_column_value, get_nested_value
from utils.rate_limiter import RateLimiter


async def output_record(record: Dict[str, Any], source_name: str) -> None:
    """
    Output a single record to the destination.
    
    This is a temporary implementation that prints records.
    In the future, this will be replaced with SQL INSERT/UPDATE queries.
    
    Args:
        record: Dictionary mapping database column names to values
        source_name: Name of the API source for logging
    """
    print(f"[{source_name}] Record: {record}")


def extract_records_from_json(response: Dict[str, Any], records_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Extract records array from JSON response.
    
    Args:
        response: The JSON response dictionary
        records_path: Optional dot-notation path to records array (e.g., "data.items")
        
    Returns:
        List of record dictionaries
        
    Raises:
        ValueError: If records cannot be extracted
    """
    if records_path:
        # Extract from nested path
        try:
            records = get_nested_value(response, records_path)
        except KeyError as e:
            raise ValueError(f"Cannot extract records from path '{records_path}': {e}")
    else:
        # Assume response is the records array
        records = response
    
    # Ensure we have a list
    if not isinstance(records, list):
        raise ValueError(f"Expected records to be a list, got {type(records).__name__}")
    
    return records


def build_pagination_params(
    config: Optional[Dict[str, Any]],
    current_page: int,
    records_fetched: int
) -> Optional[Dict[str, Any]]:
    """
    Build query parameters for pagination.
    
    Args:
        config: Pagination configuration
        current_page: Current page number (1-indexed)
        records_fetched: Number of records fetched so far
        
    Returns:
        Dictionary of query parameters, or None if no pagination
    """
    if not config:
        return None
    
    params = {}
    
    # Page number pagination
    if "page_num_param" in config:
        params[config["page_num_param"]] = current_page
    
    # Offset-based pagination
    if "skip_records_param" in config:
        # Some APIs start at 1 instead of 0, handle with initial_offset
        initial_offset = config.get("initial_offset", 0)
        params[config["skip_records_param"]] = initial_offset + records_fetched
    
    return params if params else None


def should_continue_pagination(
    config: Optional[Dict[str, Any]],
    response: Union[Dict[str, Any], str],
    records_fetched: int,
    current_batch_size: int
) -> tuple[bool, Optional[str]]:
    """
    Determine if pagination should continue.
    
    Args:
        config: Pagination configuration
        response: The API response (JSON dict or HTML string)
        records_fetched: Total records fetched so far
        current_batch_size: Number of records in current batch
        
    Returns:
        Tuple of (should_continue, next_page_url)
    """
    if not config:
        return False, None
    
    # Check total_records_key (JSON only)
    if isinstance(response, dict) and "total_records_key" in config:
        try:
            total_records = get_nested_value(response, config["total_records_key"])
            if records_fetched >= total_records:
                return False, None
        except KeyError:
            # If total_records_key not found, continue with other checks
            pass
    
    # Check next_page_url (JSON only)
    if isinstance(response, dict) and "next_page_url" in config:
        try:
            next_url = get_nested_value(response, config["next_page_url"])
            if next_url:
                return True, next_url
            else:
                return False, None
        except KeyError:
            # No next page URL found
            return False, None
    
    # Check batch_size - if we got fewer records than batch_size, we're done
    if "batch_size" in config:
        batch_size = config["batch_size"]
        if current_batch_size < batch_size:
            return False, None
        else:
            return True, None
    
    # No pagination indicators found
    return False, None


async def load_api(config: Dict[str, Any], source_name: str) -> None:
    """
    Load data from an API configuration.
    
    Fetches data from API (JSON or HTML), handles pagination, applies rate limiting,
    maps columns, applies transformations, and outputs records.
    
    Args:
        config: The API configuration dictionary (validated against API schema)
        source_name: Name of the API source for logging
        
    Raises:
        KeyError: If required fields are missing or columns not found
        ValueError: If transformations or parsing fail
        aiohttp.ClientError: If requests fail
    """
    print(f"\n=== Processing API: {source_name} ===")
    
    # Extract configuration
    endpoint = config["endpoint"]
    headers = config.get("headers")
    body = config.get("body")
    response_type = config.get("response_type", "json")
    column_map = config["column_map"]
    pagination_config = config.get("pagination")
    rate_limit_config = config["rate_limit"]
    records_path = config.get("records_path")
    
    # HTML-specific config
    html_table_config = config.get("html_table") if response_type == "html" else None
    
    # Set up rate limiter
    requests_per_minute = rate_limit_config.get("requests_per_minute", 60)
    shared_limit_key = rate_limit_config.get("shared_limit_key")
    rate_limiter = RateLimiter.get_limiter(requests_per_minute, shared_limit_key)
    
    print(f"Response type: {response_type}")
    print(f"Rate limit: {requests_per_minute} requests/minute" + 
          (f" (shared: {shared_limit_key})" if shared_limit_key else ""))
    
    # Pagination state
    current_page = 1
    records_fetched = 0
    total_records_processed = 0
    
    while True:
        # Build pagination params
        params = build_pagination_params(pagination_config, current_page, records_fetched)
        
        # Enforce rate limit
        await rate_limiter.acquire()
        
        # Make request
        print(f"\nFetching page {current_page}..." if pagination_config else "\nFetching data...")
        try:
            response = await make_request(
                endpoint=endpoint,
                headers=headers,
                body=body,
                params=params,
                method=config.get("method", "GET")
            )
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise
        
        # Parse response based on type
        if response_type == "json":
            # Extract records from JSON
            if isinstance(response, list):
                # Response is already a list of records
                records = response
            elif isinstance(response, dict):
                # Extract from nested path or use dict response
                try:
                    records = extract_records_from_json(response, records_path)
                except ValueError as e:
                    print(f"Error extracting records: {e}")
                    raise
            else:
                raise ValueError(f"Expected JSON response (dict or list), got {type(response).__name__}")
            
            print(f"Extracted {len(records)} records")
            has_header = True  # JSON always uses key-based access
            
        else:  # HTML
            # Parse HTML table
            if not isinstance(response, str):
                raise ValueError(f"Expected HTML response (str), got {type(response).__name__}")
            
            try:
                # Build config for HTML parsing
                parse_config = {
                    "html_table": html_table_config
                }
                records = parse_html_table(response, parse_config)
            except Exception as e:
                print(f"Error parsing HTML table: {e}")
                raise
            
            print(f"Extracted {len(records)} rows from HTML table")
            has_header = html_table_config.get("has_header", True) if html_table_config else True
        
        # Process each record
        records_processed = 0
        for record_index, record_data in enumerate(records):
            try:
                # Build record by mapping columns
                record = {}
                for db_column, column_def in column_map.items():
                    value = process_column_value(record_data, column_def, db_column, has_header)
                    record[db_column] = value
                
                # Output the record
                await output_record(record, source_name)
                records_processed += 1
                
            except Exception as e:
                print(f"Error processing record {record_index + 1}: {e}")
                # Continue processing other records
                continue
        
        print(f"Successfully processed {records_processed} records from this batch")
        total_records_processed += records_processed
        records_fetched += len(records)
        
        # Check if we should continue pagination
        should_continue, next_url = should_continue_pagination(
            pagination_config,
            response,
            records_fetched,
            len(records)
        )
        
        if not should_continue:
            print(f"Pagination complete")
            break
        
        # Update endpoint if next_page_url provided
        if next_url:
            endpoint = next_url
            print(f"Next page URL: {next_url}")
        
        current_page += 1
    
    print(f"\nTotal records processed from API '{source_name}': {total_records_processed}")


async def load_all_apis(sources: Dict[str, list]) -> None:
    """
    Load data from all API configurations.
    
    Args:
        sources: Dictionary with 'apis' key containing list of API configs
    """
    api_configs = sources.get("apis", [])
    
    if not api_configs:
        print("No API configurations found")
        return
    
    print(f"\n{'='*60}")
    print(f"Loading {len(api_configs)} API source(s)")
    print(f"{'='*60}")
    
    for api_config in api_configs:
        source_name = api_config.get("_source_file", "unknown")
        try:
            await load_api(api_config, source_name)
        except Exception as e:
            print(f"Error loading API '{source_name}': {e}")
            import traceback
            traceback.print_exc()
            # Continue with other APIs
            continue
    
    print(f"\n{'='*60}")
    print("API loading complete")
    print(f"{'='*60}")


def load_api_sources(sources_dir: Path) -> list:
    """
    Load API configuration files from the apis directory.
    
    Args:
        sources_dir: Path to the sources directory
        
    Returns:
        List of validated API configurations
    """
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
