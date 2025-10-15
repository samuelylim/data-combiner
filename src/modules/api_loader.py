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
from utils.http_client import make_request
from utils.schema_utils import validate_config
from utils.file_utils import parse_html_table
from utils.data_utils import process_column_value, get_nested_value
from utils.rate_limiter import RateLimiter
from .base_loader import BaseLoader


class APILoader(BaseLoader):
    """Loader for API data sources."""
    
    LOADER_TYPE = "api"
    
    def __init__(self):
        super().__init__()
        self.rate_limiters: Dict[str, RateLimiter] = {}
    
    @staticmethod
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
    
    @staticmethod
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
            records_fetched: Total number of records fetched so far
            
        Returns:
            Dictionary of query parameters, or None if no pagination
        """
        if not config:
            return None
        
        strategy = config.get("strategy")
        
        if strategy == "page_num":
            # Page number pagination
            page_param = config.get("page_param", "page")
            return {page_param: current_page}
        
        elif strategy == "offset":
            # Offset-based pagination
            limit = config.get("limit", 100)
            offset_param = config.get("offset_param", "offset")
            limit_param = config.get("limit_param", "limit")
            return {
                offset_param: records_fetched,
                limit_param: limit
            }
        
        elif strategy == "next_page_url":
            # URL-based pagination (no query params, URL extracted from response)
            return None
        
        else:
            return None
    
    @staticmethod
    def should_continue_pagination(
        config: Optional[Dict[str, Any]],
        response: Union[Dict, List],
        records_fetched: int,
        current_batch_size: int
    ) -> tuple[bool, Optional[str]]:
        """
        Determine if pagination should continue and extract next page URL if applicable.
        
        Args:
            config: Pagination configuration
            response: The API response
            records_fetched: Total records fetched so far
            current_batch_size: Number of records in current batch
            
        Returns:
            Tuple of (should_continue, next_page_url)
        """
        if not config:
            return False, None
        
        strategy = config.get("strategy")
        
        # Check max_pages limit
        max_pages = config.get("max_pages")
        if max_pages and records_fetched >= max_pages:
            return False, None
        
        # Check if current batch is empty
        if current_batch_size == 0:
            return False, None
        
        if strategy == "next_page_url":
            # Extract next page URL from response
            next_url_path = config.get("next_page_url_path")
            if not next_url_path or not isinstance(response, dict):
                return False, None
            
            try:
                next_url = get_nested_value(response, next_url_path)
                if next_url:
                    return True, next_url
                else:
                    return False, None
            except (KeyError, ValueError):
                return False, None
        
        elif strategy in ["page_num", "offset"]:
            # Check if we got fewer records than expected (last page)
            limit = config.get("limit")
            if limit and current_batch_size < limit:
                return False, None
            return True, None
        
        return False, None
    
    def get_rate_limiter(self, config: Dict[str, Any]) -> RateLimiter:
        """
        Get or create a rate limiter for this API endpoint.
        
        Args:
            config: API configuration with rate_limit settings
            
        Returns:
            RateLimiter instance
        """
        rate_limit_config = config.get("rate_limit", {})
        rate_limiter_name = rate_limit_config.get("shared_name", config.get("endpoint", "default"))
        
        if rate_limiter_name not in self.rate_limiters:
            requests_per_second = rate_limit_config.get("requests_per_second", 10)
            self.rate_limiters[rate_limiter_name] = RateLimiter(requests_per_second)
        
        return self.rate_limiters[rate_limiter_name]
    
    async def load_single(self, config: Dict[str, Any], source_name: str) -> None:
        """
        Load data from a single API configuration.
        
        Fetches data, handles pagination, maps columns, applies transformations,
        and outputs records.
        
        Args:
            config: The API configuration dictionary (validated against api.json schema)
            source_name: Name of the API source for logging
            
        Raises:
            KeyError: If required fields are missing or columns not found
            ValueError: If transformations fail or response format is invalid
            aiohttp.ClientError: If HTTP request fails
        """
        print(f"\n=== Processing API: {source_name} ===")
        
        # Extract configuration
        endpoint = config["endpoint"]
        response_type = config.get("response_type", "json")
        headers = config.get("headers")
        body = config.get("body")
        column_map = config["column_map"]
        pagination_config = config.get("pagination")
        
        # JSON-specific config
        records_path = config.get("records_path")
        
        # HTML-specific config
        html_table_config = config.get("html_table")
        
        # Get rate limiter
        rate_limiter = self.get_rate_limiter(config)
        
        # Pagination loop
        current_page = 1
        records_fetched = 0
        total_records_processed = 0
        
        while True:
            # Build pagination params
            params = self.build_pagination_params(pagination_config, current_page, records_fetched)
            
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
                        records = self.extract_records_from_json(response, records_path)
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
                    await self.output_record(record, source_name)
                    records_processed += 1
                    
                except Exception as e:
                    print(f"Error processing record {record_index + 1}: {e}")
                    # Continue processing other records
                    continue
            
            print(f"Successfully processed {records_processed} records from this batch")
            total_records_processed += records_processed
            records_fetched += len(records)
            
            # Check if we should continue pagination
            should_continue, next_url = self.should_continue_pagination(
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
    
    async def load_all(self, sources: Dict[str, List]) -> None:
        """
        Load data from all API configurations.
        
        Args:
            sources: Dictionary with 'apis' key containing list of API configs
        """
        api_configs = sources.get(self.get_source_key(), [])
        
        if not api_configs:
            print(f"No {self.LOADER_TYPE} configurations found")
            return
        
        print(f"\n{'='*60}")
        print(f"Loading {len(api_configs)} {self.LOADER_TYPE} source(s)")
        print(f"{'='*60}")
        
        for api_config in api_configs:
            source_name = api_config.get("_source_file", "unknown")
            try:
                await self.load_single(api_config, source_name)
            except Exception as e:
                print(f"Error loading {self.LOADER_TYPE} '{source_name}': {e}")
                import traceback
                traceback.print_exc()
                # Continue with other APIs
                continue
        
        print(f"\n{'='*60}")
        print(f"{self.LOADER_TYPE.upper() if self.LOADER_TYPE else 'SOURCE'} loading complete")
        print(f"{'='*60}")
    
    def load_sources(self, sources_dir: Path) -> List[Dict[str, Any]]:
        """
        Load API configuration files from the apis directory.
        
        Args:
            sources_dir: Path to the sources directory
            
        Returns:
            List of validated API configurations
        """
        apis = []
        apis_dir = sources_dir / self.get_source_key()
        api_schema_path = apis_dir / "schema.json"
        
        if not apis_dir.exists():
            return apis
        
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
                print(f"Error loading {self.LOADER_TYPE} config {file_path.name}: {e}")
                continue
        
        return apis


# Create a singleton instance
api_loader = APILoader()

# Export convenience functions for backward compatibility
async def load_api(config: Dict[str, Any], source_name: str) -> None:
    """Load data from a single API configuration."""
    await api_loader.load_single(config, source_name)


async def load_all_apis(sources: Dict[str, List]) -> None:
    """Load data from all API configurations."""
    await api_loader.load_all(sources)


def load_api_sources(sources_dir: Path) -> List[Dict[str, Any]]:
    """Load API configuration files from the apis directory."""
    return api_loader.load_sources(sources_dir)
