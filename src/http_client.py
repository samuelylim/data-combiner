"""
HTTP client utilities for making requests with advanced features.

This module provides functionality for:
- Making HTTP requests with aiohttp
- Recursive/sub HTTP requests for auth token generation
- Environment variable substitution (env[variable_name])
- Query parameter merging with existing endpoint params
- Processing string values that may contain auth token generation arrays
"""

from typing import Union, Dict, Any, Optional
import json
import os
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import aiohttp


# Global session instance
_global_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    """
    Get or create the global aiohttp session.
    
    Returns:
        The global ClientSession instance
    """
    global _global_session
    if _global_session is None or _global_session.closed:
        _global_session = aiohttp.ClientSession()
    return _global_session


async def close_session() -> None:
    """
    Close the global aiohttp session.
    
    This should be called when the application is shutting down.
    """
    global _global_session
    if _global_session is not None and not _global_session.closed:
        await _global_session.close()
        _global_session = None


def _get_nested_value(data: Dict[str, Any], key_path: str) -> Any:
    """
    Extract a value from a nested dictionary using dot notation.
    
    Args:
        data: The dictionary to extract from
        key_path: Dot-separated path (e.g., "credentials.token" or "meta.total")
    
    Returns:
        The value at the specified path
        
    Raises:
        KeyError: If the path doesn't exist in the dictionary
    
    Examples:
        >>> _get_nested_value({"credentials": {"token": "abc123"}}, "credentials.token")
        'abc123'
    """
    keys = key_path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value[key]
        else:
            raise KeyError(f"Cannot access key '{key}' in non-dict value")
    return value


def _substitute_env_vars(text: str) -> str:
    """
    Replace environment variable placeholders with actual values.
    
    Replaces patterns like env[variable_name] with os.environ['variable_name'].
    
    Args:
        text: String potentially containing env[...] patterns
        
    Returns:
        String with environment variables substituted
        
    Raises:
        KeyError: If an environment variable is not found
    
    Examples:
        >>> os.environ['API_KEY'] = 'secret123'
        >>> _substitute_env_vars('Bearer env[API_KEY]')
        'Bearer secret123'
    """
    pattern = r'env\[([^\]]+)\]'
    
    def replacer(match):
        var_name = match.group(1)
        if var_name not in os.environ:
            raise KeyError(f"Environment variable '{var_name}' not found")
        return os.environ[var_name]
    
    return re.sub(pattern, replacer, text)


async def _process_string_value(value: Union[str, list], session: aiohttp.ClientSession) -> str:
    """
    Process a string value that can be a simple string or an array with auth token generation.
    
    This can be used for endpoints, headers, body, or any other string field.
    According to the data-combiner spec, string values can be:
    1. Simple strings (returned after env var substitution)
    2. Arrays containing strings and HTTP request objects for token generation
    
    Args:
        value: The string value (string or array)
        session: aiohttp session for making HTTP requests
        
    Returns:
        The processed value as a string
        
    Examples:
        >>> await _process_string_value("Bearer token123", session)
        'Bearer token123'
        
        >>> await _process_string_value(["Bearer ", {"endpoint": "...", ...}], session)
        'Bearer abc123token'
        
        >>> await _process_string_value("https://api.env[DOMAIN]/data", session)
        'https://api.example.com/data'
    """
    if isinstance(value, str):
        return _substitute_env_vars(value)
    
    if isinstance(value, list):
        # Build the final value by processing each element
        result_parts = []
        for element in value:
            if isinstance(element, str):
                # Direct string - substitute env vars
                result_parts.append(_substitute_env_vars(element))
            elif isinstance(element, dict):
                # HTTP request for token generation
                token = await _fetch_auth_token(element, session)
                result_parts.append(token)
            else:
                raise TypeError(f"Invalid string value element type: {type(element)}")
        return ''.join(result_parts)
    
    raise TypeError(f"Invalid string value type: {type(value)}")


async def _fetch_auth_token(auth_config: Dict[str, Any], session: aiohttp.ClientSession) -> str:
    """
    Fetch an authentication token by making an HTTP request.
    
    Args:
        auth_config: Configuration dict with 'endpoint' (required), and optional
                    'headers', 'body', 'method', 'token_key'
        session: aiohttp session for making the request
        
    Returns:
        The authentication token as a string
    """
    endpoint = auth_config['endpoint']
    method = auth_config.get('method', 'GET').upper()
    headers = auth_config.get('headers', {})
    body = auth_config.get('body')
    token_key = auth_config.get('token_key')
    
    # Process headers (they might contain env vars)
    processed_headers = {}
    for key, value in headers.items():
        if isinstance(value, str):
            processed_headers[key] = _substitute_env_vars(value)
        else:
            processed_headers[key] = str(value)
    
    # Process body (substitute env vars)
    processed_body = None
    if body:
        processed_body = _substitute_env_vars(body)
    
    # Make the request
    async with session.request(
        method=method,
        url=endpoint,
        headers=processed_headers,
        data=processed_body
    ) as response:
        response.raise_for_status()
        response_data = await response.json()
    
    # Extract token using token_key if provided, otherwise use entire response
    if token_key:
        return str(_get_nested_value(response_data, token_key))
    else:
        # If response is a dict, convert to JSON string; otherwise convert to string
        if isinstance(response_data, dict):
            return json.dumps(response_data)
        return str(response_data)


async def make_request(
    endpoint: Union[str, list],
    headers: Optional[Dict[str, Any]] = None,
    body: Optional[Union[str, list]] = None,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
) -> Union[Dict[str, Any], str, bytes]:
    """
    Make an HTTP request with support for complex string processing and parameter handling.
    
    This function handles:
    - Recursive/sub HTTP requests for auth token generation in any string field
    - Environment variable substitution (env[variable_name]) in endpoint, headers, and body
    - Query parameters that may already be in the endpoint
    - Merging additional params without duplicating the '?' separator
    - Uses a global session by default for connection pooling and efficiency
    - Returns appropriate content based on Content-Type (JSON as dict, text as str, binary as bytes)
    
    Args:
        endpoint: The full URL (can be string or array for token generation), 
                 potentially including query parameters
        headers: Dictionary of headers. Values can be strings or arrays (for token generation)
        body: Request body as a string or array (for token generation)
        params: Additional query parameters to add to the endpoint
        method: HTTP method (GET, POST, PUT, DELETE, PATCH)
        session: Optional aiohttp session. If not provided, uses the global session
        
    Returns:
        Response data in appropriate format:
        - Dict for JSON responses (application/json)
        - String for text responses (text/*, application/xml, text/html, text/csv, etc.)
        - Bytes for binary responses (Excel files, images, etc.)
        
    Raises:
        aiohttp.ClientError: For HTTP-related errors
        KeyError: If environment variables or token keys are not found
        
    Examples:
        >>> # Simple GET request (uses global session)
        >>> await make_request("https://api.example.com/data")
        
        >>> # With auth token generation in header
        >>> headers = {
        ...     "authorization": ["Bearer ", {
        ...         "endpoint": "https://auth.example.com/token",
        ...         "method": "POST",
        ...         "body": '{"client_id": "env[CLIENT_ID]"}',
        ...         "token_key": "access_token"
        ...     }]
        ... }
        >>> await make_request("https://api.example.com/data", headers=headers)
        
        >>> # With additional params (handles existing '?' in endpoint)
        >>> await make_request(
        ...     "https://api.example.com/data?filter=true",
        ...     params={"page": 1, "limit": 100}
        ... )
        
        >>> # With env vars in endpoint
        >>> await make_request("https://api.env[API_DOMAIN]/data")
        
        >>> # CSV/Excel/HTML responses
        >>> csv_data = await make_request("https://api.example.com/export.csv")
        >>> excel_data = await make_request("https://api.example.com/report.xlsx")
        
        >>> # Clean up at application shutdown
        >>> await close_session()
    """
    session = get_session()
    
    # Process endpoint - handle complex token generation and env vars
    processed_endpoint = await _process_string_value(endpoint, session)
    
    # Process headers - handle complex auth token generation
    processed_headers = {}
    if headers:
        for key, value in headers.items():
            processed_headers[key] = await _process_string_value(value, session)
    
    # Process body - handle token generation and env var substitution
    processed_body = None
    if body:
        processed_body = await _process_string_value(body, session)
    
    # Handle endpoint with potential existing query parameters
    parsed_url = urlparse(processed_endpoint)
    existing_params = parse_qs(parsed_url.query)
    
    # Merge existing params with new params
    if params:
        # Flatten existing params (parse_qs returns lists)
        flat_existing = {k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                       for k, v in existing_params.items()}
        # Merge with new params (new params take precedence)
        merged_params = {**flat_existing, **params}
    else:
        merged_params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v 
                       for k, v in existing_params.items()}
    
    # Rebuild URL with merged parameters
    query_string = urlencode(merged_params, doseq=True) if merged_params else ''
    final_url = urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        query_string,
        parsed_url.fragment
    ))
    
    # Make the request
    async with session.request(
        method=method.upper(),
        url=final_url,
        headers=processed_headers,
        data=processed_body
    ) as response:
        response.raise_for_status()
        
        # Determine content type and return appropriate format
        content_type = response.headers.get('Content-Type', '').lower()
        
        if 'application/json' in content_type:
            # JSON response - parse and return as dict
            return await response.json()
        elif any(text_type in content_type for text_type in [
            'text/', 'application/xml', 'application/csv', 'text/csv',
            'application/javascript', 'application/x-www-form-urlencoded'
        ]):
            # Text-based response - return as string
            return await response.text()
        else:
            # Binary response (Excel, images, PDFs, etc.) - return as bytes
            return await response.read()
