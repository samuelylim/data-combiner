"""
Data transformation and column mapping utilities.

This module provides functions for extracting, mapping, and transforming data values.
"""

from typing import Dict, Any, List, Union
from transformers import get_transformer_class


def apply_transformation(value: Any, transform_spec: Dict[str, Any]) -> Any:
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


def process_column_value(
    row_data: Union[Dict[str, Any], List[Any]],
    column_def: Union[str, int, Dict[str, Any]],
    db_column: str,
    has_header: bool = True
) -> Any:
    """
    Extract and optionally transform a value from row data.
    
    Args:
        row_data: Either a dict (if file has headers) or list (if no headers)
        column_def: Either a string/int (column name/index) or dict with 'column' and optional 'transform'
        db_column: The database column name (for error messages)
        has_header: Whether the file has a header row
        
    Returns:
        The processed value (possibly transformed)
        
    Raises:
        KeyError: If column not found
        ValueError: If transformation fails
    """
    # Determine the source column and optional transform
    if isinstance(column_def, (str, int)):
        source_column = column_def
        transform_spec = None
    elif isinstance(column_def, dict):
        source_column = column_def.get("column") or column_def.get("key")
        if source_column is None:
            raise KeyError(f"Column definition for '{db_column}' must include 'column' or 'key' field")
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
        value = apply_transformation(value, transform_spec)
    
    return value
