"""
Response value handler module.

This module provides functionality to handle response values that can be
either raw strings or transformer specifications, following the column_map
format from the data-combiner configuration.
"""

from typing import Union, Dict, Any
from transformers import get_transformer_class


def response_value_handler(value: Any, definition: Union[str, Dict[str, Any]]) -> str:
    """
    Handle response values based on column_map definitions from configuration.
    
    This function processes values according to the data-combiner column_map format.
    If the definition is a string, it's treated as a direct key lookup and the value
    is returned as a string. If it's a dict with a transform specification, the
    transformer is applied and the result is converted to a string.
    
    Args:
        value: The value to process (can be any type depending on the transformer)
        definition: Either a string (direct mapping, value returned as-is) or a
                   dictionary with optional 'transform' key containing transformer
                   specification (e.g., {"transform": {"type": "multiply", "factor": 100}})
    
    Returns:
        Always returns a string - either the value converted to string or the
        transformed value converted to a string.
    
    Raises:
        ValueError: If the transformer type is not recognized
        KeyError: If required parameters are missing from the transformer specification
        TypeError: If parameters have wrong type
    
    Examples:
        >>> # Direct mapping (string definition)
        >>> response_value_handler("John", "full_name")
        'John'
        
        >>> # With transformation (dict definition)
        >>> response_value_handler(5, {"transform": {"type": "multiply", "factor": 100}})
        '500'
    """
    # If definition is a string, just return the value as string
    if isinstance(definition, str):
        return str(value)
    
    # Handle dictionary definition with optional transform
    if isinstance(definition, dict):
        # If no transform specified, return value as string
        if "transform" not in definition:
            return str(value)
        
        transform_spec = definition["transform"]
        
        # Validate transform specification
        if not isinstance(transform_spec, dict):
            raise TypeError("Transform specification must be a dictionary")
        
        transformer_type = transform_spec.get("type")
        if not transformer_type:
            raise KeyError("Transform specification must include 'type' field")
        
        # Get the transformer class, create instance with validation, transform, and return as string
        transformer_class = get_transformer_class(transformer_type)
        transformer = transformer_class.from_dict(transform_spec)
        transformed_value = transformer.transform(value)
        return str(transformed_value)
    
    raise TypeError(f"Invalid definition type: {type(definition)}")