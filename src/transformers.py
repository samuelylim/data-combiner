"""
Transformers for processing response values.

This module contains transformer protocols and factory functions that can be
used to modify or transform response values in various ways.
"""

from abc import ABC, abstractmethod
from typing import Protocol, Any, Dict, Type, Union


class Transformer(Protocol):
    """
    Protocol for transformer objects.
    
    Any object that implements a transform method can be used as a transformer.
    This provides maximum flexibility through structural typing.
    """
    
    def transform(self, value: Any) -> Any:
        """
        Transform the input value.
        
        Args:
            value: The value to transform
            
        Returns:
            The transformed value
        """
        ...


class TransformerBase(ABC):
    """
    Abstract base class for transformers with schema validation.
    
    Subclasses MUST define:
    - transformer_type: str - The type identifier for this transformer
    - required_params: Dict[str, Union[type, tuple[type, ...]]] - Required parameters and their types
    - transform(value): method - The transformation logic
    
    Subclasses MAY define:
    - optional_params: Dict[str, tuple[Union[type, tuple[type, ...]], Any]] - Optional parameters with (type, default)
    """
    
    transformer_type: str = ""
    required_params: Dict[str, Union[type, tuple[type, ...]]] = {}
    optional_params: Dict[str, tuple[Union[type, tuple[type, ...]], Any]] = {}
    
    @abstractmethod
    def transform(self, value: Any) -> Any:
        """
        Transform the input value.
        
        Subclasses MUST implement this method.
        
        Args:
            value: The value to transform
            
        Returns:
            The transformed value
        """
        pass
    
    @classmethod
    def from_dict(cls, spec: Dict[str, Any]) -> 'TransformerBase':
        """
        Create a transformer instance from a dictionary specification.
        
        This method validates parameters and sets them as instance attributes directly,
        eliminating the need for __init__ methods in subclasses.
        
        Args:
            spec: Dictionary with transformer parameters
            
        Returns:
            An instance of the transformer with validated attributes
            
        Raises:
            KeyError: If required parameters are missing
            TypeError: If parameters have wrong type
        """
        # Create instance without calling __init__
        instance = cls.__new__(cls)
        
        # Validate and set required parameters as attributes
        for param_name, param_type in cls.required_params.items():
            if param_name not in spec:
                raise KeyError(f"Missing required parameter '{param_name}' for {cls.transformer_type}")
            value = spec[param_name]
            # Handle both single type and tuple of types
            if not isinstance(value, param_type):
                type_name = param_type.__name__ if isinstance(param_type, type) else str(param_type)
                raise TypeError(f"Parameter '{param_name}' must be {type_name}, got {type(value).__name__}")
            setattr(instance, param_name, value)
        
        # Handle optional parameters
        for param_name, (param_type, default) in cls.optional_params.items():
            if param_name in spec:
                value = spec[param_name]
                if not isinstance(value, param_type):
                    type_name = param_type.__name__ if isinstance(param_type, type) else str(param_type)
                    raise TypeError(f"Parameter '{param_name}' must be {type_name}, got {type(value).__name__}")
                setattr(instance, param_name, value)
            else:
                setattr(instance, param_name, default)
        
        return instance


class MultiplyTransformer(TransformerBase):
    """
    Transformer that multiplies a numeric value by a factor.
    
    Attributes:
        factor: The multiplication factor (set automatically from spec)
    """
    
    transformer_type = "multiply"
    required_params = {"factor": (int, float)}  # Accept int or float
    
    # Type hint for the attribute (set by from_dict)
    factor: float
    
    def transform(self, value: float) -> float:
        """
        Multiply the input value by the configured factor.
        
        Args:
            value: The numeric value to multiply
            
        Returns:
            The value multiplied by the factor
        """
        return value * self.factor


# Registry of all transformer types
TRANSFORMER_REGISTRY: Dict[str, Type[TransformerBase]] = {
    MultiplyTransformer.transformer_type: MultiplyTransformer,
}


def get_transformer_class(transformer_type: str) -> Type[TransformerBase]:
    """
    Get the transformer class for a given type.
    
    Args:
        transformer_type: The type identifier (e.g., "multiply")
        
    Returns:
        The transformer class
        
    Raises:
        ValueError: If the transformer type is not recognized
    """
    if transformer_type not in TRANSFORMER_REGISTRY:
        raise ValueError(f"Unknown transformer type: {transformer_type}")
    return TRANSFORMER_REGISTRY[transformer_type]
