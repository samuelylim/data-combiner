"""
Data loader modules package.

This package provides automatic discovery and loading of all data source loaders.
Each loader inherits from BaseLoader and provides a consistent interface.
"""

from typing import List, Type
from .base_loader import BaseLoader
from .api_loader import APILoader, load_all_apis
from .dataset_loader import DatasetLoader, load_all_datasets
from .import_loader import ImportLoader, load_all_imports


# List of all available loader classes
ALL_LOADERS: List[Type[BaseLoader]] = [
    APILoader,
    DatasetLoader,
    ImportLoader,
]


def get_all_loaders() -> List[BaseLoader]:
    """
    Get instances of all available loaders.
    
    Returns:
        List of loader instances
    """
    return [loader_class() for loader_class in ALL_LOADERS]


def get_loader_by_type(loader_type: str) -> BaseLoader:
    """
    Get a loader instance by its type.
    
    Args:
        loader_type: The type of loader (e.g., 'api', 'dataset', 'import')
        
    Returns:
        Loader instance
        
    Raises:
        ValueError: If loader type is not found
    """
    for loader_class in ALL_LOADERS:
        loader = loader_class()
        if loader.LOADER_TYPE == loader_type:
            return loader
    
    raise ValueError(f"No loader found for type: {loader_type}")


__all__ = [
    'BaseLoader',
    'APILoader',
    'DatasetLoader',
    'ImportLoader',
    'ALL_LOADERS',
    'get_all_loaders',
    'get_loader_by_type',
    'load_all_apis',
    'load_all_datasets',
    'load_all_imports',
]
