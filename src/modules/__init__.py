"""
Data loader modules package.

This package provides automatic discovery and loading of all data source loaders.
Each loader inherits from BaseLoader and provides a consistent interface.

New loaders are automatically discovered - just create a new file with a class
that inherits from BaseLoader and it will be automatically loaded!
"""

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import List, Type, Dict, Optional

from .base_loader import BaseLoader


# Cache for discovered loaders
_LOADER_CACHE: Optional[List[Type[BaseLoader]]] = None


def _discover_loaders() -> List[Type[BaseLoader]]:
    """
    Automatically discover all loader classes in the modules package.
    
    This function scans all Python files in the modules directory,
    imports them, and finds all classes that inherit from BaseLoader
    (excluding BaseLoader itself).
    
    Returns:
        List of loader classes (not instances)
    """
    loaders: List[Type[BaseLoader]] = []
    
    # Get the current package's directory
    package_dir = Path(__file__).parent
    package_name = __name__
    
    # Iterate through all modules in this package
    for finder, module_name, ispkg in pkgutil.iter_modules([str(package_dir)]):
        # Skip __init__ and non-loader files
        if module_name.startswith('_'):
            continue
        
        # Skip the base_loader module itself
        if module_name == 'base_loader':
            continue
        
        try:
            # Import the module
            full_module_name = f"{package_name}.{module_name}"
            module = importlib.import_module(full_module_name)
            
            # Find all classes in the module
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a subclass of BaseLoader (but not BaseLoader itself)
                if (issubclass(obj, BaseLoader) and 
                    obj is not BaseLoader and
                    obj.__module__ == full_module_name):  # Ensure it's defined in this module
                    
                    loaders.append(obj)
                    print(f"[Module Discovery] Found loader: {obj.__name__} ({obj.LOADER_TYPE})")
        
        except Exception as e:
            print(f"[Module Discovery] Warning: Failed to load module '{module_name}': {e}")
            continue
    
    return loaders


def get_all_loader_classes() -> List[Type[BaseLoader]]:
    """
    Get all discovered loader classes.
    
    This function caches the results, so discovery only happens once.
    
    Returns:
        List of loader classes (not instances)
    """
    global _LOADER_CACHE
    
    if _LOADER_CACHE is None:
        _LOADER_CACHE = _discover_loaders()
    
    return _LOADER_CACHE


def get_all_loaders() -> List[BaseLoader]:
    """
    Get instances of all discovered loaders.
    
    Returns:
        List of loader instances
    """
    loader_classes = get_all_loader_classes()
    return [loader_class() for loader_class in loader_classes]


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
    loader_classes = get_all_loader_classes()
    
    for loader_class in loader_classes:
        # Create temporary instance to check type
        try:
            loader = loader_class()
            if loader.LOADER_TYPE == loader_type:
                return loader
        except Exception:
            continue
    
    raise ValueError(f"No loader found for type: {loader_type}")


def get_loader_info() -> Dict[str, Type[BaseLoader]]:
    """
    Get information about all available loaders.
    
    Returns:
        Dictionary mapping loader types to their classes
    """
    info = {}
    loader_classes = get_all_loader_classes()
    
    for loader_class in loader_classes:
        try:
            loader = loader_class()
            info[loader.LOADER_TYPE] = loader_class
        except Exception as e:
            print(f"Warning: Could not instantiate {loader_class.__name__}: {e}")
    
    return info


def extract_columns_from_column_map(column_map) -> set:
    """
    Extract database column names from a column_map configuration.
    
    Args:
        column_map: The column_map from a source configuration (can be dict or list)
        
    Returns:
        Set of database column names
    """
    columns = set()
    
    if isinstance(column_map, dict):
        # Object mapping: keys are database column names
        columns.update(column_map.keys())
    elif isinstance(column_map, list):
        # Array mapping: values are database column names
        columns.update(column_map)
    
    return columns


def scan_sources_for_columns(sources: Dict[str, list]) -> set:
    """
    Scan all loaded source configurations to extract database column names.
    
    Args:
        sources: Dictionary of loaded source configurations
        
    Returns:
        Set of all unique database column names across all sources
    """
    all_columns = set()
    
    for source_type, source_list in sources.items():
        for source_config in source_list:
            if 'column_map' in source_config:
                columns = extract_columns_from_column_map(source_config['column_map'])
                all_columns.update(columns)
    
    return all_columns


def load_all_sources(sources_dir: Path) -> Dict[str, list]:
    """
    Automatically load all source configurations using discovered loaders.
    
    This function:
    1. Discovers all available loaders
    2. Calls each loader's load_sources() method
    3. Scans all sources to determine required database columns
    4. Returns a dictionary with all loaded sources
    
    Args:
        sources_dir: Path to the sources directory
        
    Returns:
        Dictionary mapping source types to their configurations
        e.g., {'apis': [...], 'datasets': [...], 'imports': [...]}
        Also includes a special '_metadata' key with column information
    """
    sources = {}
    loaders = get_all_loaders()
    
    print(f"\n=== Loading Source Configurations ===")
    
    for loader in loaders:
        source_key = loader.get_source_key()
        try:
            loader_sources = loader.load_sources(sources_dir)
            sources[source_key] = loader_sources
            print(f"✓ Loaded {len(loader_sources)} {loader.LOADER_TYPE} source(s)")
        except Exception as e:
            print(f"✗ Error loading {loader.LOADER_TYPE} sources: {e}")
            sources[source_key] = []
    
    # Scan all sources for database columns
    all_columns = scan_sources_for_columns(sources)
    sources['_metadata'] = {
        'columns': sorted(all_columns),
        'column_count': len(all_columns)
    }
    
    print(f"\n=== Database Schema Analysis ===")
    print(f"Total unique columns required: {len(all_columns)}")
    if all_columns:
        print(f"Columns: {', '.join(sorted(all_columns))}")
    
    return sources


# Export the base class for use by other modules
__all__ = [
    'BaseLoader',
    'get_all_loaders',
    'get_all_loader_classes',
    'get_loader_by_type',
    'get_loader_info',
    'load_all_sources',
    'scan_sources_for_columns',
    'extract_columns_from_column_map',
]