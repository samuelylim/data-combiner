"""
Abstract base class for all data loaders.

This module defines the common interface and shared functionality
that all loader types (API, Dataset, Import) must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pathlib import Path
from utils.db_operations import get_db_manager


class BaseLoader(ABC):
    """
    Abstract base class for all data source loaders.
    
    Each loader type (API, Dataset, Import) should inherit from this class
    and implement the required abstract methods.
    """
    
    # Class attribute to identify the loader type
    LOADER_TYPE: Optional[str] = None
    
    def __init__(self):
        """Initialize the loader."""
        if self.LOADER_TYPE is None:
            raise NotImplementedError(f"{self.__class__.__name__} must define LOADER_TYPE")
    
    @abstractmethod
    async def load_single(self, config: Dict[str, Any], source_name: str) -> None:
        """
        Load data from a single source configuration.
        
        Args:
            config: The source configuration dictionary
            source_name: Name of the source for logging
            
        Raises:
            Exception: If loading fails
        """
        pass
    
    @abstractmethod
    async def load_all(self, sources: Dict[str, List]) -> None:
        """
        Load data from all sources of this type.
        
        Args:
            sources: Dictionary containing all source configurations
        """
        pass
    
    @abstractmethod
    def load_sources(self, sources_dir: Path) -> List[Dict[str, Any]]:
        """
        Load source configurations from the filesystem.
        
        Args:
            sources_dir: Path to the sources directory
            
        Returns:
            List of validated source configurations
        """
        pass
    
    async def output_record(self, record: Dict[str, Any], source_name: str, unique_keys: Optional[List[str]] = None) -> None:
        """
        Output a single record to the database.
        
        This method performs an upsert operation - it will insert a new record if one
        doesn't exist with the same unique key values, or update the existing record
        if it does. Also creates a citation linking the record to its source.
        
        Args:
            record: Dictionary mapping database column names to values
            source_name: Name of the source for logging and citation
            unique_keys: Optional list of column names that determine record uniqueness.
                        If not provided, uses the source's configured unique_keys.
                        If source has no configured unique_keys, uses all non-null keys in the record.
        """
        try:
            db_manager = get_db_manager()
            data_id = db_manager.upsert_record(record, source_name, unique_keys)
            print(f"[{source_name}] ✓ Record {data_id}: {record}")
        except Exception as e:
            print(f"[{source_name}] ✗ Error saving record: {e}")
            print(f"[{source_name}]   Record data: {record}")
            raise
    
    def get_source_key(self) -> str:
        """
        Get the key used to access this loader's sources in the sources dict.
        
        Returns:
            The plural form of the loader type (e.g., 'apis', 'datasets', 'imports')
        """
        return f"{self.LOADER_TYPE}s"
