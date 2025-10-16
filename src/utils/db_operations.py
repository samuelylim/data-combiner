"""
Database operations module for managing data records, sources, and citations.

This module provides the core database functionality including:
- Schema initialization
- Source registration
- Record upsert (insert/update) with citation tracking
- Configurable unique key handling
"""

import os
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Text, 
    DateTime, ForeignKey, Index, text, inspect
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DatabaseManager:
    """
    Manages database operations for the data combiner application.
    
    Handles schema creation, source registration, and record upserts with citation tracking.
    """
    
    def __init__(self, app_schema: Optional[str] = None):
        """
        Initialize the database manager.
        
        Args:
            app_schema: Name of the application schema (default from env: APP_SCHEMA)
        """
        self.app_schema = app_schema or os.getenv('APP_SCHEMA', 'data_combiner')
        self.engine = self._create_engine()
        self.metadata = MetaData()
        self._tables_initialized = False
        self._source_cache: Dict[str, int] = {}  # source_name -> source_id
    
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection parameters from environment."""
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = int(os.getenv('DB_PORT', '3306'))
        db_name = os.getenv('DB_NAME', 'data_combiner_test')
        db_user = os.getenv('DB_USER', 'testuser')
        db_password = os.getenv('DB_PASSWORD', 'testpassword')
        
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return create_engine(connection_string, echo=False)
    
    def initialize_schema(self, columns: List[str], force_recreate: bool = False) -> None:
        """
        Initialize the database schema with required tables.
        
        Creates:
        - {schema}.sources: Stores metadata about data sources
        - {schema}.data: Stores the actual data records with dynamic columns
        - {schema}.citations: Links data records to their sources
        
        Args:
            columns: List of column names required in the data table
            force_recreate: If True, drop and recreate existing tables
        """
        with self.engine.connect() as conn:
            # Create schema if it doesn't exist
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS `{self.app_schema}`"))
            conn.commit()
        
        # Define the tables
        self._define_tables(columns)
        
        # Create or update tables
        if force_recreate:
            self.metadata.drop_all(self.engine)
        
        self.metadata.create_all(self.engine)
        self._tables_initialized = True
        
        print(f"✓ Database schema '{self.app_schema}' initialized with {len(columns)} data columns")
    
    def _define_tables(self, columns: List[str]) -> None:
        """
        Define the table structures.
        
        Args:
            columns: List of column names for the data table
        """
        # Sources table
        self.sources_table = Table(
            'sources',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('name', String(255), nullable=False, unique=True),
            Column('source_type', String(50), nullable=False),  # 'api', 'dataset', 'import'
            Column('config_path', String(500)),
            Column('unique_keys', Text),  # JSON array of column names used for uniqueness
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            Index('idx_source_type', 'source_type'),
            schema=self.app_schema
        )
        
        # Data table with dynamic columns
        data_columns = [
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
        ]
        
        # Add dynamic columns from source configurations
        for col_name in columns:
            # All dynamic columns are TEXT to handle various data types
            data_columns.append(Column(col_name, Text, nullable=True))
        
        self.data_table = Table(
            'data',
            self.metadata,
            *data_columns,
            schema=self.app_schema
        )
        
        # Citations table (many-to-many between data and sources)
        self.citations_table = Table(
            'citations',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('data_id', Integer, ForeignKey(f'{self.app_schema}.data.id', ondelete='CASCADE'), nullable=False),
            Column('source_id', Integer, ForeignKey(f'{self.app_schema}.sources.id', ondelete='CASCADE'), nullable=False),
            Column('created_at', DateTime, default=datetime.utcnow),
            Index('idx_data_id', 'data_id'),
            Index('idx_source_id', 'source_id'),
            Index('idx_data_source_unique', 'data_id', 'source_id', unique=True),
            schema=self.app_schema
        )
    
    def register_source(
        self,
        source_name: str,
        source_type: str,
        config_path: Optional[str] = None,
        unique_keys: Optional[List[str]] = None
    ) -> int:
        """
        Register a data source or retrieve its ID if already exists.
        
        Args:
            source_name: Unique name of the source
            source_type: Type of source ('api', 'dataset', 'import')
            config_path: Path to the source configuration file
            unique_keys: List of column names that determine record uniqueness
            
        Returns:
            The source ID (newly created or existing)
        """
        if not self._tables_initialized:
            raise RuntimeError("Database schema not initialized. Call initialize_schema() first.")
        
        # Check cache first
        if source_name in self._source_cache:
            return self._source_cache[source_name]
        
        import json
        unique_keys_json = json.dumps(unique_keys) if unique_keys else None
        
        with self.engine.connect() as conn:
            # Try to insert, on duplicate key update
            stmt = mysql_insert(self.sources_table).values(
                name=source_name,
                source_type=source_type,
                config_path=config_path,
                unique_keys=unique_keys_json,
                updated_at=datetime.utcnow()
            )
            
            stmt = stmt.on_duplicate_key_update(
                source_type=stmt.inserted.source_type,
                config_path=stmt.inserted.config_path,
                unique_keys=stmt.inserted.unique_keys,
                updated_at=stmt.inserted.updated_at
            )
            
            result = conn.execute(stmt)
            conn.commit()
            
            source_id = result.lastrowid
            
            # If lastrowid is 0, it means we updated an existing record
            if source_id == 0:
                # Fetch the existing source_id
                select_stmt = self.sources_table.select().where(
                    self.sources_table.c.name == source_name
                )
                row = conn.execute(select_stmt).fetchone()
                if row:
                    source_id = row.id
                else:
                    raise RuntimeError(f"Failed to register source '{source_name}'")
            
            # Cache the source_id
            self._source_cache[source_name] = source_id
            
            return source_id
    
    def upsert_record(
        self,
        record: Dict[str, Any],
        source_name: str,
        unique_keys: Optional[List[str]] = None
    ) -> int:
        """
        Insert or update a data record based on unique keys.
        
        If a record with the same unique key values exists, it updates the record.
        Otherwise, it inserts a new record. Also creates a citation linking the
        record to the source.
        
        Args:
            record: Dictionary mapping column names to values
            source_name: Name of the source providing this data
            unique_keys: List of column names that determine uniqueness (default: all non-null keys in record)
            
        Returns:
            The data record ID (newly created or existing)
            
        Raises:
            RuntimeError: If schema not initialized or source not registered
            ValueError: If unique_keys contains columns not in record
        """
        if not self._tables_initialized:
            raise RuntimeError("Database schema not initialized. Call initialize_schema() first.")
        
        # Get source_id
        if source_name not in self._source_cache:
            raise RuntimeError(f"Source '{source_name}' not registered. Call register_source() first.")
        
        source_id = self._source_cache[source_name]
        
        # Determine unique keys (default to all keys in record if not specified)
        if unique_keys is None:
            unique_keys = [k for k in record.keys() if record[k] is not None]
        
        # Validate unique keys
        for key in unique_keys:
            if key not in record:
                raise ValueError(f"Unique key '{key}' not found in record")
        
        # Add timestamps
        now = datetime.utcnow()
        record_with_timestamps = {
            **record,
            'created_at': now,
            'updated_at': now
        }
        
        with self.engine.connect() as conn:
            # Build upsert statement
            stmt = mysql_insert(self.data_table).values(**record_with_timestamps)
            
            # Build update dict (exclude unique keys and created_at from update)
            update_dict = {
                k: stmt.inserted[k] 
                for k in record.keys() 
                if k not in unique_keys
            }
            update_dict['updated_at'] = stmt.inserted.updated_at
            
            if update_dict:
                stmt = stmt.on_duplicate_key_update(**update_dict)
            
            result = conn.execute(stmt)
            data_id = result.lastrowid
            
            # If lastrowid is 0, fetch the existing record id
            if data_id == 0:
                where_conditions = [
                    self.data_table.c[key] == record[key]
                    for key in unique_keys
                ]
                select_stmt = self.data_table.select().where(*where_conditions)
                row = conn.execute(select_stmt).fetchone()
                if row:
                    data_id = row.id
            
            # Create citation (if not already exists)
            citation_stmt = mysql_insert(self.citations_table).values(
                data_id=data_id,
                source_id=source_id,
                created_at=now
            )
            # Ignore duplicate citations
            citation_stmt = citation_stmt.on_duplicate_key_update(
                created_at=citation_stmt.inserted.created_at
            )
            conn.execute(citation_stmt)
            
            conn.commit()
            
            return data_id
    
    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get information about the current database schema.
        
        Returns:
            Dictionary with schema information including table names and column counts
        """
        inspector = inspect(self.engine)
        
        info = {
            'schema': self.app_schema,
            'tables': {}
        }
        
        for table_name in inspector.get_table_names(schema=self.app_schema):
            columns = inspector.get_columns(table_name, schema=self.app_schema)
            info['tables'][table_name] = {
                'columns': [col['name'] for col in columns],
                'column_count': len(columns)
            }
        
        return info
    
    def close(self):
        """Close the database connection."""
        self.engine.dispose()


# Singleton instance for use across the application
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """
    Get or create the singleton DatabaseManager instance.
    
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def reset_db_manager():
    """Reset the singleton DatabaseManager instance (useful for testing)."""
    global _db_manager
    if _db_manager is not None:
        _db_manager.close()
    _db_manager = None
