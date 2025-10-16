"""
Integration tests for database operations.

Tests the full flow of:
1. Schema initialization
2. Source registration
3. Record upsert with citations
"""

import pytest
import os
from pathlib import Path
from utils.db_operations import DatabaseManager, reset_db_manager
from sqlalchemy import text


@pytest.fixture
def test_db_manager():
    """Create a test database manager with a test schema."""
    # Use a test-specific schema
    test_schema = 'data_combiner_test_integration'
    manager = DatabaseManager(app_schema=test_schema)
    
    # Clean up any existing test schema
    with manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS `{test_schema}`"))
        conn.commit()
    
    yield manager
    
    # Cleanup after test
    with manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS `{test_schema}`"))
        conn.commit()
    
    manager.close()
    reset_db_manager()


def test_schema_initialization(test_db_manager):
    """Test that schema and tables are created correctly."""
    columns = ['name', 'email', 'license_num', 'address']
    
    test_db_manager.initialize_schema(columns)
    
    # Verify schema exists
    schema_info = test_db_manager.get_schema_info()
    assert schema_info['schema'] == 'data_combiner_test_integration'
    assert 'sources' in schema_info['tables']
    assert 'data' in schema_info['tables']
    assert 'citations' in schema_info['tables']
    
    # Verify data table has all columns
    data_columns = schema_info['tables']['data']['columns']
    for col in columns:
        assert col in data_columns


def test_source_registration(test_db_manager):
    """Test registering data sources."""
    columns = ['name', 'email']
    test_db_manager.initialize_schema(columns)
    
    # Register a source
    source_id = test_db_manager.register_source(
        source_name='test_api',
        source_type='api',
        config_path='sources/apis/test_api.json',
        unique_keys=['email']
    )
    
    assert source_id > 0
    
    # Registering again should return same ID
    source_id_2 = test_db_manager.register_source(
        source_name='test_api',
        source_type='api',
        config_path='sources/apis/test_api.json',
        unique_keys=['email']
    )
    
    assert source_id == source_id_2


def test_record_upsert_insert(test_db_manager):
    """Test inserting a new record."""
    columns = ['name', 'email', 'license_num']
    test_db_manager.initialize_schema(columns)
    
    test_db_manager.register_source(
        source_name='test_source',
        source_type='api',
        unique_keys=['license_num']
    )
    
    # Insert a new record
    record = {
        'name': 'John Doe',
        'email': 'john@example.com',
        'license_num': 'LIC123'
    }
    
    data_id = test_db_manager.upsert_record(
        record=record,
        source_name='test_source',
        unique_keys=['license_num']
    )
    
    assert data_id > 0
    
    # Verify record was inserted
    with test_db_manager.engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT * FROM `{test_db_manager.app_schema}`.`data` WHERE id = :id"),
            {'id': data_id}
        )
        row = result.fetchone()
        assert row is not None
        assert row.name == 'John Doe'
        assert row.email == 'john@example.com'
        assert row.license_num == 'LIC123'


def test_record_upsert_update(test_db_manager):
    """Test updating an existing record."""
    columns = ['name', 'email', 'license_num']
    test_db_manager.initialize_schema(columns)
    
    test_db_manager.register_source(
        source_name='source1',
        source_type='dataset',
        unique_keys=['license_num']
    )
    
    test_db_manager.register_source(
        source_name='source2',
        source_type='api',
        unique_keys=['license_num']
    )
    
    # Insert initial record from source1
    record1 = {
        'name': 'John Doe',
        'email': None,
        'license_num': 'LIC123'
    }
    
    data_id1 = test_db_manager.upsert_record(
        record=record1,
        source_name='source1',
        unique_keys=['license_num']
    )
    
    # Update with more complete data from source2
    record2 = {
        'name': 'John Doe',
        'email': 'john@example.com',
        'license_num': 'LIC123'
    }
    
    data_id2 = test_db_manager.upsert_record(
        record=record2,
        source_name='source2',
        unique_keys=['license_num']
    )
    
    # Should be same record
    assert data_id1 == data_id2
    
    # Verify record was updated
    with test_db_manager.engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT * FROM `{test_db_manager.app_schema}`.`data` WHERE id = :id"),
            {'id': data_id2}
        )
        row = result.fetchone()
        assert row is not None
        assert row.name == 'John Doe'
        assert row.email == 'john@example.com'  # Should be updated
        assert row.license_num == 'LIC123'


def test_citations(test_db_manager):
    """Test that citations are created correctly."""
    columns = ['name', 'license_num']
    test_db_manager.initialize_schema(columns)
    
    source1_id = test_db_manager.register_source(
        source_name='source1',
        source_type='dataset'
    )
    
    source2_id = test_db_manager.register_source(
        source_name='source2',
        source_type='api'
    )
    
    # Insert record from source1
    record = {
        'name': 'Business A',
        'license_num': 'LIC001'
    }
    
    data_id = test_db_manager.upsert_record(
        record=record,
        source_name='source1',
        unique_keys=['license_num']
    )
    
    # Update same record from source2
    test_db_manager.upsert_record(
        record=record,
        source_name='source2',
        unique_keys=['license_num']
    )
    
    # Verify both citations exist
    with test_db_manager.engine.connect() as conn:
        result = conn.execute(
            text(f"""
                SELECT source_id 
                FROM `{test_db_manager.app_schema}`.`citations` 
                WHERE data_id = :data_id
                ORDER BY source_id
            """),
            {'data_id': data_id}
        )
        citations = result.fetchall()
        
        assert len(citations) == 2
        assert citations[0].source_id == source1_id
        assert citations[1].source_id == source2_id


def test_multiple_unique_keys(test_db_manager):
    """Test using multiple columns as unique keys."""
    columns = ['first_name', 'last_name', 'email', 'city']
    test_db_manager.initialize_schema(columns)
    
    test_db_manager.register_source(
        source_name='test_source',
        source_type='api',
        unique_keys=['first_name', 'last_name']
    )
    
    # Insert record
    record1 = {
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john.doe@example.com',
        'city': 'New York'
    }
    
    data_id1 = test_db_manager.upsert_record(
        record=record1,
        source_name='test_source',
        unique_keys=['first_name', 'last_name']
    )
    
    # Try to insert different John Doe (different city, same name)
    record2 = {
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john.doe@example.com',
        'city': 'Los Angeles'
    }
    
    data_id2 = test_db_manager.upsert_record(
        record=record2,
        source_name='test_source',
        unique_keys=['first_name', 'last_name']
    )
    
    # Should update same record (same unique key values)
    assert data_id1 == data_id2
    
    # Verify city was updated
    with test_db_manager.engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT city FROM `{test_db_manager.app_schema}`.`data` WHERE id = :id"),
            {'id': data_id2}
        )
        row = result.fetchone()
        assert row.city == 'Los Angeles'
