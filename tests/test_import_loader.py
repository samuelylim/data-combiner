"""
Tests for the import loader module.
"""

import pytest
import asyncio
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from import_loader import (
    load_import,
    _process_column_value,
    _apply_transformation,
    _detect_file_format
)


def test_apply_transformation_multiply():
    """Test the multiply transformation."""
    transform_spec = {"type": "multiply", "factor": 100}
    result = _apply_transformation(5.5, transform_spec)
    assert result == 550.0


def test_process_column_value_simple_with_header():
    """Test simple column processing with headers."""
    row_data = {"name": "John", "age": "30", "city": "NYC"}
    result = _process_column_value(row_data, "name", "full_name", has_header=True)
    assert result == "John"


def test_process_column_value_simple_without_header():
    """Test simple column processing without headers (by index)."""
    row_data = ["John", "30", "NYC"]
    result = _process_column_value(row_data, "0", "full_name", has_header=False)
    assert result == "John"


def test_process_column_value_with_transform():
    """Test column processing with transformation."""
    row_data = {"price": 5.5}
    column_def = {
        "column": "price",
        "transform": {"type": "multiply", "factor": 100}
    }
    result = _process_column_value(row_data, column_def, "price_cents", has_header=True)
    assert result == 550.0


def test_detect_file_format_csv():
    """Test CSV format detection."""
    config = {"separator": ","}
    result = _detect_file_format(b"test", config)
    assert result == "csv"


def test_detect_file_format_tsv():
    """Test TSV format detection."""
    config = {"separator": "\t"}
    result = _detect_file_format(b"test", config)
    assert result == "tsv"


def test_detect_file_format_xlsx():
    """Test XLSX format detection."""
    config = {"sheet": "Sheet1"}
    result = _detect_file_format(b"test", config)
    assert result == "xlsx"


@pytest.mark.asyncio
async def test_load_import_example():
    """
    Integration test - load the example import configuration.
    
    This test requires internet connection to download the CSV.
    """
    # Load the example config
    project_root = Path(__file__).parent.parent
    example_config_path = project_root / "sources" / "imports" / "example.json"
    
    if not example_config_path.exists():
        pytest.skip("Example import config not found")
    
    import json
    with open(example_config_path, 'r') as f:
        config = json.load(f)
    
    # This should download and process the file
    # We're just checking it doesn't crash
    try:
        await load_import(config, "example.json")
    except Exception as e:
        pytest.fail(f"load_import failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
