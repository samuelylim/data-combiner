"""
Dataset loader for local data files.

This module handles loading and processing local dataset files (CSV, TSV, XLSX)
according to dataset configurations defined in sources/datasets/<folder>/structure.json.
"""

import json
import re
from pathlib import Path
from typing import Dict, Any, List
from utils.schema_utils import validate_config
from utils.file_utils import detect_file_format, parse_csv_tsv, parse_xlsx
from utils.data_utils import process_column_value
from .base_loader import BaseLoader


class DatasetLoader(BaseLoader):
    """Loader for local dataset files."""
    
    LOADER_TYPE = "dataset"
    
    @staticmethod
    def get_files_to_process(config: Dict[str, Any], folder_path: Path) -> List[Path]:
        """
        Get list of files to process based on file_names and file_patterns.
        
        Args:
            config: Dataset configuration dictionary
            folder_path: Path to the dataset folder
            
        Returns:
            List of file paths to process
        """
        files_to_process = []
        
        # Get specified file names
        file_names = config.get("file_names", [])
        for file_name in file_names:
            file_path = folder_path / file_name
            if file_path.exists() and file_path.is_file():
                files_to_process.append(file_path)
            else:
                print(f"Warning: Specified file not found: {file_name}")
        
        # Get files matching patterns
        file_patterns = config.get("file_patterns", [])
        for pattern in file_patterns:
            # Convert glob pattern to regex-friendly format
            try:
                for file_path in folder_path.iterdir():
                    if file_path.is_file() and re.match(pattern, file_path.name):
                        if file_path not in files_to_process:
                            files_to_process.append(file_path)
            except re.error as e:
                print(f"Warning: Invalid file pattern '{pattern}': {e}")
        
        # If no files specified, get all non-structure.json files
        if not file_names and not file_patterns:
            for file_path in folder_path.iterdir():
                if file_path.is_file() and file_path.name != "structure.json":
                    files_to_process.append(file_path)
        
        return files_to_process
    
    async def load_single(self, config: Dict[str, Any], source_name: str) -> None:
        """
        Load data from a single dataset configuration.
        
        Reads local files, parses them, maps columns, applies transformations,
        and outputs records.
        
        Args:
            config: The dataset configuration dictionary (validated against dataset schema)
            source_name: Name of the dataset source for logging
            
        Raises:
            KeyError: If required fields are missing or columns not found
            ValueError: If transformations fail
            FileNotFoundError: If files cannot be found
        """
        print(f"\n=== Processing Dataset: {source_name} ===")
        
        # Extract configuration
        folder_path = Path(config["_folder_path"])
        column_map = config["column_map"]
        has_header = config.get("has_header", False)
        
        # Get files to process
        files_to_process = self.get_files_to_process(config, folder_path)
        
        if not files_to_process:
            print(f"Warning: No files found to process in dataset '{source_name}'")
            return
        
        print(f"Found {len(files_to_process)} file(s) to process")
        
        total_records_processed = 0
        
        # Process each file
        for file_path in files_to_process:
            print(f"\nProcessing file: {file_path.name}")
            
            try:
                # Read file content
                with open(file_path, 'rb') as f:
                    content = f.read()
                
                # Detect file format
                file_format = detect_file_format(content, config)
                print(f"Detected format: {file_format}")
                
                # Parse file based on format
                if file_format == "xlsx":
                    rows = parse_xlsx(content, config)
                else:
                    rows = parse_csv_tsv(content, config)
                
                print(f"Parsed {len(rows)} rows from {file_path.name}")
                
                # Process each row
                records_processed = 0
                for row_index, row_data in enumerate(rows):
                    try:
                        # Build record by mapping columns
                        record = {}
                        for db_column, column_def in column_map.items():
                            value = process_column_value(row_data, column_def, db_column, has_header)
                            record[db_column] = value
                        
                        # Output the record
                        await self.output_record(record, source_name)
                        records_processed += 1
                        
                    except Exception as e:
                        print(f"Error processing row {row_index + 1} in {file_path.name}: {e}")
                        # Continue processing other rows
                        continue
                
                print(f"Successfully processed {records_processed} records from {file_path.name}")
                total_records_processed += records_processed
                
            except Exception as e:
                print(f"Error processing file {file_path.name}: {e}")
                # Continue with other files
                continue
        
        print(f"\nTotal records processed from dataset '{source_name}': {total_records_processed}")
    
    async def load_all(self, sources: Dict[str, List]) -> None:
        """
        Load data from all dataset configurations.
        
        Args:
            sources: Dictionary with 'datasets' key containing list of dataset configs
        """
        dataset_configs = sources.get(self.get_source_key(), [])
        
        if not dataset_configs:
            print(f"No {self.LOADER_TYPE} configurations found")
            return
        
        print(f"\n{'='*60}")
        print(f"Loading {len(dataset_configs)} {self.LOADER_TYPE} source(s)")
        print(f"{'='*60}")
        
        for dataset_config in dataset_configs:
            source_name = dataset_config.get("_source_folder", "unknown")
            try:
                await self.load_single(dataset_config, source_name)
            except Exception as e:
                print(f"Error loading {self.LOADER_TYPE} '{source_name}': {e}")
                # Continue with other datasets
                continue
        
        print(f"\n{'='*60}")
        print(f"{self.LOADER_TYPE.upper() if self.LOADER_TYPE else 'SOURCE'} loading complete")
        print(f"{'='*60}")
    
    def load_sources(self, sources_dir: Path) -> List[Dict[str, Any]]:
        """
        Load dataset configuration files from the datasets directory.
        
        Args:
            sources_dir: Path to the sources directory
            
        Returns:
            List of validated dataset configurations
        """
        datasets = []
        datasets_dir = sources_dir / self.get_source_key()
        dataset_schema_path = datasets_dir / "schema.json"
        
        if not datasets_dir.exists():
            return datasets
        
        for dataset_folder in datasets_dir.iterdir():
            if dataset_folder.is_dir():
                structure_file = dataset_folder / "structure.json"
                if structure_file.exists():
                    try:
                        with open(structure_file, 'r', encoding='utf-8') as f:
                            dataset_config = json.load(f)
                            validate_config(dataset_config, dataset_schema_path, f"{dataset_folder.name}/structure.json")
                            dataset_config['_source_folder'] = dataset_folder.name
                            dataset_config['_folder_path'] = str(dataset_folder)
                            datasets.append(dataset_config)
                    except Exception as e:
                        print(f"Error loading {self.LOADER_TYPE} config {structure_file}: {e}")
                        continue
                else:
                    print(f"Warning: Dataset folder '{dataset_folder.name}' missing structure.json")
        
        return datasets


# Create a singleton instance
dataset_loader = DatasetLoader()

# Export convenience functions for backward compatibility
async def load_dataset(config: Dict[str, Any], source_name: str) -> None:
    """Load data from a single dataset configuration."""
    await dataset_loader.load_single(config, source_name)


async def load_all_datasets(sources: Dict[str, List]) -> None:
    """Load data from all dataset configurations."""
    await dataset_loader.load_all(sources)


def load_dataset_sources(sources_dir: Path) -> List[Dict[str, Any]]:
    """Load dataset configuration files from the datasets directory."""
    return dataset_loader.load_sources(sources_dir)
