import json
from pathlib import Path
from utils.schema_utils import validate_config

def load_dataset_sources(sources_dir: Path) -> list:
    datasets = []
    datasets_dir = sources_dir / "datasets"
    dataset_schema_path = datasets_dir / "schema.json"
    if datasets_dir.exists():
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
                        print(f"Error loading dataset config {structure_file}: {e}")
                        continue
                else:
                    print(f"Warning: Dataset folder '{dataset_folder.name}' missing structure.json")
    return datasets
