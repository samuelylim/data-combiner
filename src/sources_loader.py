from pathlib import Path
from modules.api_loader import load_api_sources
from modules.dataset_loader import load_dataset_sources
from modules.import_loader import load_import_sources

def load_sources(project_root: Path) -> dict:
    sources_dir = project_root / "sources"
    return {
        "apis": load_api_sources(sources_dir),
        "datasets": load_dataset_sources(sources_dir),
        "imports": load_import_sources(sources_dir)
    }
