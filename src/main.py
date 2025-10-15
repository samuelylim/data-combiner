
import sys
import asyncio
from pathlib import Path
from utils.http_client import make_request, close_session
from modules.import_loader import load_all_imports
from modules.dataset_loader import load_all_datasets
from modules.api_loader import load_all_apis
from sources_loader import load_sources
from utils.schema_utils import load_schema, validate_config

if __name__ == "__main__":
    # Load sources from disk
    project_root = Path(__file__).parent.parent
    sources = load_sources(project_root)

    
    # Print summary
    print(f"\n=== Sources Loaded ===")
    print(f"APIs: {len(sources['apis'])}")
    print(f"Datasets: {len(sources['datasets'])}")
    print(f"Imports: {len(sources['imports'])}")
    print(f"Total: {sum(len(v) for v in sources.values())}")
    
    # Process all sources
    async def main():
        try:
            # Process datasets (local files)
            await load_all_datasets(sources)
            
            # Process imports (remote files)
            await load_all_imports(sources)
            
            # Process APIs (live data sources)
            await load_all_apis(sources)
        finally:
            # Clean up HTTP session
            await close_session()
    
    # Run async main
    asyncio.run(main())



