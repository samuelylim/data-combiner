import asyncio
from pathlib import Path
from utils.http_client import close_session
from modules import get_all_loaders, load_all_sources

if __name__ == "__main__":
    # Load sources from disk using automatic discovery
    project_root = Path(__file__).parent.parent
    sources_dir = project_root / "sources"
    sources = load_all_sources(sources_dir)

    
    # Print summary
    print(f"\n=== Sources Summary ===")
    for source_type, source_list in sorted(sources.items()):
        print(f"{source_type.capitalize()}: {len(source_list)}")
    print(f"Total: {sum(len(v) for v in sources.values())}")
    
    # Process all sources
    async def main():
        try:
            # Get all available loaders (automatically discovered)
            loaders = get_all_loaders()
            
            # Process each loader type
            for loader in loaders:
                await loader.load_all(sources)
                
        finally:
            # Clean up HTTP session
            await close_session()
    
    # Run async main
    asyncio.run(main())



