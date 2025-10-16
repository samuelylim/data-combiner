import asyncio
from pathlib import Path
from utils.http_client import close_session
from utils.db_operations import get_db_manager
from modules import get_all_loaders, load_all_sources

if __name__ == "__main__":
    # Load sources from disk using automatic discovery
    project_root = Path(__file__).parent.parent
    sources_dir = project_root / "sources"
    sources = load_all_sources(sources_dir)

    
    # Print summary
    print(f"\n=== Sources Summary ===")
    for source_type, source_list in sorted(sources.items()):
        if source_type == '_metadata':
            continue
        print(f"{source_type.capitalize()}: {len(source_list)}")
    print(f"Total: {sum(len(v) for k, v in sources.items() if k != '_metadata')}")
    
    # Initialize database schema
    print(f"\n=== Initializing Database ===")
    db_manager = get_db_manager()
    metadata = sources.get('_metadata', {})
    columns = metadata.get('columns', []) if isinstance(metadata, dict) else []
    db_manager.initialize_schema(columns)
    
    # Register all sources
    print(f"\n=== Registering Sources ===")
    loaders = get_all_loaders()
    for loader in loaders:
        source_key = loader.get_source_key()
        source_type = loader.LOADER_TYPE or 'unknown'
        
        if source_key in sources:
            source_list = sources[source_key]
            if isinstance(source_list, list):
                for source_config in source_list:
                    source_name = source_config.get('_source_file', 'unknown')
                    config_path = source_config.get('_config_path')
                    unique_keys = source_config.get('unique_keys')
                    
                    source_id = db_manager.register_source(
                        source_name=source_name,
                        source_type=source_type,
                        config_path=config_path,
                        unique_keys=unique_keys
                    )
                    print(f"✓ Registered {source_type} source '{source_name}' (ID: {source_id})")
    
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