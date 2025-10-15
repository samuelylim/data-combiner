import asyncio
from api_loader import load_all_apis
from sources_loader import load_sources
from pathlib import Path
from utils.http_client import close_session

async def test():
    sources = load_sources(Path.cwd().parent)
    print(f"Found {len(sources['apis'])} APIs")
    await load_all_apis(sources)
    await close_session()

asyncio.run(test())
