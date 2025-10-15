#!/usr/bin/env python3
"""
Test script to verify automatic module discovery.

This script demonstrates that loaders are automatically discovered
without manual imports or registrations.
"""

import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from modules import get_all_loaders, get_all_loader_classes, get_loader_info

print("=" * 70)
print("AUTOMATIC MODULE DISCOVERY TEST")
print("=" * 70)

# Test 1: Get all loader classes
print("\n[TEST 1] Discovering loader classes...")
loader_classes = get_all_loader_classes()
print(f"✓ Found {len(loader_classes)} loader class(es)")
for cls in loader_classes:
    print(f"  - {cls.__name__}")

# Test 2: Get all loader instances
print("\n[TEST 2] Instantiating loaders...")
loaders = get_all_loaders()
print(f"✓ Created {len(loaders)} loader instance(s)")
for loader in loaders:
    print(f"  - {loader.__class__.__name__} (type: {loader.LOADER_TYPE})")

# Test 3: Get loader info
print("\n[TEST 3] Getting loader information...")
info = get_loader_info()
print(f"✓ Available loader types: {list(info.keys())}")
for loader_type, loader_class in info.items():
    print(f"  - '{loader_type}' -> {loader_class.__name__}")

# Test 4: Verify methods exist
print("\n[TEST 4] Verifying loader interface...")
for loader in loaders:
    has_load_single = hasattr(loader, 'load_single') and callable(loader.load_single)
    has_load_all = hasattr(loader, 'load_all') and callable(loader.load_all)
    has_load_sources = hasattr(loader, 'load_sources') and callable(loader.load_sources)
    
    if has_load_single and has_load_all and has_load_sources:
        print(f"✓ {loader.__class__.__name__} implements required interface")
    else:
        print(f"✗ {loader.__class__.__name__} missing methods:")
        if not has_load_single:
            print(f"    - load_single")
        if not has_load_all:
            print(f"    - load_all")
        if not has_load_sources:
            print(f"    - load_sources")

print("\n" + "=" * 70)
print("DISCOVERY TEST COMPLETE!")
print("=" * 70)
print("\nTo add a new loader:")
print("1. Create a new file in src/modules/ (e.g., new_loader.py)")
print("2. Define a class that inherits from BaseLoader")
print("3. Set LOADER_TYPE class attribute")
print("4. Implement load_single(), load_all(), and load_sources()")
print("5. That's it! It will be automatically discovered.")
print("=" * 70)
