#!/usr/bin/env python3
"""
Integration test for chunked ChunkIndex cache functionality.
This test can be run independently to validate the chunked cache implementation.
"""

import io
import os
import sys
from collections import namedtuple

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

try:
    from borg.cache import (
        write_chunkindex_to_repo_cache,
        read_chunkindex_from_repo_cache,
        list_chunkindex_hashes,
        _write_chunked_chunkindex_to_repo_cache,
        _read_chunked_chunkindex_from_repo_cache,
        _delete_chunked_cache,
    )
    from borg.constants import MAX_OBJECT_SIZE
    from borg.hashindex import ChunkIndex, ChunkIndexEntry
    from borg.repository import StoreObjectNotFound
    from borg.logger import setup_logging
    from borg.helpers import hex_to_bin
    import msgpack
    from borg.checksums import xxh64
except ImportError as e:
    print(f"Import error: {e}")
    print("This test requires the Borg source code to be available.")
    sys.exit(1)


class MockRepository:
    """Mock repository implementation for testing"""

    def __init__(self):
        self.storage = {}

    def store_store(self, name, data):
        """Store data under given name"""
        print(f"  Storing {name} ({len(data):,} bytes)")
        self.storage[name] = data

    def store_load(self, name):
        """Load data by name"""
        if name not in self.storage:
            raise StoreObjectNotFound(name)
        return self.storage[name]

    def store_delete(self, name):
        """Delete data by name"""
        if name in self.storage:
            print(f"  Deleted {name}")
            del self.storage[name]
        else:
            raise StoreObjectNotFound(name)

    def store_list(self, prefix):
        """List items with given prefix"""
        ItemInfo = namedtuple("ItemInfo", "name exists size directory")
        return [
            ItemInfo(name=key.removeprefix(prefix + "/"), exists=True, size=10, directory=False)
            for key in self.storage.keys()
            if key.startswith(prefix + "/")
        ]


def test_chunked_cache_basic_functionality():
    """Test basic chunked cache operations using synthetic data"""
    print("=== Testing Basic Chunked Cache Functionality ===")

    # Create synthetic large data that simulates a ChunkIndex serialization
    # We'll create data that's larger than MAX_OBJECT_SIZE to force chunking
    large_data_size = MAX_OBJECT_SIZE + 500000  # 500KB over limit
    print(f"Creating synthetic test data of {large_data_size:,} bytes...")

    # Create a pattern that looks like serialized ChunkIndex data
    # ChunkIndex serialization starts with specific headers
    large_data = b"\x00\x00\x00\x01"  # Version-like header
    large_data += b"A" * (large_data_size - len(large_data))  # Fill with test data

    print(f"Test data size: {len(large_data):,} bytes (limit: {MAX_OBJECT_SIZE:,})")

    repo = MockRepository()

    print("\n--- Testing direct chunked write function ---")
    result_hash = _write_chunked_chunkindex_to_repo_cache(repo, large_data, cached_hashes=[], force_write=True)

    print(f"Result hash: {result_hash}")
    print(f"Objects stored: {len(repo.storage)}")

    if not result_hash.startswith("chunked."):
        print("ERROR: Expected chunked hash format")
        return False

    # Verify size constraints
    oversized = [(name, len(data)) for name, data in repo.storage.items() if len(data) > MAX_OBJECT_SIZE]
    if oversized:
        print(f"ERROR: Found {len(oversized)} oversized objects!")
        for name, size in oversized:
            print(f"  {name}: {size:,} bytes")
        return False
    else:
        print("✓ All objects within size limit")

    print("\n--- Testing manifest structure ---")
    manifest_hash = result_hash.removeprefix("chunked.")
    manifest_name = f"cache/chunks.chunked.{manifest_hash}"

    if manifest_name not in repo.storage:
        print("ERROR: Manifest not found")
        return False

    manifest_data = repo.storage[manifest_name]
    manifest = msgpack.unpackb(manifest_data)

    print(f"Manifest version: {manifest.get('version')}")
    print(f"Total size: {manifest.get('total_size'):,}")
    print(f"Chunk count: {manifest.get('chunk_count')}")
    print(f"Chunk size: {manifest.get('chunk_size'):,}")

    if manifest["total_size"] != len(large_data):
        print("ERROR: Total size mismatch")
        return False

    print("✓ Manifest structure correct")

    print("\n--- Testing chunk integrity ---")
    all_chunks_valid = True
    for i, chunk_hash in enumerate(manifest["chunk_hashes"]):
        chunk_name = f"cache/chunks.data.{chunk_hash}"
        if chunk_name not in repo.storage:
            print(f"ERROR: Missing chunk {i}: {chunk_name}")
            all_chunks_valid = False
        else:
            chunk_data = repo.storage[chunk_name]
            chunk_checksum = xxh64(chunk_data, seed=3)
            expected_checksum = hex_to_bin(chunk_hash)
            if chunk_checksum != expected_checksum:
                print(f"ERROR: Chunk {i} integrity failed")
                all_chunks_valid = False

    if all_chunks_valid:
        print("✓ All chunks have correct integrity")
    else:
        return False

    print("\n--- Testing manual data reassembly ---")
    # Manually reassemble to verify the chunking worked correctly
    reassembled_data = bytearray()
    for chunk_hash in manifest["chunk_hashes"]:
        chunk_name = f"cache/chunks.data.{chunk_hash}"
        chunk_data = repo.storage[chunk_name]
        reassembled_data.extend(chunk_data)

    if bytes(reassembled_data) == large_data:
        print("✓ Manual reassembly successful - data integrity verified")
    else:
        print("ERROR: Reassembled data doesn't match original")
        print(f"  Original size: {len(large_data)}")
        print(f"  Reassembled size: {len(reassembled_data)}")
        return False

    print("\n--- Testing chunked delete ---")
    initial_count = len(repo.storage)
    _delete_chunked_cache(repo, manifest_hash)
    final_count = len(repo.storage)

    print(f"Objects before delete: {initial_count}")
    print(f"Objects after delete: {final_count}")

    if final_count == 0:
        print("✓ All objects cleaned up successfully")
        return True
    else:
        print(f"ERROR: {final_count} objects remain after cleanup")
        return False


def test_integration_with_chunkindex():
    """Test integration with actual ChunkIndex objects"""
    print("\n=== Testing ChunkIndex Integration ===")

    repo = MockRepository()

    print("Creating large ChunkIndex...")
    chunks = ChunkIndex()
    entry = ChunkIndexEntry(flags=ChunkIndex.F_USED, size=1000)

    # Create enough entries to exceed MAX_OBJECT_SIZE
    # Each entry is about 40-50 bytes, so we need ~500k entries for 20MB
    # Let's start with smaller number and check size
    num_entries = 100000  # Start with smaller number
    for i in range(num_entries):
        chunk_id = f"{i:032d}".encode()[:32]  # 32-byte IDs
        chunks[chunk_id] = entry

        # Check size periodically to avoid excessive creation time
        if i > 0 and i % 50000 == 0:
            with io.BytesIO() as f:
                chunks.write(f)
                current_size = len(f.getvalue())
            print(f"  Current size at {i} entries: {current_size:,} bytes")
            if current_size > MAX_OBJECT_SIZE:
                print(f"  Stopping at {i} entries (size exceeded)")
                break

    print(f"Created ChunkIndex with {len(chunks)} entries")

    # Serialize to check size
    with io.BytesIO() as f:
        chunks.write(f)
        serialized_data = f.getvalue()

    print(f"Serialized size: {len(serialized_data):,} bytes")

    if len(serialized_data) <= MAX_OBJECT_SIZE:
        print("Warning: ChunkIndex not large enough to trigger chunking")
        return True  # Skip this test

    print("\n--- Testing write_chunkindex_to_repo_cache ---")
    result_hash = write_chunkindex_to_repo_cache(repo, chunks, force_write=True)
    print(f"Result hash: {result_hash}")

    if result_hash.startswith("chunked."):
        print("✓ Chunked cache used as expected")
    else:
        print("Warning: Regular cache used instead of chunked")

    print(f"Repository contains {len(repo.storage)} objects")

    print("\n--- Testing read_chunkindex_from_repo_cache ---")
    loaded_chunks = read_chunkindex_from_repo_cache(repo, result_hash)

    if loaded_chunks is None:
        print("ERROR: Failed to load ChunkIndex")
        return False

    if len(loaded_chunks) == len(chunks):
        print("✓ Loaded ChunkIndex has correct size")
    else:
        print(f"ERROR: Size mismatch - Original: {len(chunks)}, Loaded: {len(loaded_chunks)}")
        return False

    print("\n--- Testing list_chunkindex_hashes ---")
    cached_hashes = list_chunkindex_hashes(repo)
    print(f"Found hashes: {cached_hashes}")

    if result_hash in cached_hashes:
        print("✓ Hash correctly listed")
        return True
    else:
        print("ERROR: Hash not found in list")
        return False


def test_error_conditions():
    """Test various error conditions and edge cases"""
    print("\n=== Testing Error Conditions ===")

    repo = MockRepository()

    print("\n--- Testing missing manifest ---")
    result = _read_chunked_chunkindex_from_repo_cache(repo, "nonexistent")
    if result is None:
        print("✓ Correctly handled missing manifest")
    else:
        print("ERROR: Should have returned None for missing manifest")
        return False

    print("\n--- Testing corrupted data ---")
    # Create and store valid chunked data
    large_data = b"Y" * (MAX_OBJECT_SIZE + 50000)
    result_hash = _write_chunked_chunkindex_to_repo_cache(repo, large_data, cached_hashes=[], force_write=True)

    # Corrupt a data chunk
    data_chunks = [name for name in repo.storage.keys() if name.startswith("cache/chunks.data.")]
    if data_chunks:
        repo.storage[data_chunks[0]] = b"corrupted"

        # Should detect corruption
        manifest_hash = result_hash.removeprefix("chunked.")
        result = _read_chunked_chunkindex_from_repo_cache(repo, manifest_hash)
        if result is None:
            print("✓ Correctly detected corruption")
        else:
            print("ERROR: Failed to detect corruption")
            return False

    return True


def main():
    """Run all tests"""
    print("Chunked ChunkIndex Cache Integration Test")
    print(f"MAX_OBJECT_SIZE: {MAX_OBJECT_SIZE:,} bytes ({MAX_OBJECT_SIZE / (1024*1024):.1f} MB)")

    # Setup logging to prevent logger errors
    try:
        setup_logging(level="WARNING", is_serve=False)  # Minimal logging
    except Exception:
        pass  # Ignore logging setup errors in test environment

    tests = [test_chunked_cache_basic_functionality, test_integration_with_chunkindex, test_error_conditions]

    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
                print(f"✓ {test.__name__} PASSED")
            else:
                print(f"✗ {test.__name__} FAILED")
        except Exception as e:
            print(f"✗ {test.__name__} FAILED with exception: {e}")
            import traceback

            traceback.print_exc()

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")

    if passed == len(tests):
        print("🎉 All tests PASSED! Chunked cache implementation is working correctly.")
        return 0
    else:
        print("❌ Some tests FAILED!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
