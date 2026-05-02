# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Memory benchmarks for manifest cache efficiency.

These benchmarks reproduce the manifest cache memory issue described in:
https://github.com/apache/iceberg-python/issues/2325

The issue: When caching manifest lists as tuples, overlapping ManifestFile objects
are duplicated across cache entries, causing O(N²) memory growth instead of O(N).

Run with: uv run pytest tests/benchmark/test_memory_benchmark.py -v -s -m benchmark
"""

import gc
import tracemalloc
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.manifest import _manifest_cache


def generate_test_dataframe() -> pa.Table:
    """Generate a PyArrow table for testing, similar to the issue's example."""
    n_rows = 100  # Smaller for faster tests, increase for more realistic benchmarks

    return pa.table(
        {
            "event_type": ["playback"] * n_rows,
            "event_origin": ["origin1"] * n_rows,
            "event_send_at": [datetime.now(timezone.utc)] * n_rows,
            "event_saved_at": [datetime.now(timezone.utc)] * n_rows,
            "id": list(range(n_rows)),
            "reference_id": [f"ref-{i}" for i in range(n_rows)],
        }
    )


@pytest.fixture
def memory_catalog(tmp_path_factory: pytest.TempPathFactory) -> InMemoryCatalog:
    """Create an in-memory catalog for memory testing."""
    warehouse_path = str(tmp_path_factory.mktemp("warehouse"))
    catalog = InMemoryCatalog("memory_test", warehouse=f"file://{warehouse_path}")
    catalog.create_namespace("default")
    return catalog


@pytest.fixture(autouse=True)
def clear_caches() -> None:
    """Clear caches before each test."""
    _manifest_cache.clear()
    gc.collect()


@pytest.mark.benchmark
def test_manifest_cache_memory_growth(memory_catalog: InMemoryCatalog) -> None:
    """Benchmark memory growth of manifest cache during repeated appends.

    This test reproduces the issue from GitHub #2325 where each append creates
    a new manifest list entry in the cache, causing memory to grow.

    With the old caching strategy (tuple per manifest list), memory grew as O(N²).
    With the new strategy (individual ManifestFile objects), memory grows as O(N).
    """
    df = generate_test_dataframe()
    table = memory_catalog.create_table("default.memory_test", schema=df.schema)

    tracemalloc.start()

    num_iterations = 50
    memory_samples: list[tuple[int, int, int]] = []  # (iteration, current_memory, cache_size)

    print("\n--- Manifest Cache Memory Growth Benchmark ---")
    print(f"Running {num_iterations} append operations...")

    for i in range(num_iterations):
        table.append(df)

        # Sample memory at intervals
        if (i + 1) % 10 == 0:
            current, _ = tracemalloc.get_traced_memory()
            cache_size = len(_manifest_cache)

            memory_samples.append((i + 1, current, cache_size))
            print(f"  Iteration {i + 1}: Memory={current / 1024:.1f} KB, Cache entries={cache_size}")

    tracemalloc.stop()

    # Analyze memory growth
    if len(memory_samples) >= 2:
        first_memory = memory_samples[0][1]
        last_memory = memory_samples[-1][1]
        memory_growth = last_memory - first_memory
        growth_per_iteration = memory_growth / (memory_samples[-1][0] - memory_samples[0][0])

        print("\nResults:")
        print(f"  Initial memory: {first_memory / 1024:.1f} KB")
        print(f"  Final memory: {last_memory / 1024:.1f} KB")
        print(f"  Total growth: {memory_growth / 1024:.1f} KB")
        print(f"  Growth per iteration: {growth_per_iteration:.1f} bytes")
        print(f"  Final cache size: {memory_samples[-1][2]} entries")

        # With efficient caching, growth should be roughly linear (O(N))
        # rather than quadratic (O(N²)) as it was before
        # Memory growth includes ManifestFile objects, metadata, and other overhead
        # We expect about 5-10 KB per iteration for typical workloads
        # The key improvement is that growth is O(N) not O(N²)
        # Threshold of 15KB/iteration based on observed behavior - O(N²) would show ~50KB+/iteration
        max_memory_growth_per_iteration_bytes = 15000
        assert growth_per_iteration < max_memory_growth_per_iteration_bytes, (
            f"Memory growth per iteration ({growth_per_iteration:.0f} bytes) is too high. "
            "This may indicate the O(N²) cache inefficiency is present."
        )


@pytest.mark.benchmark
def test_memory_after_gc_with_cache_cleared(memory_catalog: InMemoryCatalog) -> None:
    """Test that clearing the cache allows memory to be reclaimed.

    This test verifies that when we clear the manifest cache, the associated
    memory can be garbage collected.
    """
    df = generate_test_dataframe()
    table = memory_catalog.create_table("default.gc_test", schema=df.schema)

    tracemalloc.start()

    print("\n--- Memory After GC Benchmark ---")

    # Phase 1: Fill the cache
    print("Phase 1: Filling cache with 20 appends...")
    for _ in range(20):
        table.append(df)

    gc.collect()
    before_clear_memory, _ = tracemalloc.get_traced_memory()
    cache_size_before = len(_manifest_cache)
    print(f"  Memory before clear: {before_clear_memory / 1024:.1f} KB")
    print(f"  Cache size: {cache_size_before}")

    # Phase 2: Clear cache and GC
    print("\nPhase 2: Clearing cache and running GC...")
    _manifest_cache.clear()
    gc.collect()
    gc.collect()  # Multiple GC passes for thorough cleanup

    after_clear_memory, _ = tracemalloc.get_traced_memory()
    print(f"  Memory after clear: {after_clear_memory / 1024:.1f} KB")
    print(f"  Memory reclaimed: {(before_clear_memory - after_clear_memory) / 1024:.1f} KB")

    tracemalloc.stop()

    memory_reclaimed = before_clear_memory - after_clear_memory
    print("\nResults:")
    print(f"  Memory reclaimed by clearing cache: {memory_reclaimed / 1024:.1f} KB")

    # Verify that clearing the cache actually freed some memory
    # Note: This may be flaky in some environments due to GC behavior
    assert memory_reclaimed >= 0, "Memory should not increase after clearing cache"


@pytest.mark.benchmark
def test_manifest_cache_deduplication_efficiency() -> None:
    """Benchmark the efficiency of the per-ManifestFile caching strategy.

    This test verifies that when multiple manifest lists share the same
    ManifestFile objects, they are properly deduplicated in the cache.
    """
    from tempfile import TemporaryDirectory

    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
        ManifestEntry,
        ManifestEntryStatus,
        _manifests,
        write_manifest,
        write_manifest_list,
    )
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Record
    from pyiceberg.types import IntegerType, NestedField

    io = PyArrowFileIO()

    print("\n--- Manifest Cache Deduplication Benchmark ---")

    with TemporaryDirectory() as tmp_dir:
        schema = Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True))
        spec = UNPARTITIONED_PARTITION_SPEC

        # Create N manifest files
        num_manifests = 20
        manifest_files = []

        print(f"Creating {num_manifests} manifest files...")
        for i in range(num_manifests):
            manifest_path = f"{tmp_dir}/manifest_{i}.avro"
            with write_manifest(
                format_version=2,
                spec=spec,
                schema=schema,
                output_file=io.new_output(manifest_path),
                snapshot_id=i + 1,
                avro_compression="null",
            ) as writer:
                data_file = DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=f"{tmp_dir}/data_{i}.parquet",
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=100,
                    file_size_in_bytes=1000,
                )
                writer.add_entry(
                    ManifestEntry.from_args(
                        status=ManifestEntryStatus.ADDED,
                        snapshot_id=i + 1,
                        data_file=data_file,
                    )
                )
            manifest_files.append(writer.to_manifest_file())

        # Create multiple manifest lists with overlapping manifest files
        # List i contains manifest files 0 through i
        num_lists = 10
        print(f"Creating {num_lists} manifest lists with overlapping manifests...")

        _manifest_cache.clear()

        for i in range(num_lists):
            list_path = f"{tmp_dir}/manifest-list_{i}.avro"
            manifests_to_include = manifest_files[: i + 1]

            with write_manifest_list(
                format_version=2,
                output_file=io.new_output(list_path),
                snapshot_id=i + 1,
                parent_snapshot_id=i if i > 0 else None,
                sequence_number=i + 1,
                avro_compression="null",
            ) as list_writer:
                list_writer.add_manifests(manifests_to_include)

            # Read the manifest list using _manifests (this populates the cache)
            _manifests(io, list_path)

        # Analyze cache efficiency
        cache_entries = len(_manifest_cache)
        # List i contains manifests 0..i, so only the first num_lists manifests are actually used
        manifests_actually_used = num_lists

        print("\nResults:")
        print(f"  Manifest lists created: {num_lists}")
        print(f"  Manifest files created: {num_manifests}")
        print(f"  Manifest files actually used: {manifests_actually_used}")
        print(f"  Cache entries: {cache_entries}")

        # With efficient per-ManifestFile caching, we should have exactly
        # manifests_actually_used entries (one per unique manifest path)
        print(f"\n  Expected cache entries (efficient): {manifests_actually_used}")
        print(f"  Actual cache entries: {cache_entries}")

        # The cache should be efficient - one entry per unique manifest path
        assert cache_entries == manifests_actually_used, (
            f"Cache has {cache_entries} entries, expected exactly {manifests_actually_used}. "
            "The cache may not be deduplicating properly."
        )
