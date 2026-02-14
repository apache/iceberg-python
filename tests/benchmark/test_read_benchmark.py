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
"""Read throughput micro-benchmark for ArrowScan configurations.

Measures records/sec and peak Arrow memory across streaming, concurrent_files,
and batch_size configurations introduced for issue #3036.

Memory is measured using pa.total_allocated_bytes() which tracks PyArrow's C++
memory pool (Arrow buffers, Parquet decompression), not Python heap allocations.

Run with: uv run pytest tests/benchmark/test_read_benchmark.py -v -s -m benchmark
"""

import gc
import statistics
import timeit
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog.sql import SqlCatalog

NUM_FILES = 32
ROWS_PER_FILE = 500_000
TOTAL_ROWS = NUM_FILES * ROWS_PER_FILE
NUM_RUNS = 3


def _generate_parquet_file(path: str, num_rows: int, seed: int) -> pa.Schema:
    """Write a synthetic Parquet file and return its schema."""
    table = pa.table(
        {
            "id": pa.array(range(seed, seed + num_rows), type=pa.int64()),
            "value": pa.array([float(i) * 0.1 for i in range(num_rows)], type=pa.float64()),
            "label": pa.array([f"row_{i}" for i in range(num_rows)], type=pa.string()),
            "flag": pa.array([i % 2 == 0 for i in range(num_rows)], type=pa.bool_()),
            "ts": pa.array([datetime.now(timezone.utc)] * num_rows, type=pa.timestamp("us", tz="UTC")),
        }
    )
    pq.write_table(table, path)
    return table.schema


@pytest.fixture(scope="session")
def benchmark_table(tmp_path_factory: pytest.TempPathFactory) -> "pyiceberg.table.Table":  # noqa: F821
    """Create a catalog and table with synthetic Parquet files for benchmarking."""
    warehouse_path = str(tmp_path_factory.mktemp("benchmark_warehouse"))
    catalog = SqlCatalog(
        "benchmark",
        uri=f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )
    catalog.create_namespace("default")

    # Generate files and append to table
    table = None
    for i in range(NUM_FILES):
        file_path = f"{warehouse_path}/data_{i}.parquet"
        _generate_parquet_file(file_path, ROWS_PER_FILE, seed=i * ROWS_PER_FILE)

        file_table = pq.read_table(file_path)
        if table is None:
            table = catalog.create_table("default.benchmark_read", schema=file_table.schema)
        table.append(file_table)

    return table


def _measure_peak_arrow_memory(benchmark_table, batch_size, streaming, concurrent_files):
    """Run a scan and track peak PyArrow C++ memory allocation."""
    gc.collect()
    pa.default_memory_pool().release_unused()
    baseline = pa.total_allocated_bytes()
    peak = baseline

    total_rows = 0
    for batch in benchmark_table.scan().to_arrow_batch_reader(
        batch_size=batch_size,
        streaming=streaming,
        concurrent_files=concurrent_files,
    ):
        total_rows += len(batch)
        current = pa.total_allocated_bytes()
        if current > peak:
            peak = current
        # Release the batch immediately to simulate a streaming consumer
        del batch

    return total_rows, peak - baseline


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "streaming,concurrent_files,batch_size",
    [
        pytest.param(False, 1, None, id="default"),
        pytest.param(True, 1, None, id="streaming-cf1"),
        pytest.param(True, 2, None, id="streaming-cf2"),
        pytest.param(True, 4, None, id="streaming-cf4"),
        pytest.param(True, 8, None, id="streaming-cf8"),
        pytest.param(True, 16, None, id="streaming-cf16"),
    ],
)
def test_read_throughput(
    benchmark_table: "pyiceberg.table.Table",  # noqa: F821
    streaming: bool,
    concurrent_files: int,
    batch_size: int | None,
) -> None:
    """Measure records/sec and peak Arrow memory for a scan configuration."""
    effective_batch_size = batch_size or 131_072  # PyArrow default
    if streaming:
        config_str = f"streaming=True, concurrent_files={concurrent_files}, batch_size={effective_batch_size}"
    else:
        config_str = f"streaming=False (executor.map, all files parallel), batch_size={effective_batch_size}"
    print(f"\n--- ArrowScan Read Throughput Benchmark ---")
    print(f"Config: {config_str}")
    print(f"  Files: {NUM_FILES}, Rows per file: {ROWS_PER_FILE}, Total rows: {TOTAL_ROWS}")

    elapsed_times: list[float] = []
    throughputs: list[float] = []
    peak_memories: list[int] = []

    for run in range(NUM_RUNS):
        # Measure throughput
        gc.collect()
        pa.default_memory_pool().release_unused()
        baseline_mem = pa.total_allocated_bytes()
        peak_mem = baseline_mem

        start = timeit.default_timer()
        total_rows = 0
        for batch in benchmark_table.scan().to_arrow_batch_reader(
            batch_size=batch_size,
            streaming=streaming,
            concurrent_files=concurrent_files,
        ):
            total_rows += len(batch)
            current_mem = pa.total_allocated_bytes()
            if current_mem > peak_mem:
                peak_mem = current_mem
        elapsed = timeit.default_timer() - start

        peak_above_baseline = peak_mem - baseline_mem
        rows_per_sec = total_rows / elapsed if elapsed > 0 else 0
        elapsed_times.append(elapsed)
        throughputs.append(rows_per_sec)
        peak_memories.append(peak_above_baseline)

        print(
            f"  Run {run + 1}: {elapsed:.2f}s, {rows_per_sec:,.0f} rows/s, "
            f"peak arrow mem: {peak_above_baseline / (1024 * 1024):.1f} MB"
        )

        assert total_rows == TOTAL_ROWS, f"Expected {TOTAL_ROWS} rows, got {total_rows}"

    mean_elapsed = statistics.mean(elapsed_times)
    stdev_elapsed = statistics.stdev(elapsed_times) if len(elapsed_times) > 1 else 0.0
    mean_throughput = statistics.mean(throughputs)
    mean_peak_mem = statistics.mean(peak_memories)

    print(
        f"  Mean: {mean_elapsed:.2f}s ± {stdev_elapsed:.2f}s, {mean_throughput:,.0f} rows/s, "
        f"peak arrow mem: {mean_peak_mem / (1024 * 1024):.1f} MB"
    )
