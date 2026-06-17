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
"""Memory benchmark for `add_files(check_duplicate_files=True)`.

Reproduces the per-call cost of the duplicate-files check on a growing
table. Before fix: each call materializes every DataFile in the snapshot
into a pyarrow Table (with readable_metrics, partition decode, full stats
dicts) and post-filters on file_path — peak memory grows roughly linearly
with cumulative file count, dominated by per-column stats decoding.
After fix: streaming manifest scan with set containment on file_path,
peak memory stays flat.

Run with: uv run pytest tests/benchmark/test_add_files_dup_check_benchmark.py -v -s -m benchmark
"""

from __future__ import annotations

import gc
import tempfile
import tracemalloc
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType


@pytest.fixture
def memory_catalog(tmp_path_factory: pytest.TempPathFactory) -> InMemoryCatalog:
    warehouse_path = str(tmp_path_factory.mktemp("warehouse"))
    catalog = InMemoryCatalog("memory_test", warehouse=f"file://{warehouse_path}")
    catalog.create_namespace("default")
    return catalog


def _wide_schema(num_columns: int = 30) -> tuple[Schema, pa.Schema]:
    """Build a wide-ish schema so per-column stats decoding has work to do."""
    iceberg_fields = [NestedField(field_id=1, name="id", field_type=IntegerType(), required=True)]
    for i in range(2, num_columns + 1):
        iceberg_fields.append(NestedField(field_id=i, name=f"col_{i}", field_type=StringType(), required=False))
    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(
        [pa.field("id", pa.int32(), nullable=False)]
        + [pa.field(f"col_{i}", pa.string(), nullable=True) for i in range(2, num_columns + 1)]
    )
    return iceberg_schema, arrow_schema


def _write_files(work_dir: Path, batch_idx: int, n_files: int, arrow_schema: pa.Schema) -> list[str]:
    paths: list[str] = []
    columns: dict[str, list[Any]] = {
        name: list(range(8)) if name == "id" else [f"v{batch_idx}-{j}" for j in range(8)] for name in arrow_schema.names
    }
    rows = pa.Table.from_pydict(columns, schema=arrow_schema)
    for i in range(n_files):
        p = work_dir / f"batch_{batch_idx:03d}_file_{i:05d}.parquet"
        pq.write_table(rows, p)
        paths.append(f"file://{p}")
    return paths


@pytest.mark.benchmark
def test_add_files_dup_check_memory_growth(memory_catalog: InMemoryCatalog) -> None:
    """Peak memory per `add_files(check_duplicate_files=True)` call should stay
    flat across consecutive calls on a growing table.

    With the materialize-then-filter implementation, peak grows roughly linearly
    with cumulative file count (per-column stats decoding into a pyarrow Table).
    With the streaming-scan implementation, peak stays bounded by the per-call
    workload.
    """
    num_batches = 10
    files_per_batch = 200
    iceberg_schema, arrow_schema = _wide_schema(num_columns=30)

    with tempfile.TemporaryDirectory() as tmp_root:
        data_dir = Path(tmp_root) / "data"
        data_dir.mkdir()
        table = memory_catalog.create_table("default.add_files_bench", schema=iceberg_schema)

        gc.collect()
        tracemalloc.start()

        peaks_mb: list[float] = []
        print(f"\n--- add_files dup-check benchmark ({num_batches} batches × {files_per_batch} files, 30 cols) ---")
        print(f"{'batch':>5} {'tracemalloc_peak_MB':>22} {'cumulative_files':>17}")

        cumulative = 0
        for b in range(num_batches):
            paths = _write_files(data_dir, b, files_per_batch, arrow_schema)
            tracemalloc.reset_peak()
            table.add_files(file_paths=paths, check_duplicate_files=True)
            _, peak = tracemalloc.get_traced_memory()
            peak_mb = peak / (1024 * 1024)
            peaks_mb.append(peak_mb)
            cumulative += files_per_batch
            print(f"{b:>5d} {peak_mb:>22.1f} {cumulative:>17d}")

        tracemalloc.stop()

        # Growth ratio: last call peak vs first call peak.
        # Materialize-then-filter (pre-fix): observed ~7× on this workload.
        # Streaming scan (post-fix): observed ~1×–1.5× (mostly noise).
        # Threshold of 3× catches the regression while tolerating variance.
        first_peak = peaks_mb[0]
        last_peak = peaks_mb[-1]
        ratio = last_peak / first_peak if first_peak > 0 else float("inf")
        print(f"\n  Peak ratio (last / first): {ratio:.1f}×")
        max_ratio = 3.0
        assert ratio < max_ratio, (
            f"Peak memory ratio ({ratio:.1f}×) exceeds {max_ratio}×. "
            "Dup-check materializes the full snapshot rather than streaming on file_path."
        )
