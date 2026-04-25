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
"""Standalone benchmark for `add_files(check_duplicate_files=True)`.

Measures wall-clock and `tracemalloc` peak for the dup-check phase across
N consecutive `add_files` calls on a growing table. Run before and after
the fix to compare; this script doesn't import unreleased code, so it
works against any pyiceberg checkout.

Usage:
    cd /path/to/iceberg-python
    uv run python tests/benchmark/bench_add_files_dup_check.py
"""

from __future__ import annotations

import gc
import tempfile
import time
import tracemalloc
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType


def _wide_schema(num_columns: int = 30) -> tuple[Schema, pa.Schema]:
    """Build a wide-ish schema so per-column stats decoding has work to do."""
    iceberg_fields = [NestedField(field_id=1, name="id", field_type=IntegerType(), required=True)]
    for i in range(2, num_columns + 1):
        iceberg_fields.append(
            NestedField(field_id=i, name=f"col_{i}", field_type=StringType(), required=False)
        )
    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema([pa.field("id", pa.int32(), nullable=False)] + [
        pa.field(f"col_{i}", pa.string(), nullable=True) for i in range(2, num_columns + 1)
    ])
    return iceberg_schema, arrow_schema


def _write_files(work_dir: Path, batch_idx: int, n_files: int, arrow_schema: pa.Schema) -> list[str]:
    """Write `n_files` tiny parquet files; return their absolute file:// paths."""
    paths: list[str] = []
    rows = pa.Table.from_pydict(
        {
            name: list(range(8)) if name == "id" else [f"v{batch_idx}-{j}" for j in range(8)]
            for name in arrow_schema.names
        },
        schema=arrow_schema,
    )
    for i in range(n_files):
        p = work_dir / f"batch_{batch_idx:03d}_file_{i:05d}.parquet"
        pq.write_table(rows, p)
        paths.append(f"file://{p}")
    return paths


def main() -> None:
    num_batches = 10
    files_per_batch = 200

    iceberg_schema, arrow_schema = _wide_schema(num_columns=30)

    with tempfile.TemporaryDirectory() as tmp_root:
        warehouse = Path(tmp_root) / "warehouse"
        data_dir = Path(tmp_root) / "data"
        warehouse.mkdir()
        data_dir.mkdir()

        catalog = InMemoryCatalog("bench", warehouse=f"file://{warehouse}")
        catalog.create_namespace("default")
        table = catalog.create_table("default.bench", schema=iceberg_schema)

        gc.collect()
        tracemalloc.start()

        print(f"\nadd_files(check_duplicate_files=True) benchmark")
        print(f"  batches={num_batches}, files_per_batch={files_per_batch}, columns={len(arrow_schema.names)}")
        print(f"{'batch':>5} {'wall_s':>8} {'tracemalloc_peak_MB':>22} {'cumulative_files':>17}")

        cumulative = 0
        for b in range(num_batches):
            paths = _write_files(data_dir, b, files_per_batch, arrow_schema)
            tracemalloc.reset_peak()
            t0 = time.perf_counter()
            table.add_files(file_paths=paths, check_duplicate_files=True)
            wall = time.perf_counter() - t0
            _, peak = tracemalloc.get_traced_memory()
            cumulative += files_per_batch
            print(f"{b:>5d} {wall:>8.2f} {peak / (1024 * 1024):>22.1f} {cumulative:>17d}")

        tracemalloc.stop()


if __name__ == "__main__":
    main()
