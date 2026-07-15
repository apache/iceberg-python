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
"""Benchmark for scan planning over a table with many manifests.

Every manifest embeds an identical Avro schema string, so converting it to an
Iceberg schema on every manifest open is redundant. With that conversion cached,
only the unique schemas are converted rather than one per manifest.

Run with: uv run pytest tests/benchmark/test_avro_schema_cache_benchmark.py -v -s -m benchmark
"""

from __future__ import annotations

import statistics
import timeit

import pyarrow as pa
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog


@pytest.fixture
def memory_catalog(tmp_path_factory: pytest.TempPathFactory) -> InMemoryCatalog:
    warehouse_path = str(tmp_path_factory.mktemp("warehouse"))
    catalog = InMemoryCatalog("memory_test", warehouse=f"file://{warehouse_path}")
    catalog.create_namespace("default")
    return catalog


@pytest.mark.benchmark
def test_scan_planning_many_manifests(memory_catalog: InMemoryCatalog) -> None:
    """Time `scan().plan_files()` on a table with many manifests.

    Each append creates a new manifest, so this exercises the per-manifest cost
    of scan planning, dominated by the Avro-to-Iceberg schema conversion run once
    per manifest open.
    """
    num_appends = 150
    data = pa.table(
        {
            "id": pa.array(range(1000), pa.int64()),
            "val": pa.array([f"v{i % 50}" for i in range(1000)]),
        }
    )
    table = memory_catalog.create_table("default.scan_bench", schema=data.schema)
    for _ in range(num_appends):
        table.append(data)

    table = memory_catalog.load_table("default.scan_bench")
    # Warm up, and sanity check that each append contributes one data file to plan.
    assert len(list(table.scan().plan_files())) == num_appends

    num_runs = 10
    runs = []
    for _ in range(num_runs):
        start_time = timeit.default_timer()
        list(table.scan().plan_files())
        runs.append(timeit.default_timer() - start_time)

    print(f"\n--- scan planning over {num_appends} manifests ---")
    print(f"median: {statistics.median(runs) * 1000:.1f} ms  min: {min(runs) * 1000:.1f} ms  ({num_runs} runs)")
