# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Benchmark a realistic 15-leaf partition predicate when a prepared evaluator is shared across manifests.

Run with:
    uv run pytest tests/benchmark/test_partition_evaluator_benchmark.py -v -s -m benchmark
"""

from __future__ import annotations

import statistics
import timeit

import pytest

from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThanOrEqual, LessThanOrEqual, Or
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import ManifestGroupPlanner, Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField


def _data_file(file_number: int) -> DataFile:
    return DataFile.from_args(
        file_path=f"s3://bucket/data-{file_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(file_number % 11, file_number % 15),
        record_count=100,
        file_size_in_bytes=1,
    )


def _partition_filter() -> BooleanExpression:
    """Select five day ranges, each scoped to a region."""
    windows = ((0, 1, 1), (2, 3, 4), (4, 5, 7), (6, 7, 10), (8, 10, 13))
    branches = [
        And(
            And(GreaterThanOrEqual("event_day", start_day), LessThanOrEqual("event_day", end_day)),
            EqualTo("region_id", region_id),
        )
        for start_day, end_day, region_id in windows
    ]

    combined = branches[0]
    for branch in branches[1:]:
        combined = Or(combined, branch)
    return combined


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "files_per_manifest",
    [1_000, 1],
    ids=["many-files-per-manifest", "one-file-per-manifest"],
)
def test_partition_evaluator_reuse(table_v2: Table, files_per_manifest: int) -> None:
    num_files = 1_000
    schema = Schema(
        NestedField(1, "event_day", LongType(), required=True),
        NestedField(2, "region_id", LongType(), required=True),
    )
    spec = PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "event_day"),
        PartitionField(2, 1001, IdentityTransform(), "region_id"),
        spec_id=0,
    )
    metadata = TableMetadataV2(
        location="s3://bucket/table",
        last_column_id=2,
        schemas=[schema],
        current_schema_id=schema.schema_id,
        partition_specs=[spec],
        default_spec_id=spec.spec_id,
    )
    planner = ManifestGroupPlanner(table_metadata=metadata, io=table_v2.io, row_filter=_partition_filter())
    data_files = [_data_file(file_number) for file_number in range(num_files)]

    def evaluate_files() -> int:
        partition_evaluator = planner._build_partition_evaluator(spec.spec_id)
        matches = 0
        for start in range(0, num_files, files_per_manifest):
            matches += sum(partition_evaluator(data_file) for data_file in data_files[start : start + files_per_manifest])
        return matches

    assert evaluate_files() == 67
    iterations = 100
    timings_ms = [timing * 1_000 / iterations for timing in timeit.repeat(evaluate_files, number=iterations, repeat=3)]
    file_label = "file" if files_per_manifest == 1 else "files"

    print(
        f"Evaluated partitions for {num_files} files with {files_per_manifest} {file_label} per manifest "
        f"and a 15-leaf predicate in "
        f"{statistics.mean(timings_ms):.3f}ms (best: {min(timings_ms):.3f}ms)"
    )
