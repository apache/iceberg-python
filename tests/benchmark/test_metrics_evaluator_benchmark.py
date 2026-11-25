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
"""Benchmark a realistic 15-leaf metrics predicate through manifest planning.

Run with:
    uv run pytest tests/benchmark/test_metrics_evaluator_benchmark.py -v -s -m benchmark
"""

from __future__ import annotations

import statistics
import timeit
from collections.abc import Callable
from typing import Any

import pytest

import pyiceberg.table as table_module
from pyiceberg.conversions import to_bytes
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThanOrEqual, LessThanOrEqual, Or
from pyiceberg.manifest import DataFile, FileFormat, ManifestContent, ManifestFile
from pyiceberg.schema import Schema
from pyiceberg.table import ManifestGroupPlanner, Table
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField
from pyiceberg.utils.concurrent import ExecutorFactory


def _data_file(file_number: int) -> DataFile:
    event_day = file_number % 11
    region_id = file_number % 15
    event_day_bytes = to_bytes(LongType(), event_day)
    region_id_bytes = to_bytes(LongType(), region_id)
    return DataFile.from_args(
        file_path=f"s3://bucket/data-{file_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1,
        value_counts={1: 100, 2: 100},
        null_value_counts={1: 0, 2: 0},
        lower_bounds={1: event_day_bytes, 2: region_id_bytes},
        upper_bounds={1: event_day_bytes, 2: region_id_bytes},
    )


def _metrics_filter() -> BooleanExpression:
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


def _manifest_file(manifest_number: int) -> ManifestFile:
    return ManifestFile.from_args(
        manifest_path=f"s3://bucket/manifest-{manifest_number}.avro",
        manifest_length=1,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=1,
    )


class _SerialExecutor:
    def map(self, function: Callable[..., Any], *iterables: Any, **_kwargs: Any) -> Any:
        return map(function, *iterables)


@pytest.mark.benchmark
@pytest.mark.parametrize(
    ("files_per_manifest", "partition_matches"),
    [(1_000, True), (1, True), (1_000, False), (1, False)],
    ids=["dense", "fragmented", "all-pruned-dense", "all-pruned-fragmented"],
)
def test_metrics_evaluator_reuse(
    table_v2: Table,
    monkeypatch: pytest.MonkeyPatch,
    files_per_manifest: int,
    partition_matches: bool,
) -> None:
    num_files = 1_000
    schema = Schema(
        NestedField(1, "event_day", LongType(), required=True),
        NestedField(2, "region_id", LongType(), required=True),
        *(NestedField(field_id, f"unused_{field_id}", LongType(), required=False) for field_id in range(3, 103)),
        schema_id=table_v2.metadata.current_schema_id,
    )
    metadata = table_v2.metadata.model_copy(update={"schemas": [schema]})
    planner = ManifestGroupPlanner(table_metadata=metadata, io=table_v2.io, row_filter=_metrics_filter())
    data_files = [_data_file(file_number) for file_number in range(num_files)]
    manifests = [_manifest_file(manifest_number) for manifest_number in range(0, num_files, files_per_manifest)]
    files_by_manifest = {
        manifest.manifest_path: data_files[start : start + files_per_manifest]
        for manifest, start in zip(manifests, range(0, num_files, files_per_manifest), strict=True)
    }

    def open_manifest(
        _io: Any,
        manifest: ManifestFile,
        partition_evaluator: Callable[[DataFile], bool],
        metrics_evaluator: Callable[[DataFile], bool],
    ) -> list[DataFile]:
        return [
            data_file
            for data_file in files_by_manifest[manifest.manifest_path]
            if partition_evaluator(data_file) and metrics_evaluator(data_file)
        ]

    monkeypatch.setattr(planner, "_build_manifest_evaluator", lambda _: lambda _: True)
    monkeypatch.setattr(planner, "_build_partition_evaluator", lambda _: lambda _: partition_matches)
    monkeypatch.setattr(table_module, "_open_manifest", open_manifest)
    monkeypatch.setattr(ExecutorFactory, "get_or_create", staticmethod(lambda: _SerialExecutor()))

    def evaluate_files() -> int:
        return sum(len(entries) for entries in planner.plan_manifest_entries(manifests))

    assert evaluate_files() == (67 if partition_matches else 0)
    number = 1 if partition_matches else (1_000 if files_per_manifest == 1_000 else 10)
    timings_ms = [timing * 1_000 / number for timing in timeit.repeat(evaluate_files, number=number, repeat=5)]
    file_label = "file" if files_per_manifest == 1 else "files"
    pruning_label = "all files pruned" if not partition_matches else "metrics evaluated"

    print(
        f"Evaluated metrics for {num_files} files with {files_per_manifest} {file_label} per manifest, "
        f"a 102-column schema, and a 15-leaf predicate ({pruning_label}) in "
        f"{statistics.mean(timings_ms):.3f}ms (best: {min(timings_ms):.3f}ms)"
    )
