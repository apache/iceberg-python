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

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from pyiceberg.expressions import And, BooleanExpression, EqualTo
from pyiceberg.expressions.visitors import ResidualEvaluator
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus, ManifestFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import ManifestGroupPlanner, Table, TableProperties
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.typedef import EMPTY_DICT, Properties, Record


class _CountingResidualEvaluator(ResidualEvaluator):
    def __init__(self, marker: int) -> None:
        self.marker = marker
        self.calls: list[tuple[Any, ...]] = []

    def residual_for(self, partition: Record) -> BooleanExpression:
        partition_values = tuple(partition[pos] for pos in range(len(partition)))
        self.calls.append(partition_values)
        return EqualTo("x", self.marker * 10 + partition[0])


class _TestManifestGroupPlanner(ManifestGroupPlanner):
    def __init__(
        self,
        table_metadata: TableMetadata,
        io: FileIO,
        row_filter: BooleanExpression,
        entries: list[ManifestEntry],
        evaluators: dict[int, _CountingResidualEvaluator],
        options: Properties = EMPTY_DICT,
    ) -> None:
        super().__init__(table_metadata=table_metadata, io=io, row_filter=row_filter, options=options)
        self.entries = entries
        self.evaluators = evaluators
        self.evaluator_builds: list[int] = []

    def plan_manifest_entries(self, _manifests: Iterable[ManifestFile]) -> Iterator[list[ManifestEntry]]:
        return iter([self.entries])

    def _build_residual_evaluator(self, spec_id: int) -> ResidualEvaluator:
        self.evaluator_builds.append(spec_id)
        return self.evaluators[spec_id]


def _manifest_entry(file_number: int, spec_id: int, partition: tuple[Any, ...]) -> ManifestEntry:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=f"s3://bucket/data-{file_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(*partition),
        record_count=1,
        file_size_in_bytes=1,
    )
    data_file.spec_id = spec_id
    return ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=1,
        sequence_number=1,
        file_sequence_number=1,
        data_file=data_file,
    )


def _identity_spec(spec_id: int, *source_ids: int) -> PartitionSpec:
    return PartitionSpec(
        *(
            PartitionField(
                source_id,
                1000 + spec_id * 10 + pos,
                IdentityTransform(),
                f"field_{source_id}_{pos}",
            )
            for pos, source_id in enumerate(source_ids)
        ),
        spec_id=spec_id,
    )


def _planner(
    table_v2: Table,
    row_filter: BooleanExpression,
    entries: list[ManifestEntry],
    evaluators: dict[int, _CountingResidualEvaluator],
    *partition_specs: PartitionSpec,
    options: Properties = EMPTY_DICT,
) -> _TestManifestGroupPlanner:
    metadata = table_v2.metadata.model_copy(update={"partition_specs": list(partition_specs)})
    return _TestManifestGroupPlanner(
        table_metadata=metadata,
        io=table_v2.io,
        row_filter=row_filter,
        entries=entries,
        evaluators=evaluators,
        options=options,
    )


def test_manifest_group_planner_reuses_residuals_by_spec_and_partition(table_v2: Table) -> None:
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1,)),
        _manifest_entry(1, spec_id=0, partition=(1,)),
        _manifest_entry(2, spec_id=1, partition=(1,)),
        _manifest_entry(3, spec_id=1, partition=(1,)),
        _manifest_entry(4, spec_id=0, partition=(2,)),
    ]
    evaluators = {0: _CountingResidualEvaluator(0), 1: _CountingResidualEvaluator(1)}
    planner = _planner(table_v2, EqualTo("x", 1), entries, evaluators, _identity_spec(0, 1), _identity_spec(1, 1))

    tasks = list(planner.plan_files([]))

    assert planner.evaluator_builds == [0, 1]
    assert evaluators[0].calls == [(1,), (2,)]
    assert evaluators[1].calls == [(1,)]
    assert [task.residual for task in tasks] == [
        EqualTo("x", 1),
        EqualTo("x", 1),
        EqualTo("x", 11),
        EqualTo("x", 11),
        EqualTo("x", 2),
    ]


def test_manifest_group_planner_ignores_unreferenced_partition_fields(table_v2: Table) -> None:
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1, 10)),
        _manifest_entry(1, spec_id=0, partition=(1, 20)),
        _manifest_entry(2, spec_id=0, partition=(2, 30)),
    ]
    evaluator = _CountingResidualEvaluator(0)
    planner = _planner(table_v2, EqualTo("x", 1), entries, {0: evaluator}, _identity_spec(0, 1, 2))

    tasks = list(planner.plan_files([]))

    assert evaluator.calls == [(1, 10), (2, 30)]
    assert [task.residual for task in tasks] == [EqualTo("x", 1), EqualTo("x", 1), EqualTo("x", 2)]


def test_manifest_group_planner_includes_referenced_partition_fields(table_v2: Table) -> None:
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1, 10)),
        _manifest_entry(1, spec_id=0, partition=(1, 20)),
    ]
    evaluator = _CountingResidualEvaluator(0)
    planner = _planner(
        table_v2,
        And(EqualTo("x", 1), EqualTo("y", 10)),
        entries,
        {0: evaluator},
        _identity_spec(0, 1, 2),
    )

    list(planner.plan_files([]))

    assert evaluator.calls == [(1, 10), (1, 20)]


def test_manifest_group_planner_includes_all_partition_transforms_for_referenced_source(table_v2: Table) -> None:
    spec = PartitionSpec(
        PartitionField(1, 1000, BucketTransform(7), "x_bucket_7"),
        PartitionField(1, 1001, BucketTransform(5), "x_bucket_5"),
        PartitionField(2, 1002, IdentityTransform(), "partition_hash"),
        spec_id=0,
    )
    entries = [
        _manifest_entry(0, spec_id=0, partition=(5, 0, 10)),
        _manifest_entry(1, spec_id=0, partition=(5, 1, 20)),
        _manifest_entry(2, spec_id=0, partition=(5, 0, 30)),
    ]
    evaluator = _CountingResidualEvaluator(0)
    planner = _planner(table_v2, EqualTo("x", 1), entries, {0: evaluator}, spec)

    list(planner.plan_files([]))

    assert evaluator.calls == [(5, 0, 10), (5, 1, 20)]


def test_manifest_group_planner_bounds_residual_cache(table_v2: Table) -> None:
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1,)),
        _manifest_entry(1, spec_id=0, partition=(2,)),
        _manifest_entry(2, spec_id=0, partition=(3,)),
        _manifest_entry(3, spec_id=0, partition=(1,)),
    ]
    evaluator = _CountingResidualEvaluator(0)
    planner = _planner(
        table_v2,
        EqualTo("x", 1),
        entries,
        {0: evaluator},
        _identity_spec(0, 1),
        options={TableProperties.RESIDUAL_CACHE_MAX_SIZE: "2"},
    )

    tasks = list(planner.plan_files([]))

    assert evaluator.calls == [(1,), (2,), (3,), (1,)]
    assert [task.residual for task in tasks] == [
        EqualTo("x", 1),
        EqualTo("x", 2),
        EqualTo("x", 3),
        EqualTo("x", 1),
    ]
