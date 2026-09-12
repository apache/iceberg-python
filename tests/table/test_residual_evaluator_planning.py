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

import pytest

from pyiceberg.expressions import AlwaysFalse, AlwaysTrue, And, BooleanExpression, EqualTo
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus, ManifestFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import ManifestGroupPlanner, Table, TableProperties
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.typedef import EMPTY_DICT, Properties, Record
from pyiceberg.types import LongType


class _ManifestEntriesPlanner(ManifestGroupPlanner):
    def __init__(
        self,
        table_metadata: TableMetadata,
        io: FileIO,
        row_filter: BooleanExpression,
        entries: list[ManifestEntry],
        options: Properties = EMPTY_DICT,
    ) -> None:
        super().__init__(table_metadata=table_metadata, io=io, row_filter=row_filter, options=options)
        self.entries = entries

    def plan_manifest_entries(self, _manifests: Iterable[ManifestFile]) -> Iterator[list[ManifestEntry]]:
        return iter([self.entries])


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
    *partition_specs: PartitionSpec,
    options: Properties = EMPTY_DICT,
) -> _ManifestEntriesPlanner:
    metadata = table_v2.metadata.model_copy(update={"partition_specs": list(partition_specs)})
    return _ManifestEntriesPlanner(
        table_metadata=metadata,
        io=table_v2.io,
        row_filter=row_filter,
        entries=entries,
        options=options,
    )


def test_plan_files_returns_correct_residuals_for_repeated_relevant_partitions(table_v2: Table) -> None:
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1, 10)),
        _manifest_entry(1, spec_id=0, partition=(1, 20)),
        _manifest_entry(2, spec_id=0, partition=(2, 30)),
    ]
    planner = _planner(table_v2, EqualTo("x", 1), entries, _identity_spec(0, 1, 2))

    tasks = list(planner.plan_files([]))

    assert [task.residual for task in tasks] == [AlwaysTrue(), AlwaysTrue(), AlwaysFalse()]


def test_plan_files_distinguishes_each_referenced_partition_field(table_v2: Table) -> None:
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1, 10)),
        _manifest_entry(1, spec_id=0, partition=(1, 20)),
    ]
    planner = _planner(
        table_v2,
        And(EqualTo("x", 1), EqualTo("y", 10)),
        entries,
        _identity_spec(0, 1, 2),
    )

    tasks = list(planner.plan_files([]))

    assert [task.residual for task in tasks] == [AlwaysTrue(), AlwaysFalse()]


def test_plan_files_isolates_residuals_by_partition_spec(table_v2: Table) -> None:
    predicate = EqualTo("x", 1)
    entries = [
        _manifest_entry(0, spec_id=0, partition=(1,)),
        _manifest_entry(1, spec_id=1, partition=(1,)),
    ]
    planner = _planner(
        table_v2,
        predicate,
        entries,
        _identity_spec(0, 1),
        _identity_spec(1, 2),
    )

    tasks = list(planner.plan_files([]))

    assert [task.residual for task in tasks] == [AlwaysTrue(), predicate]


def test_plan_files_distinguishes_each_transform_for_a_referenced_field(table_v2: Table) -> None:
    bucket_7: BucketTransform[int] = BucketTransform(7)
    bucket_5: BucketTransform[int] = BucketTransform(5)
    x_bucket_7 = bucket_7.transform(LongType())(1)
    x_bucket_5 = bucket_5.transform(LongType())(1)
    assert x_bucket_7 is not None
    assert x_bucket_5 is not None

    spec = PartitionSpec(
        PartitionField(1, 1000, bucket_7, "x_bucket_7"),
        PartitionField(1, 1001, bucket_5, "x_bucket_5"),
        PartitionField(2, 1002, IdentityTransform(), "partition_hash"),
        spec_id=0,
    )
    predicate = EqualTo("x", 1)
    entries = [
        _manifest_entry(0, spec_id=0, partition=(x_bucket_7, x_bucket_5, 10)),
        _manifest_entry(1, spec_id=0, partition=(x_bucket_7, (x_bucket_5 + 1) % 5, 20)),
    ]
    planner = _planner(table_v2, predicate, entries, spec)

    tasks = list(planner.plan_files([]))

    assert [task.residual for task in tasks] == [predicate, AlwaysFalse()]


@pytest.mark.parametrize("cache_size", ["0", "-1"])
def test_plan_files_rejects_non_positive_residual_cache_size(table_v2: Table, cache_size: str) -> None:
    planner = _planner(
        table_v2,
        EqualTo("x", 1),
        [_manifest_entry(0, spec_id=0, partition=(1,))],
        _identity_spec(0, 1),
        options={TableProperties.RESIDUAL_CACHE_MAX_SIZE: cache_size},
    )

    with pytest.raises(ValueError, match="read.residual-cache.max-size must be a positive integer"):
        list(planner.plan_files([]))
