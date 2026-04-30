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
from __future__ import annotations

import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.update.snapshot import _FastAppendFiles
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType


@pytest.fixture
def catalog(tmp_path: str) -> InMemoryCatalog:
    cat = InMemoryCatalog("test", warehouse=str(tmp_path))
    cat.create_namespace("default")
    return cat


def _make_data_file(i: int) -> DataFile:
    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=f"file:///tmp/part-{i:08d}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=12345,
        column_sizes={1: 100, 2: 100},
        value_counts={1: 100, 2: 100},
        null_value_counts={1: 0, 2: 0},
        nan_value_counts={},
        lower_bounds={},
        upper_bounds={},
        key_metadata=None,
        split_offsets=None,
        equality_ids=None,
        sort_order_id=None,
        spec_id=0,
    )


def test_fast_append_rolls_added_manifests_at_target_size(catalog: InMemoryCatalog) -> None:
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )
    table = catalog.create_table(
        "default.roll",
        schema=schema,
        properties={TableProperties.MANIFEST_TARGET_SIZE_BYTES: "4096"},
    )

    txn = table.transaction()
    append = _FastAppendFiles(operation=Operation.APPEND, transaction=txn, io=table.io)
    n_files = 200
    for i in range(n_files):
        append.append_data_file(_make_data_file(i))

    manifests = append._manifests()

    assert len(manifests) > 1, f"expected added files to roll into multiple manifests, got {len(manifests)}"
    assert sum(m.added_files_count or 0 for m in manifests) == n_files
    # The first manifest is written until it reaches the target, so it may slightly
    # overshoot; remaining manifests are chunked by the same entry count and should
    # be in the same ballpark.
    for m in manifests:
        assert m.manifest_length < 4 * 4096, f"manifest {m.manifest_path} is {m.manifest_length} bytes"


def test_fast_append_single_manifest_when_under_target(catalog: InMemoryCatalog) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = catalog.create_table("default.small", schema=schema)

    txn = table.transaction()
    append = _FastAppendFiles(operation=Operation.APPEND, transaction=txn, io=table.io)
    for i in range(3):
        append.append_data_file(_make_data_file(i))

    manifests = append._manifests()
    assert len(manifests) == 1
    assert manifests[0].added_files_count == 3
