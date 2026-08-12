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

from pathlib import PosixPath
from typing import Any

import pyarrow as pa
import pytest

from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.schema import Schema
from pyiceberg.table.inspect import _readable_bound
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField, StringType
from tests.catalog.test_base import InMemoryCatalog


def test_readable_bound_with_empty_bytes() -> None:
    assert _readable_bound(StringType(), to_bytes(StringType(), "")) == ""


def test_readable_bound_without_bound() -> None:
    assert _readable_bound(StringType(), None) is None


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    cat = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    cat.create_namespace("default")
    return cat


def test_inspect_entries_and_files_render_empty_string_bound(catalog: InMemoryCatalog) -> None:
    schema = Schema(NestedField(1, "s", StringType(), required=False))
    tbl = catalog.create_table("default.empty_string_bound", schema)
    tbl.append(pa.table({"s": [""]}, schema=pa.schema([pa.field("s", pa.large_string(), nullable=True)])))

    entries_metrics = tbl.inspect.entries().to_pydict()["readable_metrics"][0]["s"]
    assert entries_metrics["lower_bound"] == ""
    assert entries_metrics["upper_bound"] == ""

    files_metrics = tbl.inspect.files().to_pydict()["readable_metrics"][0]["s"]
    assert files_metrics["lower_bound"] == ""
    assert files_metrics["upper_bound"] == ""


def test_inspect_entries_and_files_render_null_bound(catalog: InMemoryCatalog) -> None:
    schema = Schema(NestedField(1, "s", StringType(), required=False))
    tbl = catalog.create_table("default.null_bound", schema)
    tbl.append(pa.table({"s": [None]}, schema=pa.schema([pa.field("s", pa.large_string(), nullable=True)])))

    entries_metrics = tbl.inspect.entries().to_pydict()["readable_metrics"][0]["s"]
    assert entries_metrics["lower_bound"] is None
    assert entries_metrics["upper_bound"] is None

    files_metrics = tbl.inspect.files().to_pydict()["readable_metrics"][0]["s"]
    assert files_metrics["lower_bound"] is None
    assert files_metrics["upper_bound"] is None


@pytest.mark.parametrize("newest_entry_first", [False, True])
def test_partitions_uses_latest_snapshot_metadata(catalog: InMemoryCatalog, newest_entry_first: bool) -> None:
    table = catalog.create_table("default.snapshot_info", Schema(NestedField(1, "s", StringType(), required=False)))

    def data_file(spec_id: int) -> DataFile:
        file = DataFile.from_args(
            content=DataFileContent.DATA,
            partition=Record("a"),
            record_count=1,
            file_size_in_bytes=1,
        )
        file.spec_id = spec_id
        return file

    older_snapshot = Snapshot(
        snapshot_id=9_000_000_000_000_000,
        timestamp_ms=1_000,
        manifest_list="older.avro",
    )
    newer_snapshot = Snapshot(
        snapshot_id=8_000_000_000_000_000,
        timestamp_ms=2_000,
        manifest_list="newer.avro",
    )
    partitions_map: dict[tuple[str, Any], Any] = {}

    updates = [(data_file(spec_id=1), older_snapshot), (data_file(spec_id=2), newer_snapshot)]
    if newest_entry_first:
        updates.reverse()

    for file, snapshot in updates:
        table.inspect._update_partitions_map_from_manifest_entry(partitions_map, file, {"part": "a"}, snapshot)

    partition_row = next(iter(partitions_map.values()))
    assert partition_row["spec_id"] == 2
    assert partition_row["last_updated_at"] == newer_snapshot.timestamp_ms
    assert partition_row["last_updated_snapshot_id"] == newer_snapshot.snapshot_id
