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
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestContent, ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.inspect import InspectTable, _readable_bound
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import DoubleType, FloatType, IntegerType, LongType, NestedField, PrimitiveType, StringType
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


def test_inspect_entries_and_files_with_data_file_without_metrics(catalog: InMemoryCatalog) -> None:
    # column_sizes and the other metric maps are optional in a data file
    schema = Schema(NestedField(1, "x", LongType(), required=False))
    tbl = catalog.create_table("default.no_metrics", schema)
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=f"{tbl.location()}/data/a.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=1,
        spec_id=tbl.metadata.default_spec_id,
    )
    with tbl.transaction() as tx:
        with tx.update_snapshot().fast_append() as fast_append:
            fast_append.append_data_file(data_file)

    entries = tbl.inspect.entries().to_pylist()
    assert len(entries) == 1
    for metric in ("column_sizes", "value_counts", "null_value_counts", "nan_value_counts"):
        assert entries[0]["data_file"][metric] == []
    assert entries[0]["readable_metrics"]["x"]["column_size"] is None

    files = tbl.inspect.files().to_pylist()
    assert len(files) == 1
    assert files[0]["column_sizes"] is None


@pytest.mark.parametrize("newest_first", [False, True])
def test_partitions_last_updated_uses_latest_snapshot_regardless_of_order(newest_first: bool) -> None:
    # Manifest entries are visited in manifest order, which is not chronological, so the
    # `partitions` metadata table must keep the snapshot with the highest commit timestamp
    # per partition regardless of the order in which the entries are aggregated.
    older = Snapshot(snapshot_id=6446744073709551000, timestamp_ms=1000, manifest_list="file:///dev/null")
    newer = Snapshot(snapshot_id=8446744073709551111, timestamp_ms=5000, manifest_list="file:///dev/null")

    data_file = DataFile.from_args(content=DataFileContent.DATA, record_count=1, file_size_in_bytes=1, partition=Record("a"))
    data_file.spec_id = 0

    inspect = InspectTable.__new__(InspectTable)
    partitions_map: dict[tuple[str, Any], Any] = {}
    for snapshot in [newer, older] if newest_first else [older, newer]:
        inspect._update_partitions_map_from_manifest_entry(partitions_map, data_file, {"part": "a"}, snapshot)

    (partition_row,) = partitions_map.values()
    assert partition_row["last_updated_at"] == newer.timestamp_ms
    assert partition_row["last_updated_snapshot_id"] == newer.snapshot_id


def test_inspect_manifests_preserves_empty_string_bounds(catalog: InMemoryCatalog) -> None:
    schema = Schema(NestedField(1, "s", StringType()))
    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "s"))
    tbl = catalog.create_table("default.empty_string_partition", schema, partition_spec=spec)
    tbl.append(pa.table({"s": [""]}, schema=pa.schema([pa.field("s", pa.large_string())])))

    partition_summary = tbl.inspect.manifests().to_pydict()["partition_summaries"][0][0]
    assert partition_summary["lower_bound"] == ""
    assert partition_summary["upper_bound"] == ""


def test_inspect_bounds_after_type_promotion(catalog: InMemoryCatalog) -> None:
    # Promotion does not rewrite existing files, so their bounds keep the narrower 4-byte encoding
    # while the current schema type is the 8-byte one.
    cases: list[tuple[str, PrimitiveType, PrimitiveType, pa.DataType, list[float]]] = [
        ("int_to_long", IntegerType(), LongType(), pa.int32(), [1, 2]),
        ("float_to_double", FloatType(), DoubleType(), pa.float32(), [1.5, 2.5]),
    ]
    for name, file_type, promoted_type, arrow_type, values in cases:
        schema = Schema(NestedField(1, "v", file_type, required=False))
        spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "v"))
        tbl = catalog.create_table(f"default.{name}", schema, partition_spec=spec)
        tbl.append(pa.table({"v": values}, schema=pa.schema([pa.field("v", arrow_type, nullable=True)])))

        with tbl.update_schema() as update:
            update.update_column("v", field_type=promoted_type)

        for files_table in [
            tbl.inspect.entries(),
            tbl.inspect.files(),
            tbl.inspect.data_files(),
            tbl.inspect.all_files(),
            tbl.inspect.all_data_files(),
        ]:
            metrics = [m["v"] for m in files_table.to_pydict()["readable_metrics"]]
            assert sorted(m["lower_bound"] for m in metrics) == values, name
            assert sorted(m["upper_bound"] for m in metrics) == values, name

        partition_summaries = tbl.inspect.manifests().to_pydict()["partition_summaries"][0]
        assert partition_summaries[0]["lower_bound"] == str(values[0]), name
        assert partition_summaries[0]["upper_bound"] == str(values[-1]), name


def test_position_deletes_empty_table(catalog: InMemoryCatalog) -> None:
    schema = Schema(NestedField(1, "x", LongType(), required=False))
    tbl = catalog.create_table("default.position_deletes_empty", schema)

    result = tbl.inspect.position_deletes()

    assert result.num_rows == 0
    assert result.column_names == ["file_path", "pos", "row", "partition", "spec_id", "delete_file_path"]


def test_position_deletes_reads_live_delete_files(catalog: InMemoryCatalog) -> None:
    schema = Schema(NestedField(1, "x", LongType(), required=False))
    tbl = catalog.create_table("default.position_deletes", schema)
    delete_path = f"{tbl.location().rstrip('/')}/data/deletes.parquet"
    delete_records = pa.table(
        {
            "file_path": ["s3://bucket/data/a.parquet", "s3://bucket/data/b.parquet"],
            "pos": pa.array([1, 4], type=pa.int64()),
        }
    )
    with tbl.io.new_output(delete_path).create() as output:
        pq.write_table(delete_records, output)

    data_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=2,
        file_size_in_bytes=0,
        spec_id=tbl.metadata.default_spec_id,
    )
    manifest: Any = SimpleNamespace(
        content=ManifestContent.DELETES,
        partition_spec_id=tbl.metadata.default_spec_id,
        fetch_manifest_entry=lambda io, discard_deleted=True: [
            SimpleNamespace(status=ManifestEntryStatus.ADDED, data_file=data_file),
            SimpleNamespace(status=ManifestEntryStatus.DELETED, data_file=data_file),
        ],
    )

    rows = sorted(
        tbl.inspect._position_deletes_from_manifest(manifest).to_pylist(),
        key=lambda row: (row["file_path"], row["pos"]),
    )

    assert [(row["file_path"], row["pos"]) for row in rows] == [
        ("s3://bucket/data/a.parquet", 1),
        ("s3://bucket/data/b.parquet", 4),
    ]
    assert all(row["row"] is None for row in rows)
    assert {row["delete_file_path"] for row in rows} == {delete_path}
    assert {row["spec_id"] for row in rows} == {tbl.metadata.default_spec_id}
