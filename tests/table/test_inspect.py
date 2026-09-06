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
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.inspect import InspectTable, _readable_bound
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.transforms import BucketTransform, IdentityTransform
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


def test_inspect_manifests_snapshot_selection(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.manifests", Schema(NestedField(1, "s", StringType())))
    empty = tbl.inspect.manifests()
    assert empty.num_rows == 0
    assert empty.equals(tbl.inspect.manifests(snapshot_id=None))
    assert tbl.inspect.all_manifests().num_rows == 0

    data = pa.table({"s": ["first"]})
    tbl.append(data)
    first = tbl.current_snapshot()
    assert first is not None
    first_rows = tbl.inspect.manifests()
    tbl.append(data)
    current = tbl.current_snapshot()
    assert current is not None
    current_rows = tbl.inspect.manifests()

    assert empty.schema == first_rows.schema == current_rows.schema
    assert first_rows.equals(tbl.inspect.manifests(snapshot_id=first.snapshot_id))
    assert current_rows.equals(tbl.inspect.manifests(snapshot_id=current.snapshot_id))
    assert first_rows["path"].to_pylist() == [manifest.manifest_path for manifest in first.manifests(tbl.io)]
    assert current_rows["path"].to_pylist() == [manifest.manifest_path for manifest in current.manifests(tbl.io)]
    assert first_rows.num_rows == 1
    assert current_rows.num_rows == 2
    assert first_rows["partition_summaries"].to_pylist() == [[]]

    all_rows = tbl.inspect.all_manifests()
    assert (
        all_rows.schema.remove(all_rows.schema.get_field_index("key_metadata")).remove(
            all_rows.schema.get_field_index("reference_snapshot_id")
        )
        == empty.schema
    )
    for snapshot, expected in [(first, first_rows), (current, current_rows)]:
        rows = [row for row in all_rows.to_pylist() if row.pop("reference_snapshot_id") == snapshot.snapshot_id]
        assert all(row.pop("key_metadata") is None for row in rows)
        assert rows == expected.to_pylist()
    assert all_rows.num_rows == 3  # Shared manifests remain repeated for each reference snapshot.

    tbl.maintenance.expire_snapshots().by_id(first.snapshot_id).commit()
    with pytest.raises(ValueError, match=f"Cannot find snapshot with ID {first.snapshot_id}"):
        tbl.inspect.manifests(snapshot_id=first.snapshot_id)


@pytest.mark.parametrize("snapshot_id", [0, -1, 9223372036854775807])
@pytest.mark.parametrize("populated", [False, True])
def test_inspect_manifests_invalid_snapshot(catalog: InMemoryCatalog, snapshot_id: int, populated: bool) -> None:
    tbl = catalog.create_table("default.invalid_snapshot", Schema(NestedField(1, "s", StringType())))
    if populated:
        tbl.append(pa.table({"s": ["value"]}))
    with pytest.raises(ValueError, match=f"Cannot find snapshot with ID {snapshot_id}"):
        tbl.inspect.manifests(snapshot_id=snapshot_id)


@pytest.mark.parametrize("value", ["old", "", None])
def test_inspect_manifests_schema_and_partition_evolution(catalog: InMemoryCatalog, value: str | None) -> None:
    schema = Schema(NestedField(1, "s", StringType()), NestedField(2, "id", IntegerType()))
    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "s_part"))
    tbl = catalog.create_table("default.evolved_manifests", schema, partition_spec=spec)
    tbl.append(pa.table({"s": [value], "id": [7]}, schema=pa.schema([("s", pa.string()), ("id", pa.int32())])))
    first = tbl.current_snapshot()
    assert first is not None
    first_rows = tbl.inspect.manifests()
    assert first_rows["partition_summaries"].to_pylist() == [
        [{"contains_null": value is None, "contains_nan": False, "lower_bound": value, "upper_bound": value}]
    ]

    with tbl.update_schema() as update:
        update.rename_column("s", "renamed")
    assert tbl.inspect.manifests(first.snapshot_id).equals(first_rows)
    with tbl.update_spec() as update:
        update.remove_field("s_part")
        update.add_field("id", BucketTransform(8), "id_bucket")
    with tbl.update_schema() as update:
        update.delete_column("renamed")
        # Reusing a name must not cause the old source ID to be resolved to the new type.
        update.add_column("s", LongType())
    tbl.append(pa.table({"s": [99], "id": [8]}, schema=pa.schema([("s", pa.int64()), ("id", pa.int32())])))
    current = tbl.current_snapshot()
    assert current is not None
    tbl = catalog.load_table(tbl.name())

    assert tbl.inspect.manifests(first.snapshot_id).equals(first_rows)
    current_rows = tbl.inspect.manifests()
    assert current_rows.equals(tbl.inspect.manifests(current.snapshot_id))
    by_spec = {row["partition_spec_id"]: row for row in current_rows.to_pylist()}
    assert by_spec[spec.spec_id] == first_rows.to_pylist()[0]
    bucket = str(BucketTransform(8).transform(IntegerType())(8))
    assert by_spec[tbl.spec().spec_id]["partition_summaries"] == [
        {"contains_null": False, "contains_nan": False, "lower_bound": bucket, "upper_bound": bucket}
    ]
    all_rows = tbl.inspect.all_manifests().to_pylist()
    assert len(all_rows) == 3
    assert [row["partition_summaries"] for row in all_rows if row["path"] == first_rows["path"][0].as_py()] == [
        first_rows["partition_summaries"][0].as_py(),
        first_rows["partition_summaries"][0].as_py(),
    ]


@pytest.mark.parametrize("missing_schema_id", [None, 999])
def test_inspect_manifests_schema_fallback(catalog: InMemoryCatalog, missing_schema_id: int | None) -> None:
    schema = Schema(NestedField(1, "s", StringType()))
    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "s"))
    tbl = catalog.create_table("default.legacy_manifests", schema, partition_spec=spec)
    tbl.append(pa.table({"s": ["legacy"]}))
    snapshot = tbl.current_snapshot()
    assert snapshot is not None
    expected = tbl.inspect.manifests()
    # Emulate legacy metadata while retaining the real local manifest and data files.
    tbl.metadata = tbl.metadata.model_copy(update={"snapshots": [snapshot.model_copy(update={"schema_id": missing_schema_id})]})
    if missing_schema_id is None:
        assert tbl.inspect.manifests(snapshot.snapshot_id).equals(expected)
        assert tbl.inspect.manifests().equals(expected)
    else:
        with pytest.warns(UserWarning, match=f"Metadata does not contain schema with id: {missing_schema_id}"):
            assert tbl.inspect.manifests(snapshot.snapshot_id).equals(expected)


@pytest.mark.parametrize(
    ("original_type", "promoted_type", "arrow_type", "value"),
    [(IntegerType(), LongType(), pa.int32(), 7), (FloatType(), DoubleType(), pa.float32(), 1.5)],
)
def test_inspect_manifests_promoted_partition_source(
    catalog: InMemoryCatalog,
    original_type: PrimitiveType,
    promoted_type: PrimitiveType,
    arrow_type: pa.DataType,
    value: int | float,
) -> None:
    schema = Schema(NestedField(1, "p", original_type))
    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "p"))
    tbl = catalog.create_table("default.promoted_manifests", schema, partition_spec=spec)
    tbl.append(pa.table({"p": [value]}, schema=pa.schema([("p", arrow_type)])))
    first = tbl.current_snapshot()
    assert first is not None
    expected = tbl.inspect.manifests()
    with tbl.update_schema() as update:
        update.update_column("p", field_type=promoted_type)
    tbl.append(pa.table({"p": [value]}, schema=pa.schema([("p", pa.int64() if isinstance(value, int) else pa.float64())])))

    assert tbl.inspect.manifests(first.snapshot_id).equals(expected)
    for row in tbl.inspect.manifests().to_pylist() + tbl.inspect.all_manifests().to_pylist():
        assert row["partition_summaries"] == expected["partition_summaries"][0].as_py()
