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
    partitions_map: dict[tuple[int, Any], Any] = {}
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


def test_inspect_partitions_and_files_after_rename_field(catalog: InMemoryCatalog) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "p", StringType(), required=False),
    )
    spec = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p"))
    tbl = catalog.create_table("default.renamed_partition_col", schema, partition_spec=spec)
    tbl.append(pa.table({"id": [1, 2], "p": ["a", "b"]}))

    with tbl.update_spec() as spec_update:
        spec_update.rename_field("p", "p_renamed")
    tbl.append(pa.table({"id": [3], "p": ["c"]}))

    partitions = tbl.inspect.partitions().to_pydict()
    assert sorted(part["p_renamed"] for part in partitions["partition"]) == ["a", "b", "c"]
    assert set(partitions["spec_id"]) == {0, 1}

    for files_table in [
        tbl.inspect.files(),
        tbl.inspect.data_files(),
        tbl.inspect.all_files(),
        tbl.inspect.all_data_files(),
    ]:
        assert sorted(part["p_renamed"] for part in files_table.to_pydict()["partition"]) == ["a", "b", "c"]

    entries = tbl.inspect.entries().to_pydict()
    assert sorted(df["partition"]["p_renamed"] for df in entries["data_file"]) == ["a", "b", "c"]


def test_inspect_files_after_v1_partition_field_readd(catalog: InMemoryCatalog) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "p", StringType(), required=False),
    )
    spec = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p"))
    tbl = catalog.create_table("default.v1_partition_readd", schema, partition_spec=spec, properties={"format-version": "1"})
    tbl.append(pa.table({"id": [1, 2], "p": ["a", "b"]}))

    with tbl.update_spec() as spec_update:
        spec_update.remove_field("p")
    with tbl.update_spec() as spec_update:
        spec_update.add_identity("p")

    # In v1, the voided field is renamed to p_1000 and the new identity field is assigned id 1001 with name p.
    # Pre-existing data files from spec 0 belong to field 1000 (p_1000), not the newly added field 1001 (p).
    for files_table in [tbl.inspect.files(), tbl.inspect.all_files()]:
        partitions = files_table.to_pydict()["partition"]
        assert sorted(part["p_1000"] for part in partitions) == ["a", "b"]
        assert all(part["p"] is None for part in partitions)

    entries = tbl.inspect.entries().to_pydict()
    entry_partitions = [df["partition"] for df in entries["data_file"]]
    assert sorted(part["p_1000"] for part in entry_partitions) == ["a", "b"]
    assert all(part["p"] is None for part in entry_partitions)
