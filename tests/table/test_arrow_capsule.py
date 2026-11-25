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
"""Tests for Arrow PyCapsule interface support on the read and write paths.

Covers the input/consumer side (write methods accept any object implementing
``__arrow_c_stream__``) and the output/producer side (scans expose
``__arrow_c_stream__`` so any Arrow consumer can ingest them).
"""

from pathlib import PosixPath
from typing import Any

import pyarrow as pa
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.io.pyarrow import _coerce_arrow_input
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "region", StringType(), required=False),
)
ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int32(), nullable=True),
        pa.field("region", pa.string(), nullable=True),
    ]
)
PARTITION_SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="region"))


class _ArrowStreamWrapper:
    """A minimal third-party-style Arrow producer.

    Exposes only ``__arrow_c_stream__`` -- it is deliberately *not* a
    ``pa.Table``/``pa.RecordBatchReader`` -- to stand in for libraries such as
    polars or arro3 without taking a dependency on them.
    """

    def __init__(self, data: pa.Table):
        self._data = data

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        return self._data.__arrow_c_stream__(requested_schema)


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def _data(ids: list[int], regions: list[str]) -> pa.Table:
    return pa.table({"id": pa.array(ids, type=pa.int32()), "region": regions}, schema=ARROW_SCHEMA)


def _string_view_data(ids: list[int], regions: list[str]) -> pa.Table:
    return pa.table(
        {"id": pa.array(ids, type=pa.int32()), "region": pa.array(regions, type=pa.string_view())},
        schema=pa.schema(
            [
                pa.field("id", pa.int32(), nullable=True),
                pa.field("region", pa.string_view(), nullable=True),
            ]
        ),
    )


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return sorted(table.to_pylist(), key=lambda r: r["id"])


def test_coerce_arrow_input() -> None:
    """Unit coverage of every branch of the coercion helper."""
    table = _data([1, 2, 3], ["us", "eu", "us"])

    # native types pass through unchanged (identity)
    assert _coerce_arrow_input(table) is table
    reader = table.to_reader()
    assert _coerce_arrow_input(reader) is reader

    # a foreign capsule producer is imported as a RecordBatchReader (streaming preserved)
    coerced = _coerce_arrow_input(_ArrowStreamWrapper(table))
    assert isinstance(coerced, pa.RecordBatchReader)
    assert coerced.read_all().num_rows == 3

    # anything else is rejected
    with pytest.raises(ValueError, match="Expected pa.Table, pa.RecordBatchReader"):
        _coerce_arrow_input(object())


# ---------------------------------------------------------------------------
# Input / consumer side (issue #2680)
# ---------------------------------------------------------------------------


def test_append_accepts_arrow_capsule(catalog: InMemoryCatalog) -> None:
    data = _data([1, 2, 3], ["us", "eu", "us"])

    # A stream of several batches must be fully drained.
    for name, stream in (("single", data), ("multi", pa.Table.from_batches(data.to_batches(max_chunksize=1)))):
        tbl = catalog.create_table(f"default.append_{name}_batch", schema=SCHEMA)

        tbl.append(_ArrowStreamWrapper(stream))

        assert _rows(tbl.scan().to_arrow()) == _rows(data), name


def test_overwrite_accepts_arrow_capsule(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.overwrite_capsule", schema=SCHEMA)
    tbl.append(_data([1, 2], ["us", "eu"]))

    tbl.overwrite(_ArrowStreamWrapper(_data([9], ["jp"])))

    assert _rows(tbl.scan().to_arrow()) == _rows(_data([9], ["jp"]))


def test_append_accepts_arrow_capsule_with_string_view(catalog: InMemoryCatalog) -> None:
    """Regression: Polars exports string columns as string_view over PyCapsule."""
    tbl = catalog.create_table("default.append_string_view", schema=SCHEMA)

    tbl.append(_ArrowStreamWrapper(_string_view_data([10, 11], ["ca", "mx"])))

    assert _rows(tbl.scan().to_arrow()) == _rows(_data([10, 11], ["ca", "mx"]))
    assert _rows(tbl.scan(row_filter="region == 'ca'").to_arrow()) == _rows(_data([10], ["ca"]))


# ---------------------------------------------------------------------------
# Output / producer side (issue #1655)
# ---------------------------------------------------------------------------


def test_scan_supports_arrow_c_stream(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.stream", schema=SCHEMA)
    tbl.append(_data([1, 2, 3], ["us", "eu", "us"]))

    consumed = pa.table(tbl.scan())

    assert _rows(consumed) == _rows(tbl.scan().to_arrow())


def test_scan_arrow_c_stream_respects_filter_projection_and_limit(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.scan_stream_filtered", schema=SCHEMA)
    tbl.append(_data([1, 2, 3], ["us", "eu", "us"]))

    scan = tbl.scan(row_filter="region == 'us'", selected_fields=("id",))
    consumed = pa.table(scan)

    assert consumed.column_names == ["id"]
    assert sorted(consumed.column("id").to_pylist()) == [1, 3]
    assert pa.table(scan.update(limit=1)).num_rows == 1


def test_capsule_roundtrip_scan_into_append(catalog: InMemoryCatalog) -> None:
    """The two halves compose: a scan (producer) can be appended into another
    table (consumer) with no explicit pyarrow conversion in between."""
    src = catalog.create_table("default.roundtrip_src", schema=SCHEMA)
    src.append(_data([1, 2, 3], ["us", "eu", "us"]))
    dst = catalog.create_table("default.roundtrip_dst", schema=SCHEMA)

    dst.append(src.scan())

    assert _rows(dst.scan().to_arrow()) == _rows(src.scan().to_arrow())


def test_scan_arrow_c_stream_casts_to_requested_schema(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.stream_requested_schema", schema=SCHEMA)
    tbl.append(_data([1, 2, 3], ["us", "eu", "us"]))
    requested = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])

    for scan in (tbl.scan(), tbl.incremental_append_scan()):
        consumed = pa.RecordBatchReader.from_stream(scan, schema=requested).read_all()

        assert consumed.schema == requested
        assert _rows(consumed) == _rows(_data([1, 2, 3], ["us", "eu", "us"]))


def test_scan_arrow_c_stream_has_no_field_ids(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.stream_field_ids", schema=SCHEMA)
    tbl.append(_data([1, 2, 3], ["us", "eu", "us"]))

    for scan in (tbl.scan(), tbl.incremental_append_scan()):
        assert all(field.metadata is None for field in pa.table(scan).schema)


def test_append_scan_matches_columns_by_name(catalog: InMemoryCatalog) -> None:
    """The source and destination give the same names to different field ids."""
    src = catalog.create_table(
        "default.by_name_src",
        schema=Schema(
            NestedField(1, "last", StringType(), required=False), NestedField(2, "first", StringType(), required=False)
        ),
    )
    src.append(pa.table({"last": ["Doe"], "first": ["Jane"]}))
    dst = catalog.create_table(
        "default.by_name_dst",
        schema=Schema(
            NestedField(1, "first", StringType(), required=False), NestedField(2, "last", StringType(), required=False)
        ),
    )

    dst.append(src.scan())

    assert dst.scan().to_arrow().to_pylist() == [{"first": "Jane", "last": "Doe"}]


def test_incremental_append_scan_supports_arrow_c_stream(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.incremental_stream", schema=SCHEMA)
    tbl.append(_data([1, 2], ["us", "eu"]))
    first_snapshot_id = tbl.metadata.current_snapshot_id
    tbl.append(_data([3], ["us"]))

    consumed = pa.table(tbl.incremental_append_scan(from_snapshot_id_exclusive=first_snapshot_id))

    assert _rows(consumed) == _rows(_data([3], ["us"]))
