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

import pyarrow as pa
import pytest

from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType
from tests.catalog.test_base import InMemoryCatalog

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


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def _data(ids: list[int], regions: list[str]) -> pa.Table:
    return pa.table({"id": pa.array(ids, type=pa.int32()), "region": regions}, schema=ARROW_SCHEMA)


def _rows(table: pa.Table) -> list[dict[str, object]]:
    return sorted(table.to_pylist(), key=lambda row: row["id"])


def test_table_exports_arrow_c_stream(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.stream_table", schema=SCHEMA)
    tbl.append(_data([1, 2, 3], ["us", "eu", "us"]))

    consumed = pa.table(tbl)

    assert _rows(consumed) == _rows(_data([1, 2, 3], ["us", "eu", "us"]))


def test_scan_exports_arrow_c_stream_with_filter_and_projection(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.stream_scan", schema=SCHEMA)
    tbl.append(_data([1, 2, 3], ["us", "eu", "us"]))

    consumed = pa.table(tbl.scan(row_filter="region == 'us'", selected_fields=("id",)))

    assert consumed.column_names == ["id"]
    assert sorted(consumed.column("id").to_pylist()) == [1, 3]


def test_incremental_append_scan_exports_arrow_c_stream(catalog: InMemoryCatalog) -> None:
    tbl = catalog.create_table("default.stream_incremental", schema=SCHEMA)
    tbl.append(_data([1, 2], ["us", "eu"]))
    tbl.append(_data([3], ["us"]))

    consumed = pa.table(tbl.incremental_append_scan())

    assert _rows(consumed) == _rows(_data([1, 2, 3], ["us", "eu", "us"]))
