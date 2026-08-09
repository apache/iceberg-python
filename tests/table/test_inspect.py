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

from pyiceberg.conversions import to_bytes
from pyiceberg.schema import Schema
from pyiceberg.table.inspect import _readable_bound
from pyiceberg.types import DoubleType, LongType, NestedField, StringType
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


def test_readable_bound_type_promotions() -> None:
    # 4-byte LE representation of integer 10 -> b'\x0a\x00\x00\x00'
    four_byte_int_bound = b"\x0a\x00\x00\x00"

    # 4-byte LE representation of float 10.0 -> b'\x00\x00\x20\x41'
    four_byte_float_bound = b"\x00\x00\x20\x41"

    # Test int -> long promotion decoding
    assert _readable_bound(LongType(), four_byte_int_bound) == 10

    # Test float -> double promotion decoding
    assert _readable_bound(DoubleType(), four_byte_float_bound) == 10.0


def test_inspect_files_type_promoted_bounds_e2e() -> None:
    import shutil
    import tempfile

    import pyarrow as pa

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, LongType, NestedField, StringType

    warehouse = tempfile.mkdtemp(prefix="iceberg_test_e2e_")
    try:
        catalog = SqlCatalog("test_e2e", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}")
        catalog.create_namespace("ns")

        tbl = catalog.create_table(
            "ns.t",
            schema=Schema(
                NestedField(1, "name", StringType(), required=False),
                NestedField(2, "qty", IntegerType(), required=False),
            ),
        )

        tbl.append(
            pa.Table.from_pylist(
                [{"name": "a", "qty": 10}],
                schema=pa.schema([pa.field("name", pa.string()), pa.field("qty", pa.int32())]),
            )
        )

        # Promote int -> long
        with tbl.update_schema() as update:
            update.update_column("qty", field_type=LongType())

        tbl = catalog.load_table("ns.t")

        # Test metadata inspection tables
        files_df = tbl.inspect.files()
        assert files_df.num_rows == 1

        entries_df = tbl.inspect.entries()
        assert entries_df.num_rows == 1

        manifests_df = tbl.inspect.manifests()
        assert manifests_df.num_rows >= 1
    finally:
        shutil.rmtree(warehouse, ignore_errors=True)
