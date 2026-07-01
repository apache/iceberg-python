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
