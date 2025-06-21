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
import os
from datetime import datetime, timedelta
from pathlib import Path, PosixPath
from unittest.mock import patch

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType
from tests.catalog.test_base import InMemoryCatalog


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def test_remove_orphaned_files(catalog: Catalog) -> None:
    identifier = "default.test_remove_orphaned_files"

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "inhabitants", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("inhabitants", pa.int32(), nullable=False),
        ]
    )

    df = pa.Table.from_pylist(
        [
            {"city": "Drachten", "inhabitants": 45019},
            {"city": "Drachten", "inhabitants": 45019},
        ],
        schema=arrow_schema,
    )
    tbl.append(df)

    orphaned_file = Path(tbl.location()) / "orphan.txt"

    orphaned_file.touch()
    assert orphaned_file.exists()

    # assert no files deleted if dry run...
    tbl.maintenance.remove_orphaned_files(dry_run=True)
    assert orphaned_file.exists()

    # should not delete because it was just created...
    tbl.maintenance.remove_orphaned_files()
    assert orphaned_file.exists()

    # modify creation date to be older than 3 days
    five_days_ago = (datetime.now() - timedelta(days=5)).timestamp()
    os.utime(orphaned_file, (five_days_ago, five_days_ago))
    tbl.maintenance.remove_orphaned_files()
    assert not orphaned_file.exists()

    # assert that all known files still exist...
    all_known_files = tbl.inspect._all_known_files()
    for files in all_known_files.values():
        for file in files:
            assert Path(file).exists()


def test_remove_orphaned_files_with_invalid_file_doesnt_error(catalog: Catalog) -> None:
    identifier = "default.test_remove_orphaned_files"

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "inhabitants", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("inhabitants", pa.int32(), nullable=False),
        ]
    )

    df = pa.Table.from_pylist(
        [
            {"city": "Drachten", "inhabitants": 45019},
            {"city": "Drachten", "inhabitants": 45019},
        ],
        schema=arrow_schema,
    )
    tbl.append(df)

    file_that_does_not_exist = "foo/bar.baz"
    with patch.object(type(tbl.maintenance), "_orphaned_files", return_value={file_that_does_not_exist}):
        with patch.object(tbl.io, "delete", wraps=tbl.io.delete) as mock_delete:
            tbl.maintenance.remove_orphaned_files(timedelta(days=3))
            mock_delete.assert_called_with(file_that_does_not_exist)
