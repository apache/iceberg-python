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
from pathlib import Path

import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType


@pytest.fixture
def catalog(tmp_path: Path) -> Catalog:
    catalog = InMemoryCatalog("test", warehouse=str(tmp_path), uri=f"sqlite:///{tmp_path}/catalog.db")
    catalog.create_namespace("default")
    return catalog


TEST_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="some_column", field_type=StringType(), required=False),
    NestedField(field_id=3, name="other_column", field_type=StringType(), required=False),
)


def test_rename_then_move_first_by_new_name(catalog: Catalog) -> None:
    """Renaming a column and then moving it by its new name in the same
    update context should succeed (matches the Java implementation).

    Regression test for https://github.com/apache/iceberg-python/issues/2599.
    """
    tbl = catalog.create_table("default.test_rename_move_first", TEST_SCHEMA)

    with tbl.update_schema() as update:
        update.rename_column("some_column", "renamed_column")
        update.move_first("renamed_column")

    assert [field.name for field in tbl.schema().fields] == ["renamed_column", "id", "other_column"]


def test_rename_then_move_before_by_new_name(catalog: Catalog) -> None:
    tbl = catalog.create_table("default.test_rename_move_before", TEST_SCHEMA)

    with tbl.update_schema() as update:
        update.rename_column("some_column", "renamed_column")
        update.move_before("renamed_column", "id")

    assert [field.name for field in tbl.schema().fields] == ["renamed_column", "id", "other_column"]


def test_rename_then_move_after_by_new_name(catalog: Catalog) -> None:
    tbl = catalog.create_table("default.test_rename_move_after", TEST_SCHEMA)

    with tbl.update_schema() as update:
        update.rename_column("some_column", "renamed_column")
        update.move_after("renamed_column", "other_column")

    assert [field.name for field in tbl.schema().fields] == ["id", "other_column", "renamed_column"]
