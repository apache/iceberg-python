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
# pylint:disable=redefined-outer-name

import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.table.update.sorting import SortOrderBuilder
from pyiceberg.transforms import (
    IdentityTransform,
)
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


def _simple_table(catalog: Catalog, table_schema_simple: Schema) -> Table:
    return _create_table_with_schema(catalog, table_schema_simple, "1")


def _table(catalog: Catalog) -> Table:
    schema_with_timestamp = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
        NestedField(3, "str", StringType(), required=False),
    )
    return _create_table_with_schema(catalog, schema_with_timestamp, "1")


def _table_v2(catalog: Catalog) -> Table:
    schema_with_timestamp = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
        NestedField(3, "str", StringType(), required=False),
    )
    return _create_table_with_schema(catalog, schema_with_timestamp, "2")


def _create_table_with_schema(catalog: Catalog, schema: Schema, format_version: str) -> Table:
    tbl_name = "default.test_schema_evolution"
    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier=tbl_name, schema=schema, properties={"format-version": format_version})


@pytest.mark.integration
def test_sort_order_builder() -> None:
    builder = SortOrderBuilder(last_sort_order_id=0)
    builder.add_sort_field(1, IdentityTransform(), SortDirection.ASC, NullOrder.NULLS_FIRST)
    builder.add_sort_field(2, IdentityTransform(), SortDirection.DESC, NullOrder.NULLS_LAST)
    assert builder.sort_order == SortOrder(
        SortField(1, IdentityTransform(), SortDirection.ASC, NullOrder.NULLS_FIRST),
        SortField(2, IdentityTransform(), SortDirection.DESC, NullOrder.NULLS_LAST),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("session_catalog_hive")])
def test_map_column_name_to_id(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple)
    for col_name, col_id in {"foo": 1, "bar": 2, "baz": 3}.items():
        assert col_id == simple_table.replace_sort_order()._column_name_to_id(col_name)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("session_catalog_hive")])
def test_replace_sort_order(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple)
    simple_table.replace_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_FIRST).desc(
        "bar", IdentityTransform(), NullOrder.NULLS_LAST
    ).commit()
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.DESC, null_order=NullOrder.NULLS_LAST),
        order_id=1,
    )
