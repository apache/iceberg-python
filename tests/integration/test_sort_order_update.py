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
from pyiceberg.transforms import (
    IdentityTransform,
)


def _simple_table(catalog: Catalog, table_schema_simple: Schema, format_version: str) -> Table:
    return _create_table_with_schema(catalog, table_schema_simple, format_version)


def _create_table_with_schema(catalog: Catalog, schema: Schema, format_version: str) -> Table:
    tbl_name = "default.test_schema_evolution"
    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier=tbl_name, schema=schema, properties={"format-version": format_version})


@pytest.mark.integration
@pytest.mark.parametrize(
    "catalog, format_version",
    [
        (pytest.lazy_fixture("session_catalog"), "1"),
        (pytest.lazy_fixture("session_catalog_hive"), "1"),
        (pytest.lazy_fixture("session_catalog"), "2"),
        (pytest.lazy_fixture("session_catalog_hive"), "2"),
    ],
)
def test_map_column_name_to_id(catalog: Catalog, format_version: str, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple, format_version)
    for col_name, col_id in {"foo": 1, "bar": 2, "baz": 3}.items():
        assert col_id == simple_table.update_sort_order()._column_name_to_id(col_name)


@pytest.mark.integration
@pytest.mark.parametrize(
    "catalog, format_version",
    [
        (pytest.lazy_fixture("session_catalog"), "1"),
        (pytest.lazy_fixture("session_catalog_hive"), "1"),
        (pytest.lazy_fixture("session_catalog"), "2"),
        (pytest.lazy_fixture("session_catalog_hive"), "2"),
    ],
)
def test_update_sort_order(catalog: Catalog, format_version: str, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple, format_version)
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_FIRST).desc(
        "bar", IdentityTransform(), NullOrder.NULLS_LAST
    ).commit()
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.DESC, null_order=NullOrder.NULLS_LAST),
        order_id=1,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "catalog, format_version",
    [
        (pytest.lazy_fixture("session_catalog"), "1"),
        (pytest.lazy_fixture("session_catalog_hive"), "1"),
        (pytest.lazy_fixture("session_catalog"), "2"),
        (pytest.lazy_fixture("session_catalog_hive"), "2"),
    ],
)
def test_increment_existing_sort_order_id(catalog: Catalog, format_version: str, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple, format_version)
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_FIRST).commit()
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        order_id=1,
    )
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_LAST).desc(
        "bar", IdentityTransform(), NullOrder.NULLS_FIRST
    ).commit()
    assert (
        len(simple_table.sort_orders()) == 3
    )  # 0: empty sort order from creating tables, 1: first sort order, 2: second sort order
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST),
        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.DESC, null_order=NullOrder.NULLS_FIRST),
        order_id=2,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "catalog, format_version",
    [
        (pytest.lazy_fixture("session_catalog"), "1"),
        (pytest.lazy_fixture("session_catalog_hive"), "1"),
        (pytest.lazy_fixture("session_catalog"), "2"),
        (pytest.lazy_fixture("session_catalog_hive"), "2"),
    ],
)
def test_update_existing_sort_order(catalog: Catalog, format_version: str, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple, format_version)
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_FIRST).commit()
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        order_id=1,
    )
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_LAST).desc(
        "bar", IdentityTransform(), NullOrder.NULLS_FIRST
    ).commit()
    # Go back to the first sort order
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_FIRST).commit()
    assert (
        len(simple_table.sort_orders()) == 3
    )  # line 133 should not create a new sort order since it is the same as the first one
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        order_id=1,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "catalog, format_version",
    [
        (pytest.lazy_fixture("session_catalog"), "1"),
        (pytest.lazy_fixture("session_catalog_hive"), "1"),
        (pytest.lazy_fixture("session_catalog"), "2"),
        (pytest.lazy_fixture("session_catalog_hive"), "2"),
    ],
)
def test_update_existing_sort_order_with_unsorted_sort_order(
    catalog: Catalog, format_version: str, table_schema_simple: Schema
) -> None:
    simple_table = _simple_table(catalog, table_schema_simple, format_version)
    simple_table.update_sort_order().asc("foo", IdentityTransform(), NullOrder.NULLS_FIRST).commit()
    assert simple_table.sort_order() == SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        order_id=1,
    )
    # Table should now be unsorted
    simple_table.update_sort_order().commit()
    # Go back to the first sort order
    assert len(simple_table.sort_orders()) == 2
    assert simple_table.sort_order() == SortOrder(order_id=0)
