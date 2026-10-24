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

import time
from typing import Any

import pyarrow as pa
import pytest
from pytest_lazy_fixtures import lf

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import CommitFailedException, NoSuchViewError
from pyiceberg.schema import Schema
from pyiceberg.view.metadata import SQLViewRepresentation, ViewVersion

TEST_NAMESPACE_IDENTIFIER = "TEST NS"


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_namespace_exists(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_namespace_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_create_namespace_if_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_create_namespace_if_already_existing(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_load_view(catalog: RestCatalog, table_schema_nested: Schema, database_name: str, view_name: str) -> None:
    identifier = (database_name, view_name)
    if not catalog.namespace_exists(database_name):
        catalog.create_namespace(database_name)

    view_version = ViewVersion(
        version_id=1,
        schema_id=1,
        timestamp_ms=int(time.time() * 1000),
        summary={},
        representations=[
            SQLViewRepresentation(
                type="sql",
                sql="SELECT 1 as some_col",
                dialect="spark",
            )
        ],
        default_namespace=["default"],
    )
    view = catalog.create_view(identifier, table_schema_nested, view_version=view_version)
    loaded_view = catalog.load_view(identifier)
    assert view == loaded_view


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_load_view_with_table_ident(
    catalog: RestCatalog, table_name: str, table_schema_nested: Schema, database_name: str
) -> None:
    table_identifier = (database_name, table_name)
    if not catalog.namespace_exists(database_name):
        catalog.create_namespace(database_name)

    if not catalog.table_exists(table_identifier):
        catalog.create_table(table_identifier, table_schema_nested)

    assert catalog.table_exists(table_identifier)
    with pytest.raises(NoSuchViewError):
        catalog.load_view(table_identifier)


def _create_tables_for_multi_table_commit(catalog: RestCatalog, database_name: str) -> tuple[pa.Table, list[Any]]:
    if not catalog.namespace_exists(database_name):
        catalog.create_namespace(database_name)
    rows = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()), "name": pa.array(["a", "b", "c"], type=pa.string())})
    tables = []
    for name in ("orders", "items"):
        identifier = (database_name, name)
        if catalog.table_exists(identifier):
            catalog.drop_table(identifier)
        tables.append(catalog.create_table(identifier, schema=rows.schema))
    return rows, tables


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_commit_transactions_appends_to_tables_atomically(catalog: RestCatalog, database_name: str) -> None:
    rows, (orders, items) = _create_tables_for_multi_table_commit(catalog, database_name)

    orders_tx = orders.transaction()
    orders_tx.append(rows)
    items_tx = items.transaction()
    items_tx.append(rows)
    items_tx.set_properties(owner="sales")
    catalog.commit_transactions(orders_tx, items_tx)

    for table in (orders, items):
        assert table.current_snapshot() is not None
        assert table.scan().to_arrow().num_rows == 3
    assert items.properties["owner"] == "sales"


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_commit_transactions_applies_nothing_on_conflict(catalog: RestCatalog, database_name: str) -> None:
    rows, (orders, items) = _create_tables_for_multi_table_commit(catalog, database_name)
    orders_tx = orders.transaction()
    orders_tx.append(rows)
    items_tx = items.transaction()
    items_tx.append(rows)

    # Another writer commits to orders after the transactions were staged.
    catalog.load_table(orders.name()).append(rows)

    with pytest.raises(CommitFailedException):
        catalog.commit_transactions(orders_tx, items_tx)

    assert catalog.load_table(orders.name()).scan().to_arrow().num_rows == 3
    assert catalog.load_table(items.name()).current_snapshot() is None


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
@pytest.mark.parametrize("concurrent_write", [False, True])
@pytest.mark.filterwarnings("ignore:Delete operation did not match any records")
def test_commit_transactions_validates_a_delete_that_matched_nothing(
    catalog: RestCatalog, database_name: str, concurrent_write: bool
) -> None:
    rows, (orders, items) = _create_tables_for_multi_table_commit(catalog, database_name)
    orders.append(rows)
    orders_tx = orders.transaction()
    orders_tx.delete("id = 100")
    items_tx = items.transaction()
    items_tx.append(rows)
    assert not orders_tx._updates

    if concurrent_write:
        # Another writer commits to orders after the delete was planned.
        catalog.load_table(orders.name()).append(rows)
        with pytest.raises(CommitFailedException):
            catalog.commit_transactions(orders_tx, items_tx)
        assert catalog.load_table(items.name()).current_snapshot() is None
    else:
        catalog.commit_transactions(orders_tx, items_tx)
        assert catalog.load_table(items.name()).scan().to_arrow().num_rows == 3
