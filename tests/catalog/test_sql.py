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
from pathlib import Path
from typing import Generator, List

import pyarrow as pa
import pytest
from pytest import TempPathFactory
from pytest_lazyfixture import lazy_fixture
from sqlalchemy.exc import ArgumentError, IntegrityError

from pyiceberg.catalog import Identifier
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import FSSPEC_FILE_IO, PY_IO_IMPL
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType


@pytest.fixture(name="warehouse", scope="session")
def fixture_warehouse(tmp_path_factory: TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("test_sql")


@pytest.fixture(name="random_identifier")
def fixture_random_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    os.makedirs(f"{warehouse}/{database_name}.db/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name


@pytest.fixture(name="another_random_identifier")
def fixture_another_random_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    database_name = database_name + "_new"
    table_name = table_name + "_new"
    os.makedirs(f"{warehouse}/{database_name}.db/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name


@pytest.fixture(scope="module")
def catalog_memory(warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": "sqlite:///:memory:",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog("test_sql_catalog", **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite(warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog.db",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog("test_sql_catalog", **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite_without_rowcount(warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog.db",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog("test_sql_catalog", **props)
    catalog.engine.dialect.supports_sane_rowcount = False
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite_fsspec(warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog.db",
        "warehouse": f"file://{warehouse}",
        PY_IO_IMPL: FSSPEC_FILE_IO,
    }
    catalog = SqlCatalog("test_sql_catalog", **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


def test_creation_with_no_uri() -> None:
    with pytest.raises(NoSuchPropertyException):
        SqlCatalog("test_ddb_catalog", not_uri="unused")


def test_creation_with_unsupported_uri() -> None:
    with pytest.raises(ArgumentError):
        SqlCatalog("test_ddb_catalog", uri="unsupported:xxx")


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_tables_idempotency(catalog: SqlCatalog) -> None:
    # Second initialization should not fail even if tables are already created
    catalog.create_tables()
    catalog.create_tables()


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_table_default_sort_order(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"
    catalog.drop_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_table_with_pyarrow_schema(
    catalog: SqlCatalog,
    pyarrow_schema_simple_without_ids: pa.Schema,
    iceberg_table_schema_simple: Schema,
    random_identifier: Identifier,
) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, pyarrow_schema_simple_without_ids)
    assert table.schema() == iceberg_table_schema_simple
    catalog.drop_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_table_custom_sort_order(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    order = SortOrder(SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_FIRST))
    table = catalog.create_table(random_identifier, table_schema_nested, sort_order=order)
    given_sort_order = table.sort_order()
    assert given_sort_order.order_id == 1, "Order ID must match"
    assert len(given_sort_order.fields) == 1, "Order must have 1 field"
    assert given_sort_order.fields[0].direction == SortDirection.ASC, "Direction must match"
    assert given_sort_order.fields[0].null_order == NullOrder.NULLS_FIRST, "Null order must match"
    assert isinstance(given_sort_order.fields[0].transform, IdentityTransform), "Transform must match"
    catalog.drop_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_table_with_default_warehouse_location(
    warehouse: Path, catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier
) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    catalog.create_table(random_identifier, table_schema_nested)
    table = catalog.load_table(random_identifier)
    assert table.identifier == (catalog.name,) + random_identifier
    assert table.metadata_location.startswith(f"file://{warehouse}")
    assert os.path.exists(table.metadata_location[len("file://") :])
    catalog.drop_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_duplicated_table(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    catalog.create_table(random_identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table(random_identifier, table_schema_nested)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_table_with_non_existing_namespace(catalog: SqlCatalog, table_schema_nested: Schema, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(identifier, table_schema_nested)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_table_without_namespace(catalog: SqlCatalog, table_schema_nested: Schema, table_name: str) -> None:
    with pytest.raises(ValueError):
        catalog.create_table(table_name, table_schema_nested)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_register_table(catalog: SqlCatalog, random_identifier: Identifier, metadata_location: str) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.register_table(random_identifier, metadata_location)
    assert table.identifier == (catalog.name,) + random_identifier
    assert table.metadata_location == metadata_location
    assert os.path.exists(metadata_location)
    catalog.drop_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_register_existing_table(catalog: SqlCatalog, random_identifier: Identifier, metadata_location: str) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    catalog.register_table(random_identifier, metadata_location)
    with pytest.raises(TableAlreadyExistsError):
        catalog.register_table(random_identifier, metadata_location)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_register_table_with_non_existing_namespace(catalog: SqlCatalog, metadata_location: str, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        catalog.register_table(identifier, metadata_location)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_register_table_without_namespace(catalog: SqlCatalog, metadata_location: str, table_name: str) -> None:
    with pytest.raises(ValueError):
        catalog.register_table(table_name, metadata_location)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_load_table(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    loaded_table = catalog.load_table(random_identifier)
    assert table.identifier == loaded_table.identifier
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_load_table_from_self_identifier(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    intermediate = catalog.load_table(random_identifier)
    assert intermediate.identifier == (catalog.name,) + random_identifier
    loaded_table = catalog.load_table(intermediate.identifier)
    assert table.identifier == loaded_table.identifier
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_drop_table(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (catalog.name,) + random_identifier
    catalog.drop_table(random_identifier)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_drop_table_from_self_identifier(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (catalog.name,) + random_identifier
    catalog.drop_table(table.identifier)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.identifier)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_drop_table_that_does_not_exist(catalog: SqlCatalog, random_identifier: Identifier) -> None:
    with pytest.raises(NoSuchTableError):
        catalog.drop_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_rename_table(
    catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    to_database_name, _to_table_name = another_random_identifier
    catalog.create_namespace(from_database_name)
    catalog.create_namespace(to_database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (catalog.name,) + random_identifier
    catalog.rename_table(random_identifier, another_random_identifier)
    new_table = catalog.load_table(another_random_identifier)
    assert new_table.identifier == (catalog.name,) + another_random_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        catalog.load_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_rename_table_from_self_identifier(
    catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    to_database_name, _to_table_name = another_random_identifier
    catalog.create_namespace(from_database_name)
    catalog.create_namespace(to_database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (catalog.name,) + random_identifier
    catalog.rename_table(table.identifier, another_random_identifier)
    new_table = catalog.load_table(another_random_identifier)
    assert new_table.identifier == (catalog.name,) + another_random_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.identifier)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_rename_table_to_existing_one(
    catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    to_database_name, _to_table_name = another_random_identifier
    catalog.create_namespace(from_database_name)
    catalog.create_namespace(to_database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (catalog.name,) + random_identifier
    new_table = catalog.create_table(another_random_identifier, table_schema_nested)
    assert new_table.identifier == (catalog.name,) + another_random_identifier
    with pytest.raises(TableAlreadyExistsError):
        catalog.rename_table(random_identifier, another_random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_rename_missing_table(catalog: SqlCatalog, random_identifier: Identifier, another_random_identifier: Identifier) -> None:
    to_database_name, _to_table_name = another_random_identifier
    catalog.create_namespace(to_database_name)
    with pytest.raises(NoSuchTableError):
        catalog.rename_table(random_identifier, another_random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_rename_table_to_missing_namespace(
    catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    catalog.create_namespace(from_database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (catalog.name,) + random_identifier
    with pytest.raises(NoSuchNamespaceError):
        catalog.rename_table(random_identifier, another_random_identifier)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_list_tables(
    catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    database_name_1, _table_name_1 = random_identifier
    database_name_2, _table_name_2 = another_random_identifier
    catalog.create_namespace(database_name_1)
    catalog.create_namespace(database_name_2)
    catalog.create_table(random_identifier, table_schema_nested)
    catalog.create_table(another_random_identifier, table_schema_nested)
    identifier_list = catalog.list_tables(database_name_1)
    assert len(identifier_list) == 1
    assert random_identifier in identifier_list

    identifier_list = catalog.list_tables(database_name_2)
    assert len(identifier_list) == 1
    assert another_random_identifier in identifier_list


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_namespace(catalog: SqlCatalog, database_name: str) -> None:
    catalog.create_namespace(database_name)
    assert (database_name,) in catalog.list_namespaces()


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_duplicate_namespace(catalog: SqlCatalog, database_name: str) -> None:
    catalog.create_namespace(database_name)
    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace(database_name)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_namespaces_sharing_same_prefix(catalog: SqlCatalog, database_name: str) -> None:
    catalog.create_namespace(database_name + "_1")
    # Second namespace is a prefix of the first one, make sure it can be added.
    catalog.create_namespace(database_name)


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_create_namespace_with_comment_and_location(catalog: SqlCatalog, database_name: str) -> None:
    test_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = catalog.list_namespaces()
    assert (database_name,) in loaded_database_list
    properties = catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
@pytest.mark.filterwarnings("ignore")
def test_create_namespace_with_null_properties(catalog: SqlCatalog, database_name: str) -> None:
    with pytest.raises(IntegrityError):
        catalog.create_namespace(namespace=database_name, properties={None: "value"})  # type: ignore

    with pytest.raises(IntegrityError):
        catalog.create_namespace(namespace=database_name, properties={"key": None})  # type: ignore


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_list_namespaces(catalog: SqlCatalog, database_list: List[str]) -> None:
    for database_name in database_list:
        catalog.create_namespace(database_name)
    db_list = catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in db_list
        assert len(catalog.list_namespaces(database_name)) == 1


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_list_non_existing_namespaces(catalog: SqlCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_namespaces("does_not_exist")


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_drop_namespace(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, table_name = random_identifier
    catalog.create_namespace(database_name)
    assert (database_name,) in catalog.list_namespaces()
    catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace(database_name)
    catalog.drop_table((database_name, table_name))
    catalog.drop_namespace(database_name)
    assert (database_name,) not in catalog.list_namespaces()


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_load_namespace_properties(catalog: SqlCatalog, database_name: str) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }

    catalog.create_namespace(database_name, test_properties)
    listed_properties = catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_load_empty_namespace_properties(catalog: SqlCatalog, database_name: str) -> None:
    catalog.create_namespace(database_name)
    listed_properties = catalog.load_namespace_properties(database_name)
    assert listed_properties == {"exists": "true"}


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
    ],
)
def test_update_namespace_properties(catalog: SqlCatalog, database_name: str) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    catalog.create_namespace(database_name, test_properties)
    update_report = catalog.update_namespace_properties(database_name, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == catalog.load_namespace_properties(database_name)["comment"]


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_commit_table(catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_nested)

    assert catalog._parse_metadata_version(table.metadata_location) == 0
    assert table.metadata.current_schema_id == 0

    transaction = table.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()
    transaction.commit_transaction()

    updated_table_metadata = table.metadata

    assert catalog._parse_metadata_version(table.metadata_location) == 1
    assert updated_table_metadata.current_schema_id == 1
    assert len(updated_table_metadata.schemas) == 2
    new_schema = next(schema for schema in updated_table_metadata.schemas if schema.schema_id == 1)
    assert new_schema
    assert new_schema == update._apply()
    assert new_schema.find_field("b").field_type == IntegerType()


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
        lazy_fixture('catalog_sqlite_fsspec'),
    ],
)
def test_append_table(catalog: SqlCatalog, table_schema_simple: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table = catalog.create_table(random_identifier, table_schema_simple)

    df = pa.Table.from_pydict(
        {
            "foo": ["a"],
            "bar": [1],
            "baz": [True],
        },
        schema=schema_to_pyarrow(table_schema_simple),
    )

    table.append(df)

    # new snapshot is written in APPEND mode
    assert len(table.metadata.snapshots) == 1
    assert table.metadata.snapshots[0].snapshot_id == table.metadata.current_snapshot_id
    assert table.metadata.snapshots[0].parent_snapshot_id is None
    assert table.metadata.snapshots[0].sequence_number == 1
    assert table.metadata.snapshots[0].summary is not None
    assert table.metadata.snapshots[0].summary.operation == Operation.APPEND
    assert table.metadata.snapshots[0].summary['added-data-files'] == '1'
    assert table.metadata.snapshots[0].summary['added-records'] == '1'
    assert table.metadata.snapshots[0].summary['total-data-files'] == '1'
    assert table.metadata.snapshots[0].summary['total-records'] == '1'

    # read back the data
    assert df == table.scan().to_arrow()


@pytest.mark.parametrize(
    'catalog',
    [
        lazy_fixture('catalog_memory'),
        lazy_fixture('catalog_sqlite'),
        lazy_fixture('catalog_sqlite_without_rowcount'),
    ],
)
def test_concurrent_commit_table(catalog: SqlCatalog, table_schema_simple: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    catalog.create_namespace(database_name)
    table_a = catalog.create_table(random_identifier, table_schema_simple)
    table_b = catalog.load_table(random_identifier)

    with table_a.update_schema() as update:
        update.add_column(path="b", field_type=IntegerType())

    with pytest.raises(CommitFailedException, match="Requirement failed: current schema id has changed: expected 0, found 1"):
        # This one should fail since it already has been updated
        with table_b.update_schema() as update:
            update.add_column(path="c", field_type=IntegerType())
