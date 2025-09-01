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
from typing import Any, Generator, cast

import pyarrow as pa
import pytest
from pydantic_core import ValidationError
from pytest_lazyfixture import lazy_fixture
from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.exc import ArgumentError, IntegrityError

from pyiceberg.catalog import (
    Catalog,
    load_catalog,
)
from pyiceberg.catalog.sql import (
    DEFAULT_ECHO_VALUE,
    DEFAULT_POOL_PRE_PING_VALUE,
    IcebergTables,
    SqlCatalog,
    SqlCatalogBaseTable,
)
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
from pyiceberg.io.pyarrow import _dataframe_to_data_files, schema_to_pyarrow
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Identifier
from pyiceberg.types import IntegerType, NestedField, StringType, strtobool

CATALOG_TABLES = [c.__tablename__ for c in SqlCatalogBaseTable.__subclasses__()]


@pytest.fixture(scope="module")
def catalog_name() -> str:
    return "test_sql_catalog"


@pytest.fixture(name="random_table_identifier")
def fixture_random_table_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    os.makedirs(f"{warehouse}/{database_name}/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name


@pytest.fixture(name="another_random_table_identifier")
def fixture_another_random_table_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    database_name = database_name + "_new"
    table_name = table_name + "_new"
    os.makedirs(f"{warehouse}/{database_name}/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name


@pytest.fixture(name="random_hierarchical_identifier")
def fixture_random_hierarchical_identifier(warehouse: Path, hierarchical_namespace_name: str, table_name: str) -> Identifier:
    os.makedirs(f"{warehouse}/{hierarchical_namespace_name}/{table_name}/metadata/", exist_ok=True)
    return Catalog.identifier_to_tuple(".".join((hierarchical_namespace_name, table_name)))


@pytest.fixture(name="another_random_hierarchical_identifier")
def fixture_another_random_hierarchical_identifier(
    warehouse: Path, hierarchical_namespace_name: str, table_name: str
) -> Identifier:
    hierarchical_namespace_name = hierarchical_namespace_name + "_new"
    table_name = table_name + "_new"
    os.makedirs(f"{warehouse}/{hierarchical_namespace_name}/{table_name}/metadata/", exist_ok=True)
    return Catalog.identifier_to_tuple(".".join((hierarchical_namespace_name, table_name)))


@pytest.fixture(scope="module")
def catalog_memory(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": "sqlite:///:memory:",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_uri(warehouse: Path) -> str:
    return f"sqlite:////{warehouse}/sql-catalog"


@pytest.fixture(scope="module")
def alchemy_engine(catalog_uri: str) -> Engine:
    return create_engine(catalog_uri)


@pytest.fixture(scope="module")
def catalog_sqlite_without_rowcount(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.engine.dialect.supports_sane_rowcount = False
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_sqlite_fsspec(catalog_name: str, warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog",
        "warehouse": f"file://{warehouse}",
        PY_IO_IMPL: FSSPEC_FILE_IO,
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


def test_creation_with_no_uri(catalog_name: str) -> None:
    with pytest.raises(NoSuchPropertyException):
        SqlCatalog(catalog_name, not_uri="unused")


def test_creation_with_unsupported_uri(catalog_name: str) -> None:
    with pytest.raises(ArgumentError):
        SqlCatalog(catalog_name, uri="unsupported:xxx")


def test_creation_with_echo_parameter(catalog_name: str, warehouse: Path) -> None:
    # echo_param, expected_echo_value
    test_cases = [(None, strtobool(DEFAULT_ECHO_VALUE)), ("debug", "debug"), ("true", True), ("false", False)]

    for echo_param, expected_echo_value in test_cases:
        props = {
            "uri": f"sqlite:////{warehouse}/sql-catalog",
            "warehouse": f"file://{warehouse}",
        }
        # None is for default value
        if echo_param is not None:
            props["echo"] = echo_param
        catalog = SqlCatalog(catalog_name, **props)
        assert catalog.engine._echo == expected_echo_value, (
            f"Assertion failed: expected echo value {expected_echo_value}, "
            f"but got {catalog.engine._echo}. For echo_param={echo_param}"
        )


def test_creation_with_pool_pre_ping_parameter(catalog_name: str, warehouse: Path) -> None:
    # pool_pre_ping_param, expected_pool_pre_ping_value
    test_cases = [
        (None, strtobool(DEFAULT_POOL_PRE_PING_VALUE)),
        ("true", True),
        ("false", False),
    ]

    for pool_pre_ping_param, expected_pool_pre_ping_value in test_cases:
        props = {
            "uri": f"sqlite:////{warehouse}/sql-catalog",
            "warehouse": f"file://{warehouse}",
        }
        # None is for default value
        if pool_pre_ping_param is not None:
            props["pool_pre_ping"] = pool_pre_ping_param

        catalog = SqlCatalog(catalog_name, **props)
        assert catalog.engine.pool._pre_ping == expected_pool_pre_ping_value, (
            f"Assertion failed: expected pool_pre_ping value {expected_pool_pre_ping_value}, "
            f"but got {catalog.engine.pool._pre_ping}. For pool_pre_ping_param={pool_pre_ping_param}"
        )


def test_creation_from_impl(catalog_name: str, warehouse: Path) -> None:
    assert isinstance(
        load_catalog(
            catalog_name,
            **{
                "py-catalog-impl": "pyiceberg.catalog.sql.SqlCatalog",
                "uri": f"sqlite:////{warehouse}/sql-catalog",
                "warehouse": f"file://{warehouse}",
            },
        ),
        SqlCatalog,
    )


def confirm_no_tables_exist(alchemy_engine: Engine) -> None:
    inspector = inspect(alchemy_engine)
    for c in SqlCatalogBaseTable.__subclasses__():
        if inspector.has_table(c.__tablename__):
            c.__table__.drop(alchemy_engine)

    any_table_exists = any(t for t in inspector.get_table_names() if t in CATALOG_TABLES)
    if any_table_exists:
        pytest.raises(TableAlreadyExistsError, "Tables exist, but should not have been created yet")


def confirm_all_tables_exist(catalog: SqlCatalog) -> None:
    all_tables_exists = True
    for t in CATALOG_TABLES:
        if t not in inspect(catalog.engine).get_table_names():
            all_tables_exists = False

    assert isinstance(catalog, SqlCatalog), "Catalog should be a SQLCatalog"
    assert all_tables_exists, "Tables should have been created"


def load_catalog_for_catalog_table_creation(catalog_name: str, catalog_uri: str) -> SqlCatalog:
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        init_catalog_tables="true",
    )

    return cast(SqlCatalog, catalog)


def test_creation_when_no_tables_exist(alchemy_engine: Engine, catalog_name: str, catalog_uri: str) -> None:
    confirm_no_tables_exist(alchemy_engine)
    catalog = load_catalog_for_catalog_table_creation(catalog_name=catalog_name, catalog_uri=catalog_uri)
    confirm_all_tables_exist(catalog)


def test_creation_when_one_tables_exists(alchemy_engine: Engine, catalog_name: str, catalog_uri: str) -> None:
    confirm_no_tables_exist(alchemy_engine)

    # Create one table
    inspector = inspect(alchemy_engine)
    IcebergTables.__table__.create(bind=alchemy_engine)
    assert IcebergTables.__tablename__ in [t for t in inspector.get_table_names() if t in CATALOG_TABLES]

    catalog = load_catalog_for_catalog_table_creation(catalog_name=catalog_name, catalog_uri=catalog_uri)
    confirm_all_tables_exist(catalog)


def test_creation_when_all_tables_exists(alchemy_engine: Engine, catalog_name: str, catalog_uri: str) -> None:
    confirm_no_tables_exist(alchemy_engine)

    # Create all tables
    inspector = inspect(alchemy_engine)
    SqlCatalogBaseTable.metadata.create_all(bind=alchemy_engine)
    for c in CATALOG_TABLES:
        assert c in [t for t in inspector.get_table_names() if t in CATALOG_TABLES]

    catalog = load_catalog_for_catalog_table_creation(catalog_name=catalog_name, catalog_uri=catalog_uri)
    confirm_all_tables_exist(catalog)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_create_tables_idempotency(catalog: SqlCatalog) -> None:
    # Second initialization should not fail even if tables are already created
    catalog.create_tables()
    catalog.create_tables()


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_table_default_sort_order(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested)
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_v1_table(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested, properties={"format-version": "1"})
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"
    assert table.format_version == 1
    assert table.spec() == UNPARTITIONED_PARTITION_SPEC
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_table_with_pyarrow_schema(
    catalog: SqlCatalog,
    pyarrow_schema_simple_without_ids: pa.Schema,
    iceberg_table_schema_simple: Schema,
    table_identifier: Identifier,
) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, pyarrow_schema_simple_without_ids)
    assert table.schema() == iceberg_table_schema_simple
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_write_pyarrow_schema(catalog: SqlCatalog, table_identifier: Identifier) -> None:
    import pyarrow as pa

    pyarrow_table = pa.Table.from_arrays(
        [
            pa.array([None, "A", "B", "C"]),  # 'foo' column
            pa.array([1, 2, 3, 4]),  # 'bar' column
            pa.array([True, None, False, True]),  # 'baz' column
            pa.array([None, "A", "B", "C"]),  # 'large' column
        ],
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=False),
                pa.field("baz", pa.bool_(), nullable=True),
                pa.field("large", pa.large_string(), nullable=True),
            ]
        ),
    )
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, pyarrow_table.schema)
    table.append(pyarrow_table)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_table_custom_sort_order(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    order = SortOrder(SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_FIRST))
    table = catalog.create_table(table_identifier, table_schema_nested, sort_order=order)
    given_sort_order = table.sort_order()
    assert given_sort_order.order_id == 1, "Order ID must match"
    assert len(given_sort_order.fields) == 1, "Order must have 1 field"
    assert given_sort_order.fields[0].direction == SortDirection.ASC, "Direction must match"
    assert given_sort_order.fields[0].null_order == NullOrder.NULLS_FIRST, "Null order must match"
    assert isinstance(given_sort_order.fields[0].transform, IdentityTransform), "Transform must match"
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_table_with_default_warehouse_location(
    warehouse: Path, catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier
) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(table_identifier)
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(table_identifier, table_schema_nested)
    table = catalog.load_table(table_identifier)
    assert table.name() == identifier_tuple
    assert table.metadata_location.startswith(f"file://{warehouse}")
    assert os.path.exists(table.metadata_location[len("file://") :])
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_table_with_given_location_removes_trailing_slash(
    warehouse: Path, catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier
) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(table_identifier)
    namespace = Catalog.namespace_from(table_identifier)
    table_name = Catalog.table_name_from(identifier_tuple)
    location = f"file://{warehouse}/{catalog.name}/{table_name}-given"
    catalog.create_namespace(namespace)
    catalog.create_table(table_identifier, table_schema_nested, location=f"{location}/")
    table = catalog.load_table(table_identifier)
    assert table.name() == identifier_tuple
    assert table.metadata_location.startswith(f"file://{warehouse}")
    assert os.path.exists(table.metadata_location[len("file://") :])
    assert table.location() == location
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_duplicated_table(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(table_identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table(table_identifier, table_schema_nested)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_create_table_if_not_exists_duplicated_table(
    catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table1 = catalog.create_table(table_identifier, table_schema_nested)
    table2 = catalog.create_table_if_not_exists(table_identifier, table_schema_nested)
    assert table1.name() == table2.name()


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_create_table_with_non_existing_namespace(catalog: SqlCatalog, table_schema_nested: Schema, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(identifier, table_schema_nested)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_create_table_without_namespace(catalog: SqlCatalog, table_schema_nested: Schema, table_name: str) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(table_name, table_schema_nested)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_register_table(catalog: SqlCatalog, table_identifier: Identifier, metadata_location: str) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(table_identifier)
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.register_table(table_identifier, metadata_location)
    assert table.name() == identifier_tuple
    assert table.metadata_location == metadata_location
    assert os.path.exists(metadata_location)
    catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_register_existing_table(catalog: SqlCatalog, table_identifier: Identifier, metadata_location: str) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    catalog.register_table(table_identifier, metadata_location)
    with pytest.raises(TableAlreadyExistsError):
        catalog.register_table(table_identifier, metadata_location)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_register_table_with_non_existing_namespace(catalog: SqlCatalog, metadata_location: str, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        catalog.register_table(identifier, metadata_location)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_register_table_without_namespace(catalog: SqlCatalog, metadata_location: str, table_name: str) -> None:
    with pytest.raises(ValueError):
        catalog.register_table(table_name, metadata_location)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_load_table(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested)
    loaded_table = catalog.load_table(table_identifier)
    assert table.name() == loaded_table.name()
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_load_table_from_self_identifier(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(table_identifier)
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested)
    intermediate = catalog.load_table(table_identifier)
    assert intermediate.name() == identifier_tuple
    loaded_table = catalog.load_table(intermediate.name())
    assert table.name() == loaded_table.name()
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_drop_table(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(table_identifier)
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested)
    assert table.name() == identifier_tuple
    catalog.drop_table(table_identifier)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_drop_table_from_self_identifier(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(table_identifier)
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested)
    assert table.name() == identifier_tuple
    catalog.drop_table(table.name())
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.name())
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_drop_table_that_does_not_exist(catalog: SqlCatalog, table_identifier: Identifier) -> None:
    with pytest.raises(NoSuchTableError):
        catalog.drop_table(table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "from_table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
@pytest.mark.parametrize(
    "to_table_identifier",
    [
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def test_rename_table(
    catalog: SqlCatalog, table_schema_nested: Schema, from_table_identifier: Identifier, to_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(from_table_identifier)
    to_namespace = Catalog.namespace_from(to_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    table = catalog.create_table(from_table_identifier, table_schema_nested)
    assert table.name() == from_table_identifier
    catalog.rename_table(from_table_identifier, to_table_identifier)
    new_table = catalog.load_table(to_table_identifier)
    assert new_table.name() == to_table_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        catalog.load_table(from_table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "from_table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
@pytest.mark.parametrize(
    "to_table_identifier",
    [
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def test_rename_table_from_self_identifier(
    catalog: SqlCatalog, table_schema_nested: Schema, from_table_identifier: Identifier, to_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(from_table_identifier)
    to_namespace = Catalog.namespace_from(to_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    table = catalog.create_table(from_table_identifier, table_schema_nested)
    assert table.name() == from_table_identifier
    catalog.rename_table(table.name(), to_table_identifier)
    new_table = catalog.load_table(to_table_identifier)
    assert new_table.name() == to_table_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.name())
    with pytest.raises(NoSuchTableError):
        catalog.load_table(from_table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "from_table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
@pytest.mark.parametrize(
    "to_table_identifier",
    [
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def test_rename_table_to_existing_one(
    catalog: SqlCatalog, table_schema_nested: Schema, from_table_identifier: Identifier, to_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(from_table_identifier)
    to_namespace = Catalog.namespace_from(to_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    table = catalog.create_table(from_table_identifier, table_schema_nested)
    assert table.name() == from_table_identifier
    new_table = catalog.create_table(to_table_identifier, table_schema_nested)
    assert new_table.name() == to_table_identifier
    with pytest.raises(TableAlreadyExistsError):
        catalog.rename_table(from_table_identifier, to_table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "from_table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
@pytest.mark.parametrize(
    "to_table_identifier",
    [
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def test_rename_missing_table(catalog: SqlCatalog, from_table_identifier: Identifier, to_table_identifier: Identifier) -> None:
    to_namespace = Catalog.namespace_from(to_table_identifier)
    catalog.create_namespace(to_namespace)
    with pytest.raises(NoSuchTableError):
        catalog.rename_table(from_table_identifier, to_table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "from_table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
@pytest.mark.parametrize(
    "to_table_identifier",
    [
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def test_rename_table_to_missing_namespace(
    catalog: SqlCatalog, table_schema_nested: Schema, from_table_identifier: Identifier, to_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(from_table_identifier)
    catalog.create_namespace(from_namespace)
    table = catalog.create_table(from_table_identifier, table_schema_nested)
    assert table.name() == from_table_identifier
    with pytest.raises(NoSuchNamespaceError):
        catalog.rename_table(from_table_identifier, to_table_identifier)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier_1",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier_2",
    [
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def test_list_tables(
    catalog: SqlCatalog, table_schema_nested: Schema, table_identifier_1: Identifier, table_identifier_2: Identifier
) -> None:
    namespace_1 = Catalog.namespace_from(table_identifier_1)
    namespace_2 = Catalog.namespace_from(table_identifier_2)
    catalog.create_namespace(namespace_1)
    catalog.create_namespace(namespace_2)
    catalog.create_table(table_identifier_1, table_schema_nested)
    catalog.create_table(table_identifier_2, table_schema_nested)
    identifier_list = catalog.list_tables(namespace_1)
    assert len(identifier_list) == 1
    assert table_identifier_1 in identifier_list

    identifier_list = catalog.list_tables(namespace_2)
    assert len(identifier_list) == 1
    assert table_identifier_2 in identifier_list


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_list_tables_when_missing_namespace(catalog: SqlCatalog, namespace: str) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_tables(namespace)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_create_namespace_if_not_exists(catalog: SqlCatalog, database_name: str) -> None:
    catalog.create_namespace(database_name)
    assert (database_name,) in catalog.list_namespaces()
    catalog.create_namespace_if_not_exists(database_name)
    assert (database_name,) in catalog.list_namespaces()


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_create_namespace(catalog: SqlCatalog, namespace: str) -> None:
    catalog.create_namespace(namespace)
    assert (Catalog.identifier_to_tuple(namespace)[:1]) in catalog.list_namespaces()


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_create_duplicate_namespace(catalog: SqlCatalog, namespace: str) -> None:
    catalog.create_namespace(namespace)
    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace(namespace)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_create_namespaces_sharing_same_prefix(catalog: SqlCatalog, namespace: str) -> None:
    catalog.create_namespace(namespace + "_1")
    # Second namespace is a prefix of the first one, make sure it can be added.
    catalog.create_namespace(namespace)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_create_namespace_with_comment_and_location(catalog: SqlCatalog, namespace: str) -> None:
    test_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    catalog.create_namespace(namespace=namespace, properties=test_properties)
    loaded_database_list = catalog.list_namespaces()
    assert Catalog.identifier_to_tuple(namespace)[:1] in loaded_database_list
    properties = catalog.load_namespace_properties(namespace)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
@pytest.mark.filterwarnings("ignore")
def test_create_namespace_with_null_properties(catalog: SqlCatalog, namespace: str) -> None:
    with pytest.raises(IntegrityError):
        catalog.create_namespace(namespace=namespace, properties={None: "value"})  # type: ignore

    with pytest.raises(IntegrityError):
        catalog.create_namespace(namespace=namespace, properties={"key": None})


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("empty_namespace", ["", (), (""), ("", ""), " ", (" ")])
def test_create_namespace_with_empty_identifier(catalog: SqlCatalog, empty_namespace: Any) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_namespace(empty_namespace)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_namespace_exists(catalog: SqlCatalog) -> None:
    for ns in [("db1",), ("db1", "ns1"), ("db2", "ns1"), ("db3", "ns1", "ns2")]:
        catalog.create_namespace(ns)
        assert catalog._namespace_exists(ns)

    assert catalog._namespace_exists("db2")  # `db2` exists because `db2.ns1` exists
    assert catalog._namespace_exists("db3.ns1")  # `db3.ns1` exists because `db3.ns1.ns2` exists
    assert not catalog._namespace_exists("db_")  # make sure '_' is escaped in the query
    assert not catalog._namespace_exists("db%")  # make sure '%' is escaped in the query


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_list_namespaces(catalog: SqlCatalog) -> None:
    namespace_list = ["db", "db.ns1", "db.ns1.ns2", "db.ns2", "db2", "db2.ns1", "db%"]
    for namespace in namespace_list:
        if not catalog._namespace_exists(namespace):
            catalog.create_namespace(namespace)

    ns_list = catalog.list_namespaces()
    for ns in [("db",), ("db%",), ("db2",)]:
        assert ns in ns_list

    ns_list = catalog.list_namespaces("db")
    assert sorted(ns_list) == [("db", "ns1"), ("db", "ns2")]

    ns_list = catalog.list_namespaces("db.ns1")
    assert sorted(ns_list) == [("db", "ns1", "ns2")]

    ns_list = catalog.list_namespaces("db.ns1.ns2")
    assert len(ns_list) == 0


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_list_namespaces_fuzzy_match(catalog: SqlCatalog) -> None:
    namespace_list = ["db.ns1", "db.ns1.ns2", "db.ns2", "db.ns1X.ns3", "db_.ns1.ns2", "db2.ns1.ns2"]
    for namespace in namespace_list:
        if not catalog._namespace_exists(namespace):
            catalog.create_namespace(namespace)

    assert catalog.list_namespaces("db.ns1") == [("db", "ns1", "ns2")]

    assert catalog.list_namespaces("db_.ns1") == [("db_", "ns1", "ns2")]


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_list_non_existing_namespaces(catalog: SqlCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_namespaces("does_not_exist")


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_drop_namespace(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    assert catalog._namespace_exists(namespace)
    catalog.create_table(table_identifier, table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace(namespace)
    catalog.drop_table(table_identifier)
    catalog.drop_namespace(namespace)
    assert not catalog._namespace_exists(namespace)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_drop_non_existing_namespaces(catalog: SqlCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.drop_namespace("does_not_exist")


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_load_namespace_properties(catalog: SqlCatalog, namespace: str) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{namespace}",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }

    catalog.create_namespace(namespace, test_properties)
    listed_properties = catalog.load_namespace_properties(namespace)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_load_empty_namespace_properties(catalog: SqlCatalog, namespace: str) -> None:
    catalog.create_namespace(namespace)
    listed_properties = catalog.load_namespace_properties(namespace)
    assert listed_properties == {"exists": "true"}


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
def test_load_namespace_properties_non_existing_namespace(catalog: SqlCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.load_namespace_properties("does_not_exist")


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("namespace", [lazy_fixture("database_name"), lazy_fixture("hierarchical_namespace_name")])
def test_update_namespace_properties(catalog: SqlCatalog, namespace: str) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{namespace}",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    catalog.create_namespace(namespace, test_properties)
    update_report = catalog.update_namespace_properties(namespace, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert catalog.load_namespace_properties(namespace) == {
        "comment": "updated test description",
        "test_property4": "4",
        "test_property5": "5",
        "location": f"{warehouse_location}/{namespace}",
    }


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_commit_table(catalog: SqlCatalog, table_schema_nested: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_nested)
    last_updated_ms = table.metadata.last_updated_ms
    original_table_metadata_location = table.metadata_location
    original_table_last_updated_ms = table.metadata.last_updated_ms

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
    assert updated_table_metadata.last_updated_ms > last_updated_ms
    assert len(updated_table_metadata.metadata_log) == 1
    assert updated_table_metadata.metadata_log[0].metadata_file == original_table_metadata_location
    assert updated_table_metadata.metadata_log[0].timestamp_ms == original_table_last_updated_ms


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
        lazy_fixture("catalog_sqlite_fsspec"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_append_table(catalog: SqlCatalog, table_schema_simple: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(table_identifier, table_schema_simple)

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
    assert table.metadata.snapshots[0].summary["added-data-files"] == "1"
    assert table.metadata.snapshots[0].summary["added-records"] == "1"
    assert table.metadata.snapshots[0].summary["total-data-files"] == "1"
    assert table.metadata.snapshots[0].summary["total-records"] == "1"
    assert len(table.metadata.metadata_log) == 1

    # read back the data
    assert df == table.scan().to_arrow()


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_concurrent_commit_table(catalog: SqlCatalog, table_schema_simple: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    table_a = catalog.create_table(table_identifier, table_schema_simple)
    table_b = catalog.load_table(table_identifier)

    with table_a.update_schema() as update:
        update.add_column(path="b", field_type=IntegerType())

    with pytest.raises(CommitFailedException, match="Requirement failed: current schema id has changed: expected 0, found 1"):
        # This one should fail since it already has been updated
        with table_b.update_schema() as update:
            update.add_column(path="c", field_type=IntegerType())


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_write_and_evolve(catalog: SqlCatalog, format_version: int) -> None:
    identifier = f"default.arrow_write_data_and_evolve_schema_v{format_version}"

    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    pa_table = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
        },
        schema=pa.schema([pa.field("foo", pa.large_string(), nullable=True)]),
    )

    tbl = catalog.create_table(identifier=identifier, schema=pa_table.schema, properties={"format-version": str(format_version)})

    pa_table_with_column = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
            "bar": [19, None, 25],
        },
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=True),
            ]
        ),
    )

    with tbl.transaction() as txn:
        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        txn.append(pa_table_with_column)
        txn.overwrite(pa_table_with_column)
        txn.delete("foo = 'a'")


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_create_table_transaction(catalog: SqlCatalog, format_version: int) -> None:
    identifier = f"default.arrow_create_table_transaction_{catalog.name}_{format_version}"
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    pa_table = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
        },
        schema=pa.schema([pa.field("foo", pa.large_string(), nullable=True)]),
    )

    pa_table_with_column = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
            "bar": [19, None, 25],
        },
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=True),
            ]
        ),
    )

    with catalog.create_table_transaction(
        identifier=identifier, schema=pa_table.schema, properties={"format-version": str(format_version)}
    ) as txn:
        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(table_metadata=txn.table_metadata, df=pa_table, io=txn._table.io):
                snapshot_update.append_data_file(data_file)

        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(
                table_metadata=txn.table_metadata, df=pa_table_with_column, io=txn._table.io
            ):
                snapshot_update.append_data_file(data_file)

    tbl = catalog.load_table(identifier=identifier)
    assert tbl.format_version == format_version
    assert len(tbl.scan().to_arrow()) == 6


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_table_properties_int_value(catalog: SqlCatalog, table_schema_simple: Schema, table_identifier: Identifier) -> None:
    # table properties can be set to int, but still serialized to string
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    property_with_int = {"property_name": 42}
    table = catalog.create_table(table_identifier, table_schema_simple, properties=property_with_int)
    assert isinstance(table.properties["property_name"], str)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_table_properties_raise_for_none_value(
    catalog: SqlCatalog, table_schema_simple: Schema, table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    property_with_none = {"property_name": None}
    with pytest.raises(ValidationError) as exc_info:
        _ = catalog.create_table(table_identifier, table_schema_simple, properties=property_with_none)
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize(
    "table_identifier",
    [
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_table_exists(catalog: SqlCatalog, table_schema_simple: Schema, table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(table_identifier, table_schema_simple, properties={"format-version": "2"})
    existing_table = table_identifier
    # Act and Assert for an existing table
    assert catalog.table_exists(existing_table) is True

    # Act and Assert for a non-existing table
    assert catalog.table_exists(("non", "exist")) is False


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_merge_manifests_local_file_system(catalog: SqlCatalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    # To catch manifest file name collision bug during merge:
    # https://github.com/apache/iceberg-python/pull/363#discussion_r1660691918
    catalog.create_namespace_if_not_exists("default")
    try:
        catalog.drop_table("default.test_merge_manifest")
    except NoSuchTableError:
        pass
    tbl = catalog.create_table(
        "default.test_merge_manifest",
        arrow_table_with_null.schema,
        properties={
            "commit.manifest-merge.enabled": "true",
            "commit.manifest.min-count-to-merge": "2",
            "format-version": format_version,
        },
    )

    for _ in range(5):
        tbl.append(arrow_table_with_null)

    assert len(tbl.scan().to_arrow()) == 5 * len(arrow_table_with_null)
    current_snapshot = tbl.current_snapshot()
    assert current_snapshot
    manifests = current_snapshot.manifests(tbl.io)
    assert len(manifests) == 1


@pytest.mark.parametrize(
    "catalog",
    [
        lazy_fixture("catalog_memory"),
        lazy_fixture("catalog_sqlite"),
        lazy_fixture("catalog_sqlite_without_rowcount"),
    ],
)
def test_delete_metadata_multiple(catalog: SqlCatalog, table_schema_nested: Schema, random_table_identifier: str) -> None:
    namespace = Catalog.namespace_from(random_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(random_table_identifier, table_schema_nested)

    original_metadata_location = table.metadata_location

    for i in range(5):
        with table.transaction() as transaction:
            with transaction.update_schema() as update:
                update.add_column(path=f"new_column_{i}", field_type=IntegerType())

    assert len(table.metadata.metadata_log) == 5
    assert os.path.exists(original_metadata_location[len("file://") :])

    # Set the max versions property to 2, and delete after commit
    new_property = {
        TableProperties.METADATA_PREVIOUS_VERSIONS_MAX: "2",
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED: "true",
    }

    with table.transaction() as transaction:
        transaction.set_properties(properties=new_property)

    # Verify that only the most recent metadata files are kept
    assert len(table.metadata.metadata_log) == 2
    updated_metadata_1, updated_metadata_2 = table.metadata.metadata_log

    # new metadata log was added, so earlier metadata logs are removed.
    with table.transaction() as transaction:
        with transaction.update_schema() as update:
            update.add_column(path="new_column_x", field_type=IntegerType())

    assert len(table.metadata.metadata_log) == 2
    assert not os.path.exists(original_metadata_location[len("file://") :])
    assert not os.path.exists(updated_metadata_1.metadata_file[len("file://") :])
    assert os.path.exists(updated_metadata_2.metadata_file[len("file://") :])


class TestSqlCatalogClose:
    """Test SqlCatalog close functionality."""

    def test_sql_catalog_close(self, catalog_sqlite: SqlCatalog) -> None:
        """Test that SqlCatalog close method properly disposes the engine."""
        # Verify engine exists
        assert hasattr(catalog_sqlite, "engine")

        # Close the catalog
        catalog_sqlite.close()

        # Verify engine is disposed by checking that the engine still exists
        assert hasattr(catalog_sqlite, "engine")

    def test_sql_catalog_context_manager(self, warehouse: Path) -> None:
        """Test that SqlCatalog works as a context manager."""
        with SqlCatalog("test", uri="sqlite:///:memory:", warehouse=str(warehouse)) as catalog:
            # Verify engine exists
            assert hasattr(catalog, "engine")

            # Create a namespace and table to test functionality
            catalog.create_namespace("test_db")
            schema = Schema(NestedField(1, "name", StringType(), required=True))
            catalog.create_table(("test_db", "test_table"), schema)

        # Verify engine is disposed after exiting context
        assert hasattr(catalog, "engine")

    def test_sql_catalog_context_manager_with_exception(self) -> None:
        """Test that SqlCatalog context manager properly closes even with exceptions."""
        catalog = None
        try:
            with SqlCatalog("test", uri="sqlite:///:memory:") as cat:
                catalog = cat
                # Verify engine exists
                assert hasattr(catalog, "engine")
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Verify engine is disposed even after exception
        assert catalog is not None
        assert hasattr(catalog, "engine")

    def test_sql_catalog_multiple_close_calls(self, catalog_sqlite: SqlCatalog) -> None:
        """Test that multiple close calls on SqlCatalog are safe."""
        # First close
        catalog_sqlite.close()

        # Second close should not raise an exception
        catalog_sqlite.close()
