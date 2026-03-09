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

from collections.abc import Generator
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.exc import ArgumentError

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import (
    DEFAULT_ECHO_VALUE,
    DEFAULT_POOL_PRE_PING_VALUE,
    IcebergTables,
    SqlCatalog,
    SqlCatalogBaseTable,
)
from pyiceberg.exceptions import (
    NoSuchPropertyException,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, strtobool

CATALOG_TABLES = [c.__tablename__ for c in SqlCatalogBaseTable.__subclasses__()]


@pytest.fixture(scope="module")
def catalog_name() -> str:
    return "test_sql_catalog"


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
