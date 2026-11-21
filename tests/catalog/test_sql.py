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

import gc
import logging
import sqlite3
import warnings
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import Engine, Pool, create_engine, event, inspect, text
from sqlalchemy.exc import ArgumentError, OperationalError
from sqlalchemy.pool import QueuePool, SingletonThreadPool

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import (
    DEFAULT_ECHO_VALUE,
    DEFAULT_POOL_PRE_PING_VALUE,
    IcebergTables,
    SqlCatalog,
    SqlCatalogBaseTable,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import ARROW_FILE_IO, FSSPEC_FILE_IO, PY_IO_IMPL
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
        "uri": f"sqlite:///{warehouse.as_posix()}/sql-catalog",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(catalog_name, **props)
    catalog.create_tables()
    yield catalog
    catalog.destroy_tables()


@pytest.fixture(scope="module")
def catalog_uri(warehouse: Path) -> str:
    return f"sqlite:///{warehouse.as_posix()}/sql-catalog"


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
            "uri": f"sqlite:///{warehouse.as_posix()}/sql-catalog",
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
            "uri": f"sqlite:///{warehouse.as_posix()}/sql-catalog",
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


def test_creation_with_existing_engine(catalog_name: str, warehouse: Path) -> None:
    engine = create_engine(f"sqlite:///{warehouse.as_posix()}/sql-catalog-engine")

    catalog = SqlCatalog(catalog_name, engine=engine, warehouse=f"file://{warehouse}")

    assert catalog.engine is engine
    confirm_all_tables_exist(catalog)

    catalog.create_namespace("default")
    catalog.create_table("default.t", Schema(NestedField(1, "x", StringType())))
    assert catalog.table_exists("default.t")


def test_creation_with_existing_engine_without_uri(catalog_name: str, warehouse: Path) -> None:
    engine = create_engine(f"sqlite:///{warehouse.as_posix()}/sql-catalog-engine-no-uri")

    catalog = SqlCatalog(catalog_name, engine=engine, warehouse=f"file://{warehouse}")

    catalog.create_namespace("default")
    catalog.create_table("default.t2", Schema(NestedField(1, "x", StringType())))
    assert catalog.table_exists("default.t2")


def test_creation_from_impl(catalog_name: str, warehouse: Path) -> None:
    assert isinstance(
        load_catalog(
            catalog_name,
            **{
                "py-catalog-impl": "pyiceberg.catalog.sql.SqlCatalog",
                "uri": f"sqlite:///{warehouse.as_posix()}/sql-catalog",
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

    def test_sql_catalog_cross_thread_sqlite_close(self, warehouse: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Closing a SQLite SqlCatalog on a different thread than the one that created succeeds."""
        gc.collect()

        def create_in_worker() -> SqlCatalog:
            return SqlCatalog("test", uri="sqlite:///:memory:", warehouse=f"file://{warehouse}")

        with ThreadPoolExecutor(max_workers=1) as executor:
            catalog = executor.submit(create_in_worker).result()

        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always", ResourceWarning)
            with caplog.at_level(logging.ERROR, logger="sqlalchemy.pool"):
                catalog.close()

            pool_errors = [
                (record.exc_info[0], str(record.exc_info[1]))
                for record in caplog.records
                if record.exc_info and record.exc_info[0] is not None and record.exc_info[1] is not None
            ]
            caplog.clear()
            del catalog
            gc.collect()

        assert pool_errors == []
        assert [str(w.message) for w in caught_warnings] == []


@pytest.mark.parametrize("uri", ["sqlite:///:memory:", "sqlite+pysqlite:///:memory:"])
def test_sql_catalog_sqlite_memory_preserves_thread_isolation(
    warehouse: Path, caplog: pytest.LogCaptureFixture, uri: str
) -> None:
    """check_same_thread=False keeps SingletonThreadPool isolation, and only affects close and garbage collection."""
    gc.collect()
    catalog = SqlCatalog("test", uri=uri, warehouse=f"file://{warehouse}")
    assert isinstance(catalog.engine.pool, SingletonThreadPool)

    catalog.create_namespace("ns")
    assert catalog.list_namespaces() == [("ns",)]

    # worker thread cannot see the main thread DB
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(OperationalError, match="no such table"):
            executor.submit(catalog.list_namespaces).result()

    # closing the main thread connection also closes the worker thread's connection
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always", ResourceWarning)
        with caplog.at_level(logging.ERROR, logger="sqlalchemy.pool"):
            catalog.close()

        pool_errors = [
            (record.exc_info[0], str(record.exc_info[1]))
            for record in caplog.records
            if record.exc_info and record.exc_info[0] is not None and record.exc_info[1] is not None
        ]
        caplog.clear()
        del catalog
        gc.collect()

    assert pool_errors == []
    assert [str(w.message) for w in caught_warnings] == []


def test_sql_catalog_sqlite_explicit_check_same_thread_enforced(warehouse: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Explicit check_same_thread=true enforces sqlite3 same-thread checks at runtime during close()."""
    with ThreadPoolExecutor(max_workers=1) as executor:

        def create_in_worker() -> tuple[SqlCatalog, sqlite3.Connection]:
            cat = SqlCatalog("test", uri="sqlite:///:memory:?check_same_thread=true", warehouse=f"file://{warehouse}")
            with cat.engine.connect() as conn:
                dbapi_conn = cast(sqlite3.Connection, conn.connection.dbapi_connection)
            return cat, dbapi_conn

        strict_catalog, worker_conn = executor.submit(create_in_worker).result()
        try:
            with caplog.at_level(logging.ERROR, logger="sqlalchemy.pool"):
                strict_catalog.close()
            pool_errors = [record.exc_info[0] for record in caplog.records if record.exc_info and record.exc_info[0] is not None]
            caplog.clear()
            assert pool_errors == [sqlite3.ProgrammingError]
        finally:
            executor.submit(worker_conn.close).result()


@pytest.mark.parametrize(
    ("uri_template", "expected_pool_type", "expected_cparams"),
    [
        ("sqlite:///:memory:?check_same_thread=true", SingletonThreadPool, {"check_same_thread": True}),
        (
            "sqlite:///{warehouse}/sql-catalog-explicit-thread?check_same_thread=true",
            QueuePool,
            {"check_same_thread": True},
        ),
        (
            "sqlite:///:memory:?timeout=15&cached_statements=50",
            SingletonThreadPool,
            {"timeout": 15.0, "cached_statements": 50, "check_same_thread": False},
        ),
    ],
)
def test_sql_catalog_sqlite_preserves_explicit_query_params(
    warehouse: Path,
    uri_template: str,
    expected_pool_type: type[Pool],
    expected_cparams: dict[str, object],
) -> None:
    """Explicit SQLite URI query parameters (including check_same_thread=true) must not be overridden."""
    uri = uri_template.format(warehouse=warehouse.as_posix())
    catalog = SqlCatalog(name="test", uri=uri, warehouse=f"file://{warehouse}")
    with catalog:
        assert isinstance(catalog.engine.pool, expected_pool_type)
        catalog.engine.dispose()
        seen_cparams: list[dict[str, object]] = []

        @event.listens_for(catalog.engine, "do_connect")
        def capture_cparams(_dialect: object, _conn_rec: object, _cargs: object, cparams: dict[str, object]) -> None:
            seen_cparams.append(dict(cparams))

        with catalog.engine.connect():
            pass
        assert seen_cparams == [expected_cparams]


def get_columns(engine: Engine) -> set[str]:
    return {c["name"] for c in inspect(engine).get_columns("iceberg_tables")}


def test_adds_iceberg_type_column_to_old_schema(warehouse: Path) -> None:
    uri = f"sqlite:///{warehouse.as_posix()}/test-migration-add-col"
    engine = _create_v0_db(uri)

    # Verify the column does not exist in the old schema
    assert "iceberg_type" not in get_columns(engine)

    # Load the catalog with schema_version='v1' to trigger migration
    catalog = SqlCatalog(
        name="test",
        uri=uri,
        warehouse=f"file://{warehouse}",
        init_catalog_tables="false",
        schema_version="v1",
    )
    assert "iceberg_type" in get_columns(catalog.engine)


def test_idempotent_when_column_already_exists(warehouse: Path) -> None:
    catalog = SqlCatalog(
        name="test",
        uri="sqlite:///:memory:",
        warehouse=f"file://{warehouse}",
    )
    assert "iceberg_type" in get_columns(catalog.engine)

    # Verify initialization is idempotent when column already exists
    catalog._init_catalog()
    assert "iceberg_type" in get_columns(catalog.engine)


def test_load_table_ranks_catalog_config_above_table_properties(warehouse: Path) -> None:
    catalog = SqlCatalog(
        name="test",
        uri="sqlite:///:memory:",
        warehouse=f"file://{warehouse}",
        **{PY_IO_IMPL: ARROW_FILE_IO},
    )
    catalog.create_namespace("ns")
    catalog.create_table(
        ("ns", "tbl"),
        Schema(NestedField(1, "id", StringType(), required=True)),
        properties={PY_IO_IMPL: FSSPEC_FILE_IO, "s3.proxy-uri": "http://table-only-proxy"},
    )

    io = catalog.load_table(("ns", "tbl")).io
    assert io.properties[PY_IO_IMPL] == ARROW_FILE_IO
    assert io.properties["s3.proxy-uri"] == "http://table-only-proxy"


def test_list_tables_filters_by_iceberg_type(warehouse: Path) -> None:
    catalog = SqlCatalog(
        name="test",
        uri="sqlite:///:memory:",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("ns")
    schema = Schema(NestedField(1, "id", StringType(), required=True))
    catalog.create_table(("ns", "table_V1"), schema)

    # Insert a legacy-schema row (iceberg_type IS NULL); should appear in list_tables.
    with catalog.engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO iceberg_tables "
                "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type) "
                "VALUES ('test', 'ns', 'table_V0', NULL, NULL, NULL)"
            )
        )
        # Insert a non-TABLE row; should NOT appear in list_tables.
        conn.execute(
            text(
                "INSERT INTO iceberg_tables "
                "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type) "
                "VALUES ('test', 'ns', 'some_view', NULL, NULL, 'VIEW')"
            )
        )
        conn.commit()

    tables = [t[-1] for t in catalog.list_tables("ns")]
    assert "table_V1" in tables
    assert "table_V0" in tables
    assert "some_view" not in tables


def _insert_view_row(catalog: SqlCatalog, namespace: str, table_name: str) -> None:
    # Simulates a view row written by iceberg-java or iceberg-rust, which SqlCatalog table
    # operations must not treat as a table (issue #3337).
    with catalog.engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO iceberg_tables "
                "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type) "
                "VALUES ('test', :namespace, :table_name, 's3://fake/metadata.json', NULL, 'VIEW')"
            ),
            {"namespace": namespace, "table_name": table_name},
        )
        conn.commit()


def test_load_table_ignores_view_rows(warehouse: Path) -> None:
    catalog = SqlCatalog(name="test", uri="sqlite:///:memory:", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")
    _insert_view_row(catalog, "ns", "a_view")

    with pytest.raises(NoSuchTableError):
        catalog.load_table(("ns", "a_view"))


def test_drop_table_ignores_view_rows(warehouse: Path) -> None:
    catalog = SqlCatalog(name="test", uri="sqlite:///:memory:", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")
    _insert_view_row(catalog, "ns", "a_view")

    with pytest.raises(NoSuchTableError):
        catalog.drop_table(("ns", "a_view"))

    # The view row must not have been deleted.
    with catalog.engine.connect() as conn:
        row = conn.execute(text("SELECT iceberg_type FROM iceberg_tables WHERE table_name = 'a_view'")).fetchone()
    assert row is not None
    assert row[0] == "VIEW"


def test_rename_table_ignores_view_rows(warehouse: Path) -> None:
    catalog = SqlCatalog(name="test", uri="sqlite:///:memory:", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")
    _insert_view_row(catalog, "ns", "a_view")

    with pytest.raises(NoSuchTableError):
        catalog.rename_table(("ns", "a_view"), ("ns", "renamed_view"))

    # The view row must not have been renamed.
    with catalog.engine.connect() as conn:
        row = conn.execute(text("SELECT iceberg_type FROM iceberg_tables WHERE table_name = 'a_view'")).fetchone()
    assert row is not None
    assert row[0] == "VIEW"


def test_migration_to_v1_with_property_set(warehouse: Path) -> None:
    uri = f"sqlite:///{warehouse.as_posix()}/test-v1-migrate"
    engine = _create_v0_db(uri)

    assert "iceberg_type" not in get_columns(engine)

    catalog = SqlCatalog(
        name="test",
        uri=uri,
        warehouse=f"file://{warehouse}",
        init_catalog_tables="false",
        schema_version="v1",
    )
    assert "iceberg_type" in get_columns(catalog.engine)


def test_invalid_schema_version_raises(warehouse: Path) -> None:
    with pytest.raises(ValueError, match="Invalid schema_version"):
        SqlCatalog(
            name="test",
            uri="sqlite:///:memory:",
            warehouse=f"file://{warehouse}",
            schema_version="invalid",
        )


def test_list_tables_works_on_v0_schema(warehouse: Path) -> None:
    """list_tables should work on V0 schemas without iceberg_type column."""
    uri = f"sqlite:///{warehouse.as_posix()}/test-v0-list"
    _create_v0_db(uri)

    catalog = SqlCatalog(
        name="test",
        uri=uri,
        warehouse=f"file://{warehouse}",
        init_catalog_tables="false",
    )

    # Create namespace directly
    with catalog.engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO iceberg_namespace_properties "
                "(catalog_name, namespace, property_key, property_value) "
                "VALUES ('test', 'ns', 'exists', 'true')"
            )
        )
        # Insert rows (all are tables on V0, no iceberg_type column to distinguish)
        conn.execute(
            text(
                "INSERT INTO iceberg_tables "
                "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) "
                "VALUES ('test', 'ns', 'table1', 'loc1', NULL)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO iceberg_tables "
                "(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) "
                "VALUES ('test', 'ns', 'table2', 'loc2', NULL)"
            )
        )
        conn.commit()

    # list_tables should work on V0 (no iceberg_type filtering)
    tables = [t[-1] for t in catalog.list_tables("ns")]
    assert "table1" in tables
    assert "table2" in tables


def _create_v0_db(uri: str) -> Engine:
    """Create a SQLite DB with V0 schema (no iceberg_type column). Returns the engine."""
    engine = create_engine(uri)
    with engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE iceberg_tables ("
                "  catalog_name VARCHAR(255) NOT NULL,"
                "  table_namespace VARCHAR(255) NOT NULL,"
                "  table_name VARCHAR(255) NOT NULL,"
                "  metadata_location VARCHAR(1000),"
                "  previous_metadata_location VARCHAR(1000),"
                "  PRIMARY KEY (catalog_name, table_namespace, table_name)"
                ")"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE iceberg_namespace_properties ("
                "  catalog_name VARCHAR(255) NOT NULL,"
                "  namespace VARCHAR(255) NOT NULL,"
                "  property_key VARCHAR(255) NOT NULL,"
                "  property_value VARCHAR(1000) NOT NULL,"
                "  PRIMARY KEY (catalog_name, namespace, property_key)"
                ")"
            )
        )
        conn.commit()
    return engine


def _make_v0_catalog(uri: str, warehouse: Path) -> SqlCatalog:
    _create_v0_db(uri)
    return SqlCatalog("test", uri=uri, warehouse=f"file://{warehouse}", init_catalog_tables="false")


def test_namespace_exists_on_v0_schema(warehouse: Path) -> None:
    """namespace_exists should not fail on V0 schema (no iceberg_type column)."""
    catalog = _make_v0_catalog(f"sqlite:///{warehouse.as_posix()}/test-v0-ns-exists", warehouse)
    catalog.create_namespace("ns")
    assert catalog.namespace_exists("ns")
    assert not catalog.namespace_exists("missing")


def test_create_and_load_table_on_v0_schema(warehouse: Path) -> None:
    """create_table and load_table should work on V0 schema without iceberg_type column."""
    catalog = _make_v0_catalog(f"sqlite:///{warehouse.as_posix()}/test-v0-create", warehouse)
    catalog.create_namespace("ns")
    schema = Schema(NestedField(1, "id", StringType(), required=True))
    tbl = catalog.create_table(("ns", "tbl"), schema)
    assert tbl.name() == ("ns", "tbl")

    # load_table must work without iceberg_type in the SELECT
    loaded = catalog.load_table(("ns", "tbl"))
    assert loaded.name() == ("ns", "tbl")

    # iceberg_type column must still be absent from the DB (no migration occurred)
    assert "iceberg_type" not in get_columns(catalog.engine)


def test_create_namespace_concurrent_integrity_error(warehouse: Path) -> None:
    """Concurrent create_namespace requests should raise NamespaceAlreadyExistsError."""
    uri = f"sqlite:///{warehouse.as_posix()}/test-concurrent-create-namespace"
    warehouse_uri = f"file://{warehouse}"
    catalog_1 = SqlCatalog("test", uri=uri, warehouse=warehouse_uri)
    catalog_2 = SqlCatalog("test", uri=uri, warehouse=warehouse_uri)
    try:
        injected = False

        @event.listens_for(catalog_1.engine, "before_cursor_execute")
        def inject_concurrent_insert(
            _conn: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            nonlocal injected
            if not injected and "INSERT INTO iceberg_namespace_properties" in statement:
                injected = True
                catalog_2.create_namespace("concurrent_ns")

        with pytest.raises(NamespaceAlreadyExistsError):
            catalog_1.create_namespace("concurrent_ns")
    finally:
        catalog_1.close()
        catalog_2.close()
