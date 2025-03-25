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
from __future__ import annotations

import json
import os
from typing import Any, Generator, List
from unittest.mock import MagicMock, patch

import pytest

from pyiceberg.catalog.snowflake import SnowflakeCatalog
from pyiceberg.table.metadata import TableMetadataUtil


class TestSnowflakeIdentifier:
    def test_get_table_name(self) -> None:
        sf_id = SnowflakeCatalog._SnowflakeIdentifier.table_from_id("db.schema.table")
        assert sf_id.table_name == "db.schema.table"

        sf_id = SnowflakeCatalog._SnowflakeIdentifier.table_from_id("schema.table")
        assert sf_id.table_name == "schema.table"

        sf_id = SnowflakeCatalog._SnowflakeIdentifier.table_from_id("table")
        assert sf_id.table_name == "table"

        with pytest.raises(ValueError):
            SnowflakeCatalog._SnowflakeIdentifier.table_from_id("db.schema.table.extra")

    def test_get_schema_name(self) -> None:
        sf_id = SnowflakeCatalog._SnowflakeIdentifier.schema_from_string("db.schema")
        assert sf_id.schema_name == "db.schema"

        sf_id = SnowflakeCatalog._SnowflakeIdentifier.schema_from_string("schema")
        assert sf_id.schema_name == "schema"

        with pytest.raises(ValueError):
            SnowflakeCatalog._SnowflakeIdentifier.schema_from_string("db.schema.extra")


class MockSnowflakeCursor:
    q = ""
    q_params: List[Any] = []
    qs: List[Any] = []

    def __enter__(self) -> Any:
        return self

    def __exit__(*args: Any) -> None:
        pass

    def rollback(self) -> None:
        pass

    def fetchall(self) -> Any:
        if "SHOW ICEBERG TABLES" in self.q:
            return [
                {
                    "database_name": "db",
                    "schema_name": "schema",
                    "table_name": "tbl_1",
                },
                {
                    "database_name": "db",
                    "schema_name": "schema",
                    "table_name": "tbl_2",
                },
            ]

        return []

    def fetchone(self) -> Any:
        if "SYSTEM$GET_ICEBERG_TABLE_INFORMATION" in self.q:
            _schema_simulator = "s3"

            if "gcs_table" in self.q_params[0]:
                _schema_simulator = "gcs"

            return {
                "METADATA": json.dumps(
                    {
                        "metadataLocation": f"{_schema_simulator}://bucket/path/to/metadata.json",
                    }
                )
            }

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        self.q = args[0]
        self.q_params = args[1]
        self.qs.append(args)


class MockSnowflakeConnection:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._cursor = MockSnowflakeCursor()
        self._cursor.qs = []

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        return self._cursor


class MockCreds:
    def get_frozen_credentials(self) -> Any:
        creds = MagicMock()

        creds.access_key = ""
        creds.secret_key = ""
        creds.token = ""

        return creds


@pytest.fixture(scope="function")
def catalog() -> Generator[SnowflakeCatalog, None, None]:
    with patch(
        "pyiceberg.serializers.FromInputFile.table_metadata",
        return_value=TableMetadataUtil.parse_obj(
            {
                "format-version": 2,
                "location": "s3://bucket/path/to/",
                "last-column-id": 4,
                "schemas": [{}],
                "partition-specs": [{}],
            }
        ),
    ):
        with patch("pyiceberg.catalog.snowflake.Session.get_credentials", MockCreds):
            with patch("pyiceberg.catalog.snowflake.SnowflakeConnection", MockSnowflakeConnection):
                yield SnowflakeCatalog(
                    name="test",
                    user="",
                    account="",
                )


def test_load_table_s3(catalog: SnowflakeCatalog) -> None:
    tbl = catalog.load_table("db.schema.table")

    assert tbl is not None


def test_load_table_gcs(catalog: SnowflakeCatalog) -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/creds.json"
    tbl = catalog.load_table("db.schema.gcs_table")

    assert tbl is not None


def test_register_table(catalog: SnowflakeCatalog) -> None:
    qs = catalog.connection._cursor.qs

    tbl = catalog.register_table("db.schema.table", "s3://bucket/path/to/metadata.json")

    assert len(qs) == 2

    assert qs[0][0] == "CREATE ICEBERG TABLE (%s) METADATA_FILE_PATH = (%s)"
    assert qs[0][1] == ("db.schema.table", "s3://bucket/path/to/metadata.json")

    assert tbl is not None


def test_drop_table(catalog: SnowflakeCatalog) -> None:
    catalog.drop_table("db.schema.table")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 1

    assert qs[0][0] == "DROP TABLE IF EXISTS (%s)"
    assert qs[0][1] == ("db.schema.table",)


def test_rename_table(catalog: SnowflakeCatalog) -> None:
    catalog.rename_table("table", "schema.new_table")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 2

    assert qs[0][0] == "ALTER TABLE (%s) RENAME TO (%s)"
    assert qs[0][1] == ("table", "schema.new_table")


def test_create_namespace_schema_only(catalog: SnowflakeCatalog) -> None:
    catalog.create_namespace("schema")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 1

    assert qs[0][0] == "CREATE SCHEMA IF NOT EXISTS (%s)"
    assert qs[0][1] == ("schema",)


def test_create_namespace_with_db(catalog: SnowflakeCatalog) -> None:
    catalog.create_namespace("db.schema")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 2

    assert qs[0][0] == "CREATE DATABASE IF NOT EXISTS (%s)"
    assert qs[0][1] == ("db",)

    assert qs[1][0] == "CREATE SCHEMA IF NOT EXISTS (%s)"
    assert qs[1][1] == ("db.schema",)


def test_drop_namespace_schema_only(catalog: SnowflakeCatalog) -> None:
    catalog.drop_namespace("schema")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 1

    assert qs[0][0] == "DROP SCHEMA IF EXISTS (%s)"
    assert qs[0][1] == ("schema",)


def test_drop_namespace_with_db(catalog: SnowflakeCatalog) -> None:
    catalog.drop_namespace("db.schema")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 2

    assert qs[0][0] == "DROP DATABASE IF EXISTS (%s)"
    assert qs[0][1] == ("db",)

    assert qs[1][0] == "DROP SCHEMA IF EXISTS (%s)"
    assert qs[1][1] == ("db.schema",)


def test_list_tables_schema_only(catalog: SnowflakeCatalog) -> None:
    tabs = catalog.list_tables("schema")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 1

    assert qs[0][0] == "SHOW ICEBERG TABLES IN SCHEMA (%s)"
    assert qs[0][1] == ("schema",)

    assert len(tabs) == 2
    assert tabs[0] == ("db", "schema", "tbl_1")
    assert tabs[1] == ("db", "schema", "tbl_2")


def test_list_tables_with_db(catalog: SnowflakeCatalog) -> None:
    tabs = catalog.list_tables("db.schema")

    qs = catalog.connection._cursor.qs

    assert len(qs) == 1

    assert qs[0][0] == "SHOW ICEBERG TABLES IN DATABASE (%s)"
    assert qs[0][1] == ("db",)

    assert len(tabs) == 2
    assert tabs[0] == ("db", "schema", "tbl_1")
    assert tabs[1] == ("db", "schema", "tbl_2")
