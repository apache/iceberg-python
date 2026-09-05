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
from typing import Any
from uuid import UUID

import pytest

from pyiceberg.schema import Schema
from pyiceberg.view import View
from pyiceberg.view.metadata import SQLViewRepresentation, ViewHistoryEntry, ViewMetadata, ViewVersion


@pytest.fixture
def view(example_view_metadata_v1: dict[str, Any]) -> View:
    metadata = ViewMetadata.model_validate(example_view_metadata_v1)
    return View(("default", "test_view"), metadata)


def test_view_schema(view: View) -> None:
    schema = view.schema()
    assert isinstance(schema, Schema)
    assert schema.schema_id == 1
    assert len(schema.fields) == 3
    assert schema.find_field("x") is not None
    assert schema.find_field("y") is not None
    assert schema.find_field("z") is not None


def test_view_schemas(view: View) -> None:
    schemas = view.schemas()
    assert isinstance(schemas, dict)
    assert len(schemas) == 1
    assert 1 in schemas
    assert isinstance(schemas[1], Schema)


def test_view_current_version(view: View) -> None:
    version = view.current_version()
    assert isinstance(version, ViewVersion)
    assert version.version_id == 1
    assert version.schema_id == 1


def test_view_versions(view: View) -> None:
    versions = view.versions
    assert len(versions) == 1
    assert isinstance(versions[0], ViewVersion)
    assert versions[0].version_id == 1


def test_view_version_by_id(view: View) -> None:
    version = view.version(1)
    assert isinstance(version, ViewVersion)
    assert version.version_id == 1
    assert version == view.current_version()


def test_view_history(view: View) -> None:
    history = view.history()
    assert len(history) == 1
    assert isinstance(history[0], ViewHistoryEntry)
    assert history[0].version_id == 1
    assert history[0].timestamp_ms == 1602638573874


def test_view_properties(view: View) -> None:
    assert view.properties == {"comment": "this is a test view"}


def test_view_location(view: View) -> None:
    assert view.location() == "s3://bucket/test/location/test_view"


def test_view_uuid(view: View) -> None:
    assert view.uuid() == UUID("a20125c8-7284-442c-9aea-15fee620737c")


def test_view_sql_for_dialect(view: View) -> None:
    repr = view.sql_for("spark")
    assert isinstance(repr, SQLViewRepresentation)
    assert repr.dialect == "spark"
    assert repr.sql == "SELECT * FROM prod.db.table"


def test_view_sql_for_dialect_ignore_case(view: View) -> None:
    repr = view.sql_for("Spark")
    assert isinstance(repr, SQLViewRepresentation)
    assert repr.dialect == "spark"
    assert repr.sql == "SELECT * FROM prod.db.table"


def test_view_schemas_multiple(example_view_metadata_v1_multiple_versions: dict[str, Any]) -> None:
    view = View(("default", "test_view"), ViewMetadata.model_validate(example_view_metadata_v1_multiple_versions))
    schemas = view.schemas()
    assert len(schemas) == 2
    assert 1 in schemas
    assert 2 in schemas
    assert view.schema().schema_id == 2


def test_view_versions_multiple(example_view_metadata_v1_multiple_versions: dict[str, Any]) -> None:
    view = View(("default", "test_view"), ViewMetadata.model_validate(example_view_metadata_v1_multiple_versions))
    assert len(view.versions) == 2
    assert view.current_version().version_id == 2


def test_view_version_unknown_id(view: View) -> None:
    assert not view.version(999)


def test_view_sql_for_unknown_dialect(view: View) -> None:
    assert not view.sql_for("trino")
