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
import shutil
import uuid
from pathlib import Path

import pyarrow as pa
import pytest

from pyiceberg.catalog.hadoop import HadoopCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


def _schema() -> Schema:
    return Schema(NestedField(1, "id", LongType(), required=False))


def _catalog(tmp_path: Path) -> HadoopCatalog:
    return HadoopCatalog("hadoop", warehouse=f"file://{tmp_path}")


def test_create_load_and_commit_table(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace("db")

    table = catalog.create_table("db.tbl", _schema())
    assert table.metadata_location == f"file://{tmp_path}/db/tbl/metadata/v1.metadata.json"
    assert (tmp_path / "db/tbl/metadata/version-hint.text").read_text() == "1"

    table.append(pa.table({"id": [2, 1]}))
    table = catalog.load_table("db.tbl")

    assert table.scan().to_arrow().to_pylist() == [{"id": 2}, {"id": 1}]
    assert (tmp_path / "db/tbl/metadata/v2.metadata.json").exists()
    assert (tmp_path / "db/tbl/metadata/version-hint.text").read_text() == "2"


def test_version_hint_is_optional(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace("db")
    catalog.create_table("db.tbl", _schema())

    metadata_dir = tmp_path / "db/tbl/metadata"
    (metadata_dir / "version-hint.text").unlink()
    shutil.move(metadata_dir / "v1.metadata.json", metadata_dir / f"00000-{uuid.uuid4()}.metadata.json")

    table = catalog.load_table("db.tbl")

    assert table.metadata_location.startswith(f"file://{tmp_path}/db/tbl/metadata/00000-")
    assert table.schema() == _schema()


def test_stale_version_hint_falls_back_to_listing(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace("db")
    catalog.create_table("db.tbl", _schema())
    (tmp_path / "db/tbl/metadata/version-hint.text").write_text("99")

    table = catalog.load_table("db.tbl")

    assert table.metadata_location.endswith("v1.metadata.json")


def test_nested_namespaces_and_tables(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace(("a", "b"))
    catalog.create_table(("a", "b", "tbl"), _schema())

    assert catalog.list_namespaces() == [("a",)]
    assert catalog.list_namespaces("a") == [("a", "b")]
    assert catalog.list_tables(("a", "b")) == [("a", "b", "tbl")]
    assert catalog.load_namespace_properties(("a", "b")) == {"location": f"file://{tmp_path}/a/b"}


def test_create_and_drop_namespace(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace("db")

    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace("db")
    with pytest.raises(NotImplementedError):
        catalog.create_namespace("with-properties", {"owner": "test"})

    catalog.create_table("db.tbl", _schema())
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace("db")

    catalog.drop_table("db.tbl")
    catalog.drop_namespace("db")

    with pytest.raises(NoSuchNamespaceError):
        catalog.drop_namespace("db")
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_namespaces("db")


def test_create_and_load_errors(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace("db")
    table = catalog.create_table("db.tbl", _schema())

    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table("db.tbl", _schema())
    with pytest.raises(NoSuchTableError):
        catalog.load_table("db.missing")
    with pytest.raises(ValueError):
        catalog.create_table("db.other", _schema(), location=f"file://{tmp_path}/elsewhere")
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.name() + ("extra",))


def test_unsupported_operations(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    catalog.create_namespace("db")
    catalog.create_table("db.tbl", _schema())

    with pytest.raises(NotImplementedError):
        catalog.rename_table("db.tbl", "db.renamed")
    with pytest.raises(NotImplementedError):
        catalog.register_table("db.registered", "file:///metadata.json")
    with pytest.raises(NotImplementedError):
        catalog.update_namespace_properties("db", updates={"owner": "test"})

    assert catalog.view_exists("db.view") is False
    assert catalog.list_views("db") == []


def test_load_catalog_hadoop_requires_warehouse() -> None:
    from pyiceberg.catalog import load_catalog

    with pytest.raises(ValueError, match="Warehouse missing"):
        load_catalog("hadoop_test", type="hadoop")
