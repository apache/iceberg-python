#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from pathlib import Path, PosixPath
from typing import Generator, List

import pytest

from pyiceberg.catalog import Catalog, MetastoreCatalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import WAREHOUSE
from pyiceberg.schema import Schema
from tests.conftest import clean_up


@pytest.fixture(scope="function")
def memory_catalog(tmp_path: PosixPath) -> Generator[Catalog, None, None]:
    test_catalog = InMemoryCatalog(
        "test.in_memory.catalog", **{WAREHOUSE: tmp_path.absolute().as_posix(), "test.key": "test.value"}
    )
    yield test_catalog

    clean_up(test_catalog)


@pytest.fixture(scope="function")
def sqlite_catalog_memory(warehouse: Path) -> Generator[Catalog, None, None]:
    test_catalog = SqlCatalog("sqlitememory", uri="sqlite:///:memory:", warehouse=f"file://{warehouse}")

    yield test_catalog

    clean_up(test_catalog)


@pytest.fixture(scope="function")
def sqlite_catalog_file(warehouse: Path) -> Generator[Catalog, None, None]:
    test_catalog = SqlCatalog("sqlitefile", uri=f"sqlite:////{warehouse}/sql-catalog.db", warehouse=f"file://{warehouse}")

    yield test_catalog

    clean_up(test_catalog)


@pytest.fixture(scope="function")
def rest_catalog() -> Generator[Catalog, None, None]:
    test_catalog = RestCatalog("rest", uri="http://localhost:8181")

    yield test_catalog

    clean_up(test_catalog)


@pytest.fixture(scope="function")
def hive_catalog() -> Generator[Catalog, None, None]:
    test_catalog = HiveCatalog(
        "test_hive_catalog",
        **{
            "uri": "http://localhost:9083",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    yield test_catalog
    clean_up(test_catalog)


CATALOGS = [
    pytest.lazy_fixture("memory_catalog"),
    pytest.lazy_fixture("sqlite_catalog_memory"),
    pytest.lazy_fixture("sqlite_catalog_file"),
    pytest.lazy_fixture("rest_catalog"),
    pytest.lazy_fixture("hive_catalog"),
]


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_table_with_default_location(
    test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.name() == identifier
    assert MetastoreCatalog._parse_metadata_version(table.metadata_location) == 0


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_table_with_invalid_database(test_catalog: Catalog, table_schema_nested: Schema, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier, table_schema_nested)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_duplicated_table(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
    test_catalog.create_namespace(database_name)
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table((database_name, table_name), table_schema_nested)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_table_if_not_exists_duplicated_table(
    test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    test_catalog.create_namespace(database_name)
    table1 = test_catalog.create_table((database_name, table_name), table_schema_nested)
    table2 = test_catalog.create_table_if_not_exists((database_name, table_name), table_schema_nested)
    assert table1.name() == table2.name()


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_load_table(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    loaded_table = test_catalog.load_table(identifier)
    assert table.name() == loaded_table.name()
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_list_tables(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_list: List[str]) -> None:
    test_catalog.create_namespace(database_name)
    for table_name in table_list:
        test_catalog.create_table((database_name, table_name), table_schema_nested)
    identifier_list = test_catalog.list_tables(database_name)
    assert len(identifier_list) == len(table_list)
    for table_name in table_list:
        assert (database_name, table_name) in identifier_list


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_rename_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    new_database_name = f"{database_name}_new"
    test_catalog.create_namespace(database_name)
    test_catalog.create_namespace(new_database_name)

    identifier = (database_name, table_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier

    new_table_name = f"rename-{table_name}"
    new_identifier = (new_database_name, new_table_name)
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)

    assert new_table.name() == new_identifier
    assert new_table.metadata_location == table.metadata_location

    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_rename_table_already_exists(
    test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str
) -> None:
    new_database_name = f"{database_name}_new"
    test_catalog.create_namespace(database_name)
    test_catalog.create_namespace(new_database_name)

    identifier = (database_name, table_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier

    new_table_name = f"rename-{table_name}"
    new_identifier = (new_database_name, new_table_name)
    new_table = test_catalog.create_table(new_identifier, table_schema_nested)
    assert new_table.name() == new_identifier

    with pytest.raises(TableAlreadyExistsError):
        test_catalog.rename_table(identifier, new_identifier)

    assert test_catalog.table_exists(identifier)
    assert test_catalog.table_exists(new_identifier)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_drop_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_purge_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    if isinstance(test_catalog, HiveCatalog):
        pytest.skip("HiveCatalog does not support purge_table operation yet")

    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.name() == identifier
    test_catalog.purge_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_table_exists(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
    test_catalog.create_namespace(database_name)
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    assert test_catalog.table_exists((database_name, table_name)) is True


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_namespace(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_duplicate_namespace(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(database_name)


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_namepsace_if_not_exists(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    test_catalog.create_namespace_if_not_exists(database_name)
    assert (database_name,) in test_catalog.list_namespaces()


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_create_namespace_with_comment(test_catalog: Catalog, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
    }
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_list_namespaces(test_catalog: Catalog, database_list: List[str]) -> None:
    for database_name in database_list:
        test_catalog.create_namespace(database_name)
    db_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in db_list
    assert len(test_catalog.list_namespaces(list(database_list)[0])) == 0


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_drop_namespace(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)
    test_catalog.drop_table((database_name, table_name))
    test_catalog.drop_namespace(database_name)
    assert (database_name,) not in test_catalog.list_namespaces()


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_load_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in test_properties.items():
        assert v == listed_properties[k]


@pytest.mark.integration
@pytest.mark.parametrize("test_catalog", CATALOGS)
def test_update_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog.create_namespace(database_name, test_properties)
    update_report = test_catalog.update_namespace_properties(database_name, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == test_catalog.load_namespace_properties(database_name)["comment"]
