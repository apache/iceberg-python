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

"""Pytest fixtures for parameterized catalog behavior tests."""

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from pytest_lazyfixture import lazy_fixture

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.typedef import Identifier


def _create_memory_catalog(name: str, warehouse: Path) -> InMemoryCatalog:
    return InMemoryCatalog(name, warehouse=f"file://{warehouse}")


def _create_sql_catalog(name: str, warehouse: Path) -> SqlCatalog:
    catalog = SqlCatalog(
        name,
        uri="sqlite:///:memory:",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_tables()
    return catalog


def _create_sql_without_rowcount_catalog(name: str, warehouse: Path) -> SqlCatalog:
    props = {
        "uri": f"sqlite:////{warehouse}/sql-catalog",
        "warehouse": f"file://{warehouse}",
    }
    catalog = SqlCatalog(name, **props)
    catalog.engine.dialect.supports_sane_rowcount = False
    catalog.create_tables()
    return catalog


_CATALOG_FACTORIES = {
    "memory": _create_memory_catalog,
    "sql": _create_sql_catalog,
    "sql_without_rowcount": _create_sql_without_rowcount_catalog,
}


@pytest.fixture(params=list(_CATALOG_FACTORIES.keys()))
def catalog(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[Catalog, None, None]:
    """Parameterized fixture that yields catalogs listed in _CATALOG_FACTORIES."""
    catalog_type = request.param
    factory = _CATALOG_FACTORIES[catalog_type]
    cat = factory("test_catalog", tmp_path)
    yield cat
    if hasattr(cat, "destroy_tables"):
        cat.destroy_tables()


@pytest.fixture(params=list(_CATALOG_FACTORIES.keys()))
def catalog_with_warehouse(
    request: pytest.FixtureRequest,
    warehouse: Path,
) -> Generator[Catalog, None, None]:
    factory = _CATALOG_FACTORIES[request.param]
    cat = factory("test_catalog", warehouse)
    yield cat
    if hasattr(cat, "destroy_tables"):
        cat.destroy_tables()


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


@pytest.fixture(scope="session")
def fixed_test_table_identifier() -> Identifier:
    return "com", "organization", "department", "my_table"


@pytest.fixture(scope="session")
def another_fixed_test_table_identifier() -> Identifier:
    return "com", "organization", "department_alt", "my_another_table"


@pytest.fixture(scope="session")
def fixed_test_table_namespace() -> Identifier:
    return "com", "organization", "department"


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("fixed_test_table_identifier"),
        lazy_fixture("random_table_identifier"),
        lazy_fixture("random_hierarchical_identifier"),
    ],
)
def test_table_identifier(request) -> Identifier:
    return request.param


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("another_fixed_test_table_identifier"),
        lazy_fixture("another_random_table_identifier"),
        lazy_fixture("another_random_hierarchical_identifier"),
    ],
)
def another_table_identifier(request) -> Identifier:
    return request.param


@pytest.fixture(
    params=[
        lazy_fixture("database_name"),
        lazy_fixture("hierarchical_namespace_name"),
        lazy_fixture("fixed_test_table_namespace"),
    ],
)
def test_namespace(request) -> Identifier:
    ns = request.param
    if isinstance(ns, tuple):
        return ns
    if "." in ns:
        return tuple(ns.split("."))
    return (ns,)


@pytest.fixture(scope="session")
def test_namespace_properties() -> dict[str, str]:
    return {"key1": "value1", "key2": "value2"}


@pytest.fixture(scope="session")
def test_table_properties() -> dict[str, str]:
    return {
        "key1": "value1",
        "key2": "value2",
    }
