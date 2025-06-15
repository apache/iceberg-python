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
from typing import Generator

import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.dynamodb import DynamoDbCatalog
from pyiceberg.catalog.glue import GLUE_CATALOG_ENDPOINT, GlueCatalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import WAREHOUSE
from tests.conftest import clean_up, get_bucket_name, get_glue_endpoint, get_s3_path

# The number of tables/databases used in list_table/namespace test
LIST_TEST_NUMBER = 2


@pytest.fixture(scope="function")
def dynamodb() -> Generator[Catalog, None, None]:
    test_catalog = DynamoDbCatalog("test_dynamodb_catalog", warehouse=get_s3_path(get_bucket_name()))
    yield test_catalog
    clean_up(test_catalog)


@pytest.fixture(scope="function")
def glue() -> Generator[Catalog, None, None]:
    test_catalog = GlueCatalog(
        "test_glue_catalog", **{"warehouse": get_s3_path(get_bucket_name()), GLUE_CATALOG_ENDPOINT: get_glue_endpoint()}
    )
    yield test_catalog
    clean_up(test_catalog)


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
        uri="thrift://localhost:9083",
    )
    yield test_catalog
    clean_up(test_catalog)


@pytest.mark.integration
@pytest.mark.parametrize(
    "test_catalog",
    [
        pytest.lazy_fixture("glue"),
        pytest.lazy_fixture("dynamodb"),
        pytest.lazy_fixture("memory_catalog"),
        pytest.lazy_fixture("sqlite_catalog_memory"),
        pytest.lazy_fixture("sqlite_catalog_file"),
        pytest.lazy_fixture("rest_catalog"),
        pytest.lazy_fixture("hive_catalog"),
    ],
)
def test_create_namespace(
    test_catalog: Catalog,
    database_name: str,
) -> None:
    test_catalog.create_namespace(database_name)
    # note the use of `in` because some catalogs have a "default" namespace
    assert (database_name,) in test_catalog.list_namespaces()
