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

import pytest
from moto import mock_aws

from pyiceberg.catalog import Catalog
from pyiceberg.io import WAREHOUSE
from tests.conftest import BUCKET_NAME


@pytest.fixture(scope="function")
@mock_aws
def glue(_bucket_initialize: None, moto_endpoint_url: str) -> Catalog:
    from pyiceberg.catalog.glue import GlueCatalog

    catalog = GlueCatalog(name="glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})

    return catalog


@pytest.fixture(scope="function")
@mock_aws
def dynamodb(_bucket_initialize: None, moto_endpoint_url: str) -> Catalog:
    from pyiceberg.catalog.dynamodb import DynamoDbCatalog

    catalog = DynamoDbCatalog(name="dynamodb", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})

    return catalog


@pytest.fixture(scope="function")
def memory_catalog(tmp_path: PosixPath) -> Catalog:
    from pyiceberg.catalog.memory import InMemoryCatalog

    return InMemoryCatalog("test.in_memory.catalog", **{WAREHOUSE: tmp_path.absolute().as_posix(), "test.key": "test.value"})


@pytest.fixture(scope="function")
def sqllite_catalog_memory(warehouse: Path) -> Catalog:
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("sqlitememory", uri="sqlite:///:memory:", warehouse=f"file://{warehouse}")

    return catalog


@pytest.fixture(scope="function")
def sqllite_catalog_file(warehouse: Path) -> Catalog:
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("sqlitefile", uri=f"sqlite:////{warehouse}/sql-catalog.db", warehouse=f"file://{warehouse}")

    return catalog


@pytest.mark.parametrize(
    "catalog",
    [
        pytest.lazy_fixture("glue"),
        pytest.lazy_fixture("dynamodb"),
        pytest.lazy_fixture("memory_catalog"),
        pytest.lazy_fixture("sqllite_catalog_memory"),
        pytest.lazy_fixture("sqllite_catalog_file"),
    ],
)
def test_create_namespace_no_properties(
    catalog: Catalog,
    database_name: str,
) -> None:
    catalog.create_namespace(namespace=database_name)
    loaded_database_list = catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = catalog.load_namespace_properties(database_name)
    assert properties == {}
    catalog.drop_namespace(database_name)
    assert len(catalog.list_namespaces()) == 0
