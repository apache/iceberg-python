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
# pylint:disable=redefined-outer-name

import pytest
from pytest_lazy_fixtures import lf

from pyiceberg.catalog.rest import RestCatalog

TEST_NAMESPACE_IDENTIFIER = "TEST NS"


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_namespace_exists(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_namespace_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_create_namespace_if_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_create_namespace_if_already_existing(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_create_table_invalid_sort_order(catalog: RestCatalog) -> None:
    from pyiceberg.catalog.rest import Endpoints
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    namespace = "default"
    table_name = "test_invalid_sort_order"

    # Ensure namespace exists
    catalog.create_namespace_if_not_exists(namespace)

    # Define a simple schema
    schema = Schema(NestedField(1, "x", LongType()))

    # Create the payload manually to bypass client-side validation
    # We want a sort order that references a non-existent field ID (e.g., 9999)
    payload = {
        "name": table_name,
        "schema": schema.model_dump(by_alias=True),
        "write-order": {
            "order-id": 1,
            "fields": [
                {
                    "transform": "identity",
                    "source-id": 9999,  # Non-existent field
                    "direction": "asc",
                    "null-order": "nulls-first",
                }
            ],
        },
        "stage-create": False,
    }

    # Send the request using the underlying session
    # Endpoints.create_table is "namespaces/{namespace}/tables"
    url = catalog.url(Endpoints.create_table, namespace=namespace)

    response = catalog._session.post(url, json=payload)

    assert response.status_code == 400
    response_json = response.json()
    response_json["error"].pop("stack", None)
    assert response_json == {
        "error": {
            "message": "Cannot find source column for sort field: identity(9999) ASC NULLS FIRST",
            "type": "ValidationException",
            "code": 400,
        }
    }
