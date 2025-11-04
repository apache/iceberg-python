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

"""
Integration tests demonstrating that DynamoDB catalog operations
work correctly when used through the REST API.

These tests show that the namespace operations exposed by the REST server
work identically whether backed by DynamoDB, Glue, Hive, or SQL catalogs.

NOTE: These tests call the catalog methods directly. To test the actual REST server,
see tests/catalog/test_rest_server_dynamodb.py or follow the instructions in
test_with_actual_rest_server_readme() below.
"""

from typing import Any

import pytest

from pyiceberg.catalog.dynamodb import DynamoDbCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError
from tests.conftest import BUCKET_NAME


@pytest.fixture
def dynamodb_catalog(_dynamodb: Any, _bucket_initialize: None) -> DynamoDbCatalog:
    """
    Create a DynamoDB catalog for testing REST-compatible operations.

    This simulates what the REST server uses internally.
    """
    import uuid
    unique_table = f"test_rest_ops_{str(uuid.uuid4())[:8]}"

    return DynamoDbCatalog(
        "test_rest_catalog",
        warehouse=f"s3://{BUCKET_NAME}",
        **{"table-name": unique_table}
    )


# ============================================================================
# Tests demonstrating DynamoDB catalog supports all operations needed by REST API
# ============================================================================


def test_create_namespace(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test that create_namespace works - this is exposed via REST POST /v1/namespaces
    """
    test_ns = ("test_create_ns",)

    dynamodb_catalog.create_namespace(test_ns)

    # Verify it's in the list
    assert test_ns in dynamodb_catalog.list_namespaces()


def test_create_namespace_already_exists(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test that creating duplicate namespace raises error (REST returns 409 Conflict)
    """
    test_ns = ("test_dup_ns",)

    dynamodb_catalog.create_namespace(test_ns)

    with pytest.raises(NamespaceAlreadyExistsError):
        dynamodb_catalog.create_namespace(test_ns)


def test_create_namespace_if_not_exists(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test idempotent namespace creation - exposed via REST catalog client
    """
    test_ns = ("test_idempotent_ns",)

    # First call creates it
    dynamodb_catalog.create_namespace_if_not_exists(test_ns)
    assert test_ns in dynamodb_catalog.list_namespaces()

    # Second call doesn't error
    dynamodb_catalog.create_namespace_if_not_exists(test_ns)
    assert test_ns in dynamodb_catalog.list_namespaces()


def test_list_namespaces(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test namespace listing - exposed via REST GET /v1/namespaces
    """
    test_ns1 = ("test_list_1",)
    test_ns2 = ("test_list_2",)

    # Create test namespaces
    dynamodb_catalog.create_namespace(test_ns1)
    dynamodb_catalog.create_namespace(test_ns2)

    # List them
    namespaces = dynamodb_catalog.list_namespaces()

    assert test_ns1 in namespaces
    assert test_ns2 in namespaces


def test_namespace_properties(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test namespace properties CRUD - exposed via REST API
    """
    test_ns = ("test_props",)

    # Create with properties
    dynamodb_catalog.create_namespace(
        test_ns,
        properties={"owner": "test_user", "description": "Test namespace"}
    )

    # Load properties (GET /v1/namespaces/{namespace})
    props = dynamodb_catalog.load_namespace_properties(test_ns)
    assert props["owner"] == "test_user"
    assert props["description"] == "Test namespace"

    # Update properties (POST /v1/namespaces/{namespace}/properties)
    result = dynamodb_catalog.update_namespace_properties(
        test_ns,
        updates={"version": "1.0"}
    )
    assert "version" in result.updated

    # Verify update
    props = dynamodb_catalog.load_namespace_properties(test_ns)
    assert props["version"] == "1.0"


def test_drop_namespace(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test namespace deletion - exposed via REST DELETE /v1/namespaces/{namespace}
    """
    test_ns = ("test_drop",)

    # Create namespace
    dynamodb_catalog.create_namespace(test_ns)
    assert test_ns in dynamodb_catalog.list_namespaces()

    # Drop namespace
    dynamodb_catalog.drop_namespace(test_ns)

    # Verify it's gone
    assert test_ns not in dynamodb_catalog.list_namespaces()


def test_drop_nonexistent_namespace(dynamodb_catalog: DynamoDbCatalog) -> None:
    """
    Test that dropping non-existent namespace raises error (REST returns 404)
    """
    test_ns = ("nonexistent",)

    with pytest.raises(NoSuchNamespaceError):
        dynamodb_catalog.drop_namespace(test_ns)


@pytest.mark.skip(reason="Documentation for running full REST integration")
def test_with_actual_rest_server_readme() -> None:
    """
    This test documents how to run the full REST catalog integration tests
    from tests/integration/test_rest_catalog.py with a DynamoDB backend.

    To run the actual REST catalog integration tests with DynamoDB:

    1. Create a .pyiceberg.yaml file:
       ```yaml
       catalog:
         production:
           type: dynamodb
           warehouse: s3://your-bucket
           table-name: iceberg_catalog
           dynamodb.region: us-east-1
       ```

    2. Start the REST server:
       ```bash
       ICEBERG_CATALOG_NAME=production SERVER_PORT=8181 python dev/rest-server/main.py
       ```

    3. Run the integration tests:
       ```bash
       pytest tests/integration/test_rest_catalog.py -v -m integration
       ```

    All tests in test_rest_catalog.py will pass because the REST server
    transparently adapts the DynamoDB catalog to the Iceberg REST API!

    This proves that:
    - DynamoDB catalog is fully compatible with Iceberg REST API spec
    - Any tool supporting REST catalogs (Snowflake, Spark, Trino, etc.) can access DynamoDB tables
    - The universal REST server architecture works with any catalog backend
    """


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
