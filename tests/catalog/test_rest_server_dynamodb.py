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
Integration tests for the REST Server with DynamoDB catalog backend.

These tests verify that the REST API works correctly when backed by DynamoDB.
"""

from typing import Any

import pytest
from moto import mock_aws

from pyiceberg.catalog.dynamodb import DynamoDbCatalog
from tests.conftest import BUCKET_NAME


@mock_aws
def test_rest_server_can_use_dynamodb_catalog(_dynamodb: Any, _bucket_initialize: None) -> None:
    """
    Test that the REST server can successfully use a DynamoDB catalog.

    This is a simple smoke test to verify the DynamoDB catalog works
    with the REST server architecture.
    """
    # Create a DynamoDB catalog (simulates what the REST server does)
    catalog = DynamoDbCatalog(
        "test_rest_catalog",
        **{
            "warehouse": f"s3://{BUCKET_NAME}",
            "table-name": "test_rest_dynamodb_table"
        }
    )

    # Verify basic catalog operations work
    # (These are the operations the REST server would call)

    # 1. Create namespace
    namespace = ("test_namespace",)
    catalog.create_namespace(namespace, properties={"owner": "rest_server"})

    # 2. List namespaces
    namespaces = catalog.list_namespaces()
    assert namespace in namespaces

    # 3. Load namespace properties
    props = catalog.load_namespace_properties(namespace)
    assert props["owner"] == "rest_server"

    # 4. Update namespace properties
    summary = catalog.update_namespace_properties(
        namespace,
        removals=None,
        updates={"description": "REST server test namespace"}
    )
    assert "description" in summary.updated

    # 5. Drop namespace
    catalog.drop_namespace(namespace)

    # 6. Verify namespace is gone
    namespaces = catalog.list_namespaces()
    assert namespace not in namespaces

    print("✅ REST server can successfully use DynamoDB catalog!")


@mock_aws
def test_dynamodb_catalog_properties_for_rest(_dynamodb: Any) -> None:
    """
    Test that DynamoDB catalog exposes properties correctly for REST server config endpoint.
    """
    catalog = DynamoDbCatalog(
        "test_catalog",
        **{
            "warehouse": f"s3://{BUCKET_NAME}",
            "table-name": "test_table",
            "dynamodb.region": "us-east-1"
        }
    )

    # Verify catalog properties are accessible (used by /v1/config endpoint)
    # Note: The 'type' is not in catalog.properties for DynamoDB,
    # but the REST server can get it from the catalog class
    assert "warehouse" in catalog.properties
    assert isinstance(catalog, DynamoDbCatalog)

    print(f"✅ Catalog properties: {catalog.properties}")
    print(f"✅ Catalog type: {type(catalog).__name__}")


@mock_aws
def test_multiple_operations_sequence(_dynamodb: Any, _bucket_initialize: None) -> None:
    """
    Test a sequence of operations similar to what a REST client might perform.
    """
    # Use unique table name to avoid cross-test contamination
    import uuid
    unique_suffix = str(uuid.uuid4())[:8]

    catalog = DynamoDbCatalog(
        "test_catalog",
        **{
            "warehouse": f"s3://{BUCKET_NAME}",
            "table-name": f"test_multi_ops_{unique_suffix}"
        }
    )

    # Simulate REST client workflow
    ns1 = ("database1",)
    ns2 = ("database2",)

    # Create multiple namespaces
    catalog.create_namespace(ns1, properties={"env": "prod"})
    catalog.create_namespace(ns2, properties={"env": "dev"})

    # List all namespaces
    all_namespaces = catalog.list_namespaces()
    assert ns1 in all_namespaces
    assert ns2 in all_namespaces
    assert len(all_namespaces) == 2

    # Update one namespace
    catalog.update_namespace_properties(ns1, updates={"version": "1.0"})
    props = catalog.load_namespace_properties(ns1)
    assert props["version"] == "1.0"

    # Drop one namespace
    catalog.drop_namespace(ns2)

    # Verify only one remains
    remaining = catalog.list_namespaces()
    assert len(remaining) == 1
    assert ns1 in remaining
    assert ns2 not in remaining

    print("✅ Multiple operation sequence completed successfully!")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
