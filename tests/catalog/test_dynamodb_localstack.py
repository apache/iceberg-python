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
"""Integration tests for DynamoDB catalog using LocalStack.

These tests require LocalStack to be running on localhost:4566.
To run LocalStack: docker run -d -p 4566:4566 -e SERVICES=dynamodb,s3 localstack/localstack

Run these tests with: pytest tests/catalog/test_dynamodb_localstack.py -v
"""
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from pyiceberg.catalog.dynamodb import DynamoDbCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

# LocalStack configuration
LOCALSTACK_ENDPOINT = "http://localhost:4566"
TEST_BUCKET = f"test-iceberg-bucket-{uuid.uuid4().hex[:8]}"
TEST_REGION = "us-east-1"


def is_localstack_running() -> bool:
    """Check if LocalStack is running and accessible."""
    try:
        import requests

        response = requests.get(f"{LOCALSTACK_ENDPOINT}/_localstack/health", timeout=2)
        return response.status_code == 200
    except Exception:
        return False


# Skip all tests if LocalStack is not running
pytestmark = pytest.mark.skipif(
    not is_localstack_running(),
    reason="LocalStack is not running. Start with: docker run -d -p 4566:4566 -e SERVICES=dynamodb,s3 localstack/localstack",
)


@pytest.fixture(scope="module")
def s3_bucket():  # type: ignore
    """Create an S3 bucket in LocalStack for testing."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=TEST_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    # Create bucket
    s3_client.create_bucket(Bucket=TEST_BUCKET)

    yield TEST_BUCKET

    # Cleanup: delete all objects and the bucket
    try:
        response = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
        if "Contents" in response:
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(Bucket=TEST_BUCKET, Delete={"Objects": objects})
        s3_client.delete_bucket(Bucket=TEST_BUCKET)
    except ClientError:
        pass  # Bucket might already be deleted


@pytest.fixture(scope="function")
def catalog(s3_bucket: str):  # type: ignore
    """Create a DynamoDB catalog connected to LocalStack."""
    catalog_name = f"test_catalog_{uuid.uuid4().hex[:8]}"
    table_name = f"iceberg_catalog_{uuid.uuid4().hex[:8]}"

    catalog = DynamoDbCatalog(
        catalog_name,
        **{
            "table-name": table_name,
            "warehouse": f"s3://{s3_bucket}",
            "dynamodb.endpoint": LOCALSTACK_ENDPOINT,
            "s3.endpoint": LOCALSTACK_ENDPOINT,
            "dynamodb.region": TEST_REGION,
            "dynamodb.access-key-id": "test",
            "dynamodb.secret-access-key": "test",
        },
    )

    yield catalog

    # Cleanup: delete the DynamoDB table
    try:
        catalog.dynamodb.delete_table(TableName=table_name)
    except ClientError:
        pass  # Table might already be deleted


@pytest.fixture
def simple_schema() -> Schema:
    """Simple test schema."""
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        schema_id=1,
        identifier_field_ids=[1],
    )


def test_localstack_connection(catalog: DynamoDbCatalog) -> None:
    """Test that catalog can connect to LocalStack."""
    # Verify DynamoDB table exists
    response = catalog.dynamodb.describe_table(TableName=catalog.dynamodb_table_name)
    assert response["Table"]["TableStatus"] == "ACTIVE"
    assert "Table" in response


def test_create_namespace_localstack(catalog: DynamoDbCatalog) -> None:
    """Test creating a namespace in LocalStack."""
    namespace = "test_namespace"
    catalog.create_namespace(namespace, properties={"owner": "test_user"})

    # Verify namespace was created
    namespaces = catalog.list_namespaces()
    assert (namespace,) in namespaces

    # Verify properties
    props = catalog.load_namespace_properties(namespace)
    assert props["owner"] == "test_user"


def test_create_table_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test creating a table in LocalStack."""
    namespace = "test_db"
    table_name = "test_table"
    identifier = (namespace, table_name)

    # Create namespace first
    catalog.create_namespace(namespace)

    # Create table
    table = catalog.create_table(identifier, simple_schema)

    assert table.name() == identifier
    assert table.schema() == simple_schema

    # Verify table can be loaded
    loaded_table = catalog.load_table(identifier)
    assert loaded_table.name() == identifier


def test_drop_table_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test dropping a table in LocalStack."""
    namespace = "test_db"
    table_name = "test_table"
    identifier = (namespace, table_name)

    # Create namespace and table
    catalog.create_namespace(namespace)
    catalog.create_table(identifier, simple_schema)

    # Verify table exists
    assert catalog.table_exists(identifier)

    # Drop table
    catalog.drop_table(identifier)

    # Verify table no longer exists
    assert not catalog.table_exists(identifier)

    with pytest.raises(NoSuchTableError):
        catalog.load_table(identifier)


def test_rename_table_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test renaming a table in LocalStack."""
    namespace = "test_db"
    old_name = "old_table"
    new_name = "new_table"
    old_identifier = (namespace, old_name)
    new_identifier = (namespace, new_name)

    # Create namespace and table
    catalog.create_namespace(namespace)
    table = catalog.create_table(old_identifier, simple_schema)
    old_metadata_location = table.metadata_location

    # Rename table
    catalog.rename_table(old_identifier, new_identifier)

    # Verify new table exists
    new_table = catalog.load_table(new_identifier)
    assert new_table.name() == new_identifier
    assert new_table.metadata_location == old_metadata_location

    # Verify old table no longer exists
    with pytest.raises(NoSuchTableError):
        catalog.load_table(old_identifier)


def test_list_tables_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test listing tables in LocalStack."""
    namespace = "test_db"
    table_names = ["table1", "table2", "table3"]

    # Create namespace
    catalog.create_namespace(namespace)

    # Create tables
    for table_name in table_names:
        catalog.create_table((namespace, table_name), simple_schema)

    # List tables
    tables = catalog.list_tables(namespace)

    for table_name in table_names:
        assert (namespace, table_name) in tables


def test_duplicate_namespace_localstack(catalog: DynamoDbCatalog) -> None:
    """Test creating duplicate namespace raises error."""
    namespace = "test_namespace"
    catalog.create_namespace(namespace)

    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace(namespace)


def test_duplicate_table_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test creating duplicate table raises error."""
    namespace = "test_db"
    table_name = "test_table"
    identifier = (namespace, table_name)

    catalog.create_namespace(namespace)
    catalog.create_table(identifier, simple_schema)

    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table(identifier, simple_schema)


def test_drop_non_empty_namespace_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test that dropping a non-empty namespace raises error."""
    from pyiceberg.exceptions import NamespaceNotEmptyError

    namespace = "test_db"
    table_name = "test_table"
    identifier = (namespace, table_name)

    catalog.create_namespace(namespace)
    catalog.create_table(identifier, simple_schema)

    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace(namespace)


def test_cache_with_localstack(catalog: DynamoDbCatalog, simple_schema: Schema) -> None:
    """Test that caching works with LocalStack."""
    namespace = "test_db"
    table_name = "test_table"
    identifier = (namespace, table_name)

    catalog.create_namespace(namespace)
    catalog.create_table(identifier, simple_schema)

    # First load - should populate cache
    table1 = catalog.load_table(identifier)
    cache_key = catalog._get_cache_key(identifier)

    # Verify cache is populated
    if catalog._cache:
        assert catalog._cache.get(cache_key) is not None

    # Second load - should use cache
    table2 = catalog.load_table(identifier)
    assert table1.metadata_location == table2.metadata_location


def test_update_namespace_properties_localstack(catalog: DynamoDbCatalog) -> None:
    """Test updating namespace properties in LocalStack."""
    namespace = "test_namespace"
    initial_props = {"owner": "user1", "department": "engineering"}

    catalog.create_namespace(namespace, properties=initial_props)

    # Update properties
    updates = {"owner": "user2", "location": "s3://bucket/path"}
    removals = {"department"}

    report = catalog.update_namespace_properties(namespace, removals, updates)

    assert "owner" in report.updated
    assert "location" in report.updated
    assert "department" in report.removed

    # Verify updated properties
    props = catalog.load_namespace_properties(namespace)
    assert props["owner"] == "user2"
    assert props["location"] == "s3://bucket/path"
    assert "department" not in props


def test_load_non_existent_table_localstack(catalog: DynamoDbCatalog) -> None:
    """Test loading non-existent table raises error."""
    namespace = "test_db"
    table_name = "non_existent"
    identifier = (namespace, table_name)

    catalog.create_namespace(namespace)

    with pytest.raises(NoSuchTableError):
        catalog.load_table(identifier)


def test_drop_non_existent_namespace_localstack(catalog: DynamoDbCatalog) -> None:
    """Test dropping non-existent namespace raises error."""
    with pytest.raises(NoSuchNamespaceError):
        catalog.drop_namespace("non_existent")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
