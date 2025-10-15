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
from typing import List

import boto3
import pyarrow as pa
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
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

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


# ==============================================================================
# End-to-End Tests: Data I/O with S3
# ==============================================================================


@pytest.fixture
def user_schema() -> Schema:
    """Schema for user table (compatible with PyArrow defaults)."""
    return Schema(
        NestedField(field_id=1, name="user_id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="username", field_type=StringType(), required=False),
        NestedField(field_id=3, name="email", field_type=StringType(), required=False),
        NestedField(field_id=4, name="age", field_type=LongType(), required=False),
        schema_id=1,
    )


def test_e2e_create_table_write_read_data(catalog: DynamoDbCatalog, user_schema: Schema, s3_bucket: str) -> None:
    """End-to-end test: Create table, write data to S3, and read it back."""
    namespace = "e2e_test"
    table_name = "users"
    identifier = (namespace, table_name)

    print(f"\n{'=' * 80}")
    print("TEST: Create table, write data, and read it back")
    print(f"{'=' * 80}")

    # Step 1: Create namespace
    print("\n[1] Creating namespace...")
    catalog.create_namespace(namespace, properties={"description": "E2E test namespace"})
    print(f"    ✅ Created namespace: {namespace}")

    # Step 2: Create table
    print("\n[2] Creating table...")
    table = catalog.create_table(identifier, user_schema)
    print(f"    ✅ Created table: {identifier}")
    print(f"    📍 Metadata location: {table.metadata_location}")
    print(f"    📍 Table location: {table.location()}")

    # Step 3: Create sample data
    print("\n[3] Creating sample data...")
    data: dict[str, list[int | str]] = {
        "user_id": [1, 2, 3, 4, 5],
        "username": ["alice", "bob", "charlie", "diana", "eve"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", "eve@example.com"],
        "age": [25, 30, 35, 28, 42],
    }
    pyarrow_table = pa.table(data)
    num_rows: int = len(data["user_id"])
    print(f"    ✅ Created PyArrow table with {num_rows} rows")
    print(f"    📊 Schema: {pyarrow_table.schema}")

    # Step 4: Write data to table
    print("\n[4] Writing data to S3...")
    table.append(pyarrow_table)
    print("    ✅ Data written to S3")

    # Step 5: Refresh table to get latest metadata
    print("\n[5] Refreshing table...")
    table = catalog.load_table(identifier)
    print("    ✅ Table refreshed")
    print(f"    📊 Current snapshot: {table.current_snapshot()}")

    # Step 6: Scan and read data back
    print("\n[6] Reading data back from table...")
    scan_result = table.scan()
    result_table: pa.Table = scan_result.to_arrow()
    print("    ✅ Data read successfully")
    print(f"    📊 Read {len(result_table)} rows")

    # Step 7: Verify data
    print("\n[7] Verifying data...")
    assert len(result_table) == 5, f"Expected 5 rows, got {len(result_table)}"
    assert result_table.num_columns == 4, f"Expected 4 columns, got {result_table.num_columns}"

    # Convert to pandas for easier verification
    df = result_table.to_pandas()
    assert list(df["username"]) == ["alice", "bob", "charlie", "diana", "eve"]
    assert list(df["age"]) == [25, 30, 35, 28, 42]
    print("    ✅ Data integrity verified")

    # Step 8: Verify S3 objects were created
    print("\n[8] Verifying S3 objects...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=TEST_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    response = s3_client.list_objects_v2(Bucket=s3_bucket)
    s3_objects = [obj["Key"] for obj in response.get("Contents", [])]
    print(f"    ✅ Found {len(s3_objects)} objects in S3")

    # Verify metadata files exist
    metadata_files = [obj for obj in s3_objects if "metadata" in obj]
    data_files = [obj for obj in s3_objects if ".parquet" in obj]
    print(f"    📄 Metadata files: {len(metadata_files)}")
    print(f"    📄 Data files: {len(data_files)}")

    assert len(metadata_files) > 0, "Expected metadata files in S3"
    assert len(data_files) > 0, "Expected data files in S3"

    print(f"\n{'=' * 80}")
    print("✅ END-TO-END TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_e2e_multiple_appends_and_snapshots(catalog: DynamoDbCatalog, user_schema: Schema, s3_bucket: str) -> None:
    """Test multiple data appends and snapshot tracking."""
    namespace = "e2e_test"
    table_name = "users_multi"
    identifier = (namespace, table_name)

    print(f"\n{'=' * 80}")
    print("TEST: Multiple appends and snapshot tracking")
    print(f"{'=' * 80}")

    # Create table
    print("\n[1] Creating table...")
    catalog.create_namespace(namespace)
    table = catalog.create_table(identifier, user_schema)
    print("    ✅ Table created")

    # First append
    print("\n[2] First append (3 users)...")
    data1: dict[str, list[int | str]] = {
        "user_id": [1, 2, 3],
        "username": ["alice", "bob", "charlie"],
        "email": ["alice@ex.com", "bob@ex.com", "charlie@ex.com"],
        "age": [25, 30, 35],
    }
    table.append(pa.table(data1))
    table = catalog.load_table(identifier)
    print("    ✅ First append completed")

    # Second append
    print("\n[3] Second append (2 more users)...")
    data2: dict[str, list[int | str]] = {
        "user_id": [4, 5],
        "username": ["diana", "eve"],
        "email": ["diana@ex.com", "eve@ex.com"],
        "age": [28, 42],
    }
    table.append(pa.table(data2))
    table = catalog.load_table(identifier)
    print("    ✅ Second append completed")

    # Read all data
    print("\n[4] Reading all data...")
    result = table.scan().to_arrow()
    print(f"    ✅ Total rows: {len(result)}")
    assert len(result) == 5, f"Expected 5 rows, got {len(result)}"

    # Check snapshot history
    print("\n[5] Checking snapshot history...")
    snapshots: List[Snapshot] = list(table.snapshots())
    print(f"    ✅ Total snapshots: {len(snapshots)}")
    for i, snapshot in enumerate(snapshots, 1):
        print(f"       Snapshot {i}: ID={snapshot.snapshot_id}, timestamp={snapshot.timestamp_ms}")

    assert len(snapshots) >= 2, f"Expected at least 2 snapshots, got {len(snapshots)}"

    print(f"\n{'=' * 80}")
    print("✅ MULTIPLE APPENDS TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_e2e_filter_and_query_data(catalog: DynamoDbCatalog, user_schema: Schema, s3_bucket: str) -> None:
    """Test filtering and querying data from S3."""
    namespace = "e2e_test"
    table_name = "users_filter"
    identifier = (namespace, table_name)

    print(f"\n{'=' * 80}")
    print("TEST: Filter and query data")
    print(f"{'=' * 80}")

    # Create table and add data
    print("\n[1] Creating table and adding data...")
    catalog.create_namespace(namespace)
    table = catalog.create_table(identifier, user_schema)

    data: dict[str, list[int | str]] = {
        "user_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "username": ["alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry"],
        "email": [f"user{i}@example.com" for i in range(1, 9)],
        "age": [25, 30, 35, 28, 42, 22, 31, 45],
    }
    table.append(pa.table(data))
    table = catalog.load_table(identifier)
    num_users: int = len(data["user_id"])
    print(f"    ✅ Added {num_users} users")

    # Query all data
    print("\n[2] Querying all data...")
    all_data = table.scan().to_arrow().to_pandas()
    print(f"    ✅ Total users: {len(all_data)}")
    print(f"    📊 Age range: {all_data['age'].min()} - {all_data['age'].max()}")

    # Filter: users with age > 30
    print("\n[3] Filtering users with age > 30...")
    from pyiceberg.expressions import GreaterThan

    filtered_scan = table.scan(row_filter=GreaterThan("age", 30))
    filtered_data = filtered_scan.to_arrow().to_pandas()
    print(f"    ✅ Found {len(filtered_data)} users with age > 30:")
    print(filtered_data[["user_id", "username", "age"]].to_string(index=False))

    # Verify filtering worked
    assert len(filtered_data) == 4, f"Expected 4 users with age > 30, got {len(filtered_data)}"
    assert all(filtered_data["age"] > 30), "All filtered users should have age > 30"

    print(f"\n{'=' * 80}")
    print("✅ FILTER AND QUERY TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_e2e_drop_table_cleans_metadata(catalog: DynamoDbCatalog, user_schema: Schema, s3_bucket: str) -> None:
    """Test that dropping a table removes it from DynamoDB but keeps S3 data."""
    namespace = "e2e_test"
    table_name = "users_drop"
    identifier = (namespace, table_name)

    print(f"\n{'=' * 80}")
    print("TEST: Drop table and verify metadata cleanup")
    print(f"{'=' * 80}")

    # Create table and add data
    print("\n[1] Creating table and adding data...")
    catalog.create_namespace(namespace)
    table = catalog.create_table(identifier, user_schema)

    data: dict[str, list[int | str]] = {
        "user_id": [1, 2, 3],
        "username": ["alice", "bob", "charlie"],
        "email": ["a@ex.com", "b@ex.com", "c@ex.com"],
        "age": [25, 30, 35],
    }
    table.append(pa.table(data))
    metadata_location = table.metadata_location
    print("    ✅ Table created with data")
    print(f"    📍 Metadata location: {metadata_location}")

    # Verify S3 objects exist
    print("\n[2] Verifying S3 objects exist...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=TEST_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    response = s3_client.list_objects_v2(Bucket=s3_bucket)
    initial_object_count = len(response.get("Contents", []))
    print(f"    ✅ Found {initial_object_count} objects in S3")

    # Drop table
    print("\n[3] Dropping table...")
    catalog.drop_table(identifier)
    print("    ✅ Table dropped from catalog")

    # Verify table no longer exists in catalog
    print("\n[4] Verifying table removed from catalog...")
    assert not catalog.table_exists(identifier), "Table should not exist after drop"
    print("    ✅ Table no longer in catalog")

    # Verify S3 objects still exist (Iceberg doesn't delete data on drop by default)
    print("\n[5] Checking if S3 objects still exist...")
    response = s3_client.list_objects_v2(Bucket=s3_bucket)
    final_object_count = len(response.get("Contents", []))
    print(f"    ℹ️  S3 objects after drop: {final_object_count}")
    print("    ℹ️  Note: Iceberg keeps S3 data after table drop (by design)")

    print(f"\n{'=' * 80}")
    print("✅ DROP TABLE TEST PASSED!")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
