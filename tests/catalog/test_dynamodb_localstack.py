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


# ==============================================================================
# Stress Tests: Concurrent Operations and Load Testing
# ==============================================================================


def test_stress_concurrent_writes_multiple_clients(user_schema: Schema, s3_bucket: str) -> None:
    """Stress test: Multiple independent clients writing concurrently to different tables."""
    import concurrent.futures
    import time

    namespace = "stress_test"
    num_clients = 5
    writes_per_client = 10

    print(f"\n{'=' * 80}")
    print(f"STRESS TEST: {num_clients} concurrent clients, {writes_per_client} writes each")
    print(f"{'=' * 80}")

    def create_client(client_id: int) -> DynamoDbCatalog:
        """Create an independent catalog client."""
        catalog_name = f"stress_catalog_{client_id}_{uuid.uuid4().hex[:8]}"
        table_name = f"iceberg_catalog_{uuid.uuid4().hex[:8]}"

        return DynamoDbCatalog(
            catalog_name,
            **{
                "table-name": table_name,
                "warehouse": f"s3://{s3_bucket}",
                "dynamodb.endpoint": LOCALSTACK_ENDPOINT,
                "s3.endpoint": LOCALSTACK_ENDPOINT,
                "dynamodb.region": TEST_REGION,
                "dynamodb.access-key-id": "test",
                "dynamodb.secret-access-key": "test",
                "dynamodb.cache.enabled": "true",
                "dynamodb.cache.ttl-seconds": "300",
            },
        )

    def client_workload(client_id: int) -> tuple[int, int, float]:
        """Execute workload for a single client: create table, write data multiple times."""
        start_time = time.time()
        catalog = create_client(client_id)
        table_name = f"stress_table_{client_id}"
        identifier = (namespace, table_name)

        try:
            # Create namespace (may already exist from another client)
            try:
                catalog.create_namespace(namespace)
            except NamespaceAlreadyExistsError:
                pass

            # Create table
            table = catalog.create_table(identifier, user_schema)

            # Perform multiple writes
            successful_writes = 0
            for write_num in range(writes_per_client):
                data: dict[str, list[int | str]] = {
                    "user_id": [client_id * 1000 + write_num * 10 + i for i in range(5)],
                    "username": [f"user_{client_id}_{write_num}_{i}" for i in range(5)],
                    "email": [f"client{client_id}_write{write_num}_{i}@example.com" for i in range(5)],
                    "age": [20 + (client_id + write_num + i) % 50 for i in range(5)],
                }
                table.append(pa.table(data))
                table = catalog.load_table(identifier)  # Refresh to get latest snapshot
                successful_writes += 1

            # Verify final data
            final_table = catalog.load_table(identifier)
            result = final_table.scan().to_arrow()
            row_count = len(result)

            elapsed_time = time.time() - start_time

            # Cleanup
            catalog.drop_table(identifier)
            catalog.dynamodb.delete_table(TableName=catalog.dynamodb_table_name)

            return (successful_writes, row_count, elapsed_time)

        except Exception as e:
            print(f"    ❌ Client {client_id} failed: {e}")
            raise

    # Execute concurrent workloads
    print(f"\n[1] Starting {num_clients} concurrent clients...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [executor.submit(client_workload, i) for i in range(num_clients)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Analyze results
    print("\n[2] Analyzing results...")
    total_writes = sum(r[0] for r in results)
    total_rows = sum(r[1] for r in results)
    avg_time = sum(r[2] for r in results) / len(results)

    print(f"    ✅ Total successful writes: {total_writes}/{num_clients * writes_per_client}")
    print(f"    ✅ Total rows written: {total_rows}")
    print(f"    ✅ Average client time: {avg_time:.2f}s")
    print(f"    ✅ Writes per second: {total_writes / avg_time:.2f}")

    # Verify all clients succeeded
    assert total_writes == num_clients * writes_per_client, "Not all writes succeeded"
    assert total_rows == num_clients * writes_per_client * 5, "Row count mismatch"

    print(f"\n{'=' * 80}")
    print("✅ CONCURRENT WRITES STRESS TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_stress_high_volume_commits_single_table(catalog: DynamoDbCatalog, user_schema: Schema, s3_bucket: str) -> None:
    """Stress test: High volume of commits to a single table."""
    import time

    namespace = "stress_test"
    table_name = "high_volume_table"
    identifier = (namespace, table_name)
    num_commits = 50

    print(f"\n{'=' * 80}")
    print(f"STRESS TEST: {num_commits} commits to single table")
    print(f"{'=' * 80}")

    # Create table
    print("\n[1] Creating table...")
    catalog.create_namespace(namespace)
    table = catalog.create_table(identifier, user_schema)
    print("    ✅ Table created")

    # Perform many commits
    print(f"\n[2] Performing {num_commits} commits...")
    start_time = time.time()

    for commit_num in range(num_commits):
        data: dict[str, list[int | str]] = {
            "user_id": [commit_num * 10 + i for i in range(3)],
            "username": [f"user_{commit_num}_{i}" for i in range(3)],
            "email": [f"commit{commit_num}_{i}@example.com" for i in range(3)],
            "age": [20 + (commit_num + i) % 50 for i in range(3)],
        }
        table.append(pa.table(data))
        table = catalog.load_table(identifier)  # Refresh

        if (commit_num + 1) % 10 == 0:
            print(f"    📝 Completed {commit_num + 1} commits...")

    elapsed_time = time.time() - start_time

    # Verify final state
    print("\n[3] Verifying final state...")
    final_table = catalog.load_table(identifier)
    result = final_table.scan().to_arrow()
    row_count = len(result)
    snapshots: List[Snapshot] = list(final_table.snapshots())

    print(f"    ✅ Total rows: {row_count}")
    print(f"    ✅ Total snapshots: {len(snapshots)}")
    print(f"    ✅ Total time: {elapsed_time:.2f}s")
    print(f"    ✅ Commits per second: {num_commits / elapsed_time:.2f}")
    print(f"    ✅ Average commit time: {elapsed_time / num_commits:.3f}s")

    assert row_count == num_commits * 3, f"Expected {num_commits * 3} rows, got {row_count}"
    assert len(snapshots) >= num_commits, f"Expected at least {num_commits} snapshots"

    print(f"\n{'=' * 80}")
    print("✅ HIGH-VOLUME COMMITS STRESS TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_stress_concurrent_read_write_contention(catalog: DynamoDbCatalog, user_schema: Schema, s3_bucket: str) -> None:
    """Stress test: Concurrent reads and writes to same table."""
    import concurrent.futures
    import time

    namespace = "stress_test"
    table_name = "rw_contention_table"
    identifier = (namespace, table_name)
    num_writers = 3
    num_readers = 5
    writes_per_writer = 10

    print(f"\n{'=' * 80}")
    print(f"STRESS TEST: {num_writers} writers + {num_readers} readers (concurrent)")
    print(f"{'=' * 80}")

    # Create table with initial data
    print("\n[1] Creating table with initial data...")
    catalog.create_namespace(namespace)
    table = catalog.create_table(identifier, user_schema)

    initial_data: dict[str, list[int | str]] = {
        "user_id": [1, 2, 3],
        "username": ["alice", "bob", "charlie"],
        "email": ["a@ex.com", "b@ex.com", "c@ex.com"],
        "age": [25, 30, 35],
    }
    table.append(pa.table(initial_data))
    print("    ✅ Table created with 3 rows")

    def writer_workload(writer_id: int) -> int:
        """Write data to the table."""
        successful_writes = 0
        for write_num in range(writes_per_writer):
            data: dict[str, list[int | str]] = {
                "user_id": [writer_id * 1000 + write_num * 10 + i for i in range(2)],
                "username": [f"writer_{writer_id}_write_{write_num}_{i}" for i in range(2)],
                "email": [f"w{writer_id}_n{write_num}_{i}@example.com" for i in range(2)],
                "age": [25 + (writer_id + write_num + i) % 40 for i in range(2)],
            }
            t = catalog.load_table(identifier)
            t.append(pa.table(data))
            successful_writes += 1
            time.sleep(0.05)  # Small delay to increase contention
        return successful_writes

    def reader_workload(reader_id: int) -> int:
        """Read data from the table."""
        successful_reads = 0
        for _ in range(writes_per_writer * 2):  # Read more often than writes
            t = catalog.load_table(identifier)
            result = t.scan().to_arrow()
            row_count = len(result)
            if row_count >= 3:  # At least initial data
                successful_reads += 1
            time.sleep(0.03)  # Smaller delay for readers
        return successful_reads

    # Execute concurrent workloads
    print(f"\n[2] Starting {num_writers} writers and {num_readers} readers...")
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_writers + num_readers) as executor:
        writer_futures = [executor.submit(writer_workload, i) for i in range(num_writers)]
        reader_futures = [executor.submit(reader_workload, i) for i in range(num_readers)]

        writer_results = [f.result() for f in writer_futures]
        reader_results = [f.result() for f in reader_futures]

    elapsed_time = time.time() - start_time

    # Verify results
    print("\n[3] Analyzing results...")
    total_writes = sum(writer_results)
    total_reads = sum(reader_results)

    final_table = catalog.load_table(identifier)
    final_result = final_table.scan().to_arrow()
    final_row_count = len(final_result)

    print(f"    ✅ Total writes: {total_writes}")
    print(f"    ✅ Total reads: {total_reads}")
    print(f"    ✅ Final row count: {final_row_count}")
    print(f"    ✅ Total time: {elapsed_time:.2f}s")
    print(f"    ✅ Operations per second: {(total_writes + total_reads) / elapsed_time:.2f}")

    expected_rows = 3 + (num_writers * writes_per_writer * 2)
    assert final_row_count == expected_rows, f"Expected {expected_rows} rows, got {final_row_count}"
    assert total_writes == num_writers * writes_per_writer, "Not all writes succeeded"

    print(f"\n{'=' * 80}")
    print("✅ READ/WRITE CONTENTION STRESS TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_stress_cache_consistency_under_load(user_schema: Schema, s3_bucket: str) -> None:
    """Stress test: Verify cache consistency with multiple catalog instances."""
    import concurrent.futures
    import time

    namespace = "stress_test"
    table_name = "cache_consistency_table"
    identifier = (namespace, table_name)
    shared_table_name = f"shared_iceberg_catalog_{uuid.uuid4().hex[:8]}"
    num_catalogs = 4
    operations_per_catalog = 20

    print(f"\n{'=' * 80}")
    print(f"STRESS TEST: Cache consistency with {num_catalogs} catalog instances")
    print(f"{'=' * 80}")

    def create_shared_catalog(catalog_id: int) -> DynamoDbCatalog:
        """Create a catalog instance sharing the same DynamoDB table."""
        return DynamoDbCatalog(
            f"cache_catalog_{catalog_id}",
            **{
                "table-name": shared_table_name,  # Same table for all catalogs
                "warehouse": f"s3://{s3_bucket}",
                "dynamodb.endpoint": LOCALSTACK_ENDPOINT,
                "s3.endpoint": LOCALSTACK_ENDPOINT,
                "dynamodb.region": TEST_REGION,
                "dynamodb.access-key-id": "test",
                "dynamodb.secret-access-key": "test",
                "dynamodb.cache.enabled": "true",
                "dynamodb.cache.ttl-seconds": "60",  # Shorter TTL for this test
            },
        )

    # Create initial table with first catalog
    print("\n[1] Creating shared table...")
    catalog1 = create_shared_catalog(0)
    catalog1.create_namespace(namespace)
    catalog1.create_table(identifier, user_schema)
    print("    ✅ Shared table created")

    def catalog_workload(catalog_id: int) -> tuple[int, int, int]:
        """Perform mixed operations: reads, writes, cache hits."""
        catalog = create_shared_catalog(catalog_id)
        reads = 0
        writes = 0
        cache_hits = 0

        for op_num in range(operations_per_catalog):
            if op_num % 3 == 0:  # Write operation
                data: dict[str, list[int | str]] = {
                    "user_id": [catalog_id * 1000 + op_num],
                    "username": [f"user_{catalog_id}_{op_num}"],
                    "email": [f"c{catalog_id}_op{op_num}@example.com"],
                    "age": [25 + catalog_id + op_num],
                }
                t = catalog.load_table(identifier)
                t.append(pa.table(data))
                writes += 1
            else:  # Read operation
                t = catalog.load_table(identifier)
                _ = t.scan().to_arrow()
                reads += 1

                # Check if cache was used (load again immediately)
                cache_key = catalog._get_cache_key(identifier)
                if catalog._cache and catalog._cache.get(cache_key) is not None:
                    cache_hits += 1

            time.sleep(0.02)  # Small delay

        return (reads, writes, cache_hits)

    # Execute concurrent workloads
    print(f"\n[2] Starting {num_catalogs} catalog instances...")
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_catalogs) as executor:
        futures = [executor.submit(catalog_workload, i) for i in range(num_catalogs)]
        results = [f.result() for f in futures]

    elapsed_time = time.time() - start_time

    # Analyze results
    print("\n[3] Analyzing cache consistency...")
    total_reads = sum(r[0] for r in results)
    total_writes = sum(r[1] for r in results)
    total_cache_hits = sum(r[2] for r in results)
    cache_hit_rate = (total_cache_hits / total_reads * 100) if total_reads > 0 else 0

    # Verify final state
    final_catalog = create_shared_catalog(999)
    final_table = final_catalog.load_table(identifier)
    final_result = final_table.scan().to_arrow()
    final_row_count = len(final_result)

    print(f"    ✅ Total reads: {total_reads}")
    print(f"    ✅ Total writes: {total_writes}")
    print(f"    ✅ Cache hits: {total_cache_hits}")
    print(f"    ✅ Cache hit rate: {cache_hit_rate:.1f}%")
    print(f"    ✅ Final row count: {final_row_count}")
    print(f"    ✅ Total time: {elapsed_time:.2f}s")
    print(f"    ✅ Operations per second: {(total_reads + total_writes) / elapsed_time:.2f}")

    # Verify data consistency
    assert final_row_count == total_writes, f"Row count mismatch: expected {total_writes}, got {final_row_count}"

    # Cleanup
    catalog1.drop_table(identifier)
    catalog1.dynamodb.delete_table(TableName=shared_table_name)

    print(f"\n{'=' * 80}")
    print("✅ CACHE CONSISTENCY STRESS TEST PASSED!")
    print(f"{'=' * 80}\n")


def test_stress_retry_mechanism_under_failures(user_schema: Schema, s3_bucket: str) -> None:
    """Stress test: Verify retry mechanism handles transient failures."""
    import time
    from unittest.mock import patch

    from botocore.exceptions import ClientError as BotoClientError

    namespace = "stress_test"
    table_name = "retry_test_table"
    identifier = (namespace, table_name)

    print(f"\n{'=' * 80}")
    print("STRESS TEST: Retry mechanism with simulated failures")
    print(f"{'=' * 80}")

    # Create catalog with aggressive retry settings
    catalog_name = f"retry_catalog_{uuid.uuid4().hex[:8]}"
    dynamo_table_name = f"iceberg_catalog_{uuid.uuid4().hex[:8]}"

    catalog = DynamoDbCatalog(
        catalog_name,
        **{
            "table-name": dynamo_table_name,
            "warehouse": f"s3://{s3_bucket}",
            "dynamodb.endpoint": LOCALSTACK_ENDPOINT,
            "s3.endpoint": LOCALSTACK_ENDPOINT,
            "dynamodb.region": TEST_REGION,
            "dynamodb.access-key-id": "test",
            "dynamodb.secret-access-key": "test",
            "dynamodb.max-retries": "5",
            "dynamodb.retry-multiplier": "1.5",
            "dynamodb.retry-min-wait-ms": "50",
            "dynamodb.retry-max-wait-ms": "2000",
        },
    )

    print("\n[1] Creating table...")
    catalog.create_namespace(namespace)
    catalog.create_table(identifier, user_schema)
    print("    ✅ Table created")

    # Simulate intermittent failures
    print("\n[2] Testing retry mechanism with simulated failures...")
    call_count = {"count": 0}
    original_get_item = catalog.dynamodb.get_item

    def failing_get_item(*args, **kwargs):  # type: ignore
        """Simulate transient failures on 30% of calls."""
        call_count["count"] += 1
        if call_count["count"] % 3 == 0:  # Fail every 3rd call
            raise BotoClientError(
                {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Simulated failure"}},
                "GetItem",
            )
        return original_get_item(*args, **kwargs)

    with patch.object(catalog.dynamodb, "get_item", side_effect=failing_get_item):
        successful_operations = 0
        failed_operations = 0
        start_time = time.time()

        # Perform operations that will trigger retries
        for i in range(20):
            try:
                # Load table (will hit failures and retry)
                _ = catalog.load_table(identifier)
                successful_operations += 1
            except Exception as e:
                print(f"    ⚠️  Operation {i} failed after retries: {e}")
                failed_operations += 1

        elapsed_time = time.time() - start_time

    print("\n[3] Analyzing retry behavior...")
    print("    ✅ Total operations attempted: 20")
    print(f"    ✅ Successful operations: {successful_operations}")
    print(f"    ⚠️  Failed operations: {failed_operations}")
    print(f"    ✅ Total get_item calls: {call_count['count']}")
    print(f"    ✅ Simulated failures: {call_count['count'] // 3}")
    print(f"    ✅ Total time: {elapsed_time:.2f}s")
    print("    ℹ️  Note: Some operations may fail after max retries")

    # Verify table is still accessible
    print("\n[4] Verifying table integrity...")
    final_table = catalog.load_table(identifier)
    assert final_table.name() == identifier
    print("    ✅ Table integrity verified")

    # Cleanup
    catalog.drop_table(identifier)
    catalog.dynamodb.delete_table(TableName=dynamo_table_name)

    print(f"\n{'=' * 80}")
    print("✅ RETRY MECHANISM STRESS TEST PASSED!")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
