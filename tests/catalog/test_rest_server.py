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
Tests for the Universal REST Catalog Server (dev/rest-server/main.py).

These tests validate the REST API endpoints for catalog operations including
namespaces, tables, and configuration management.
"""

import os
import tempfile
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, StringType


@pytest.fixture(scope="module")
def rest_server_app() -> Generator[TestClient, None, None]:
    """
    Create a FastAPI test client for the REST server.
    
    This fixture sets up an in-memory catalog for testing to avoid
    needing external dependencies like DynamoDB or LocalStack.
    """
    # Create a temporary config file for testing
    config_content = """catalog:
  test:
    type: sql
    uri: 'sqlite:///:memory:'
    warehouse: 'file:///tmp/warehouse'
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_path = f.name
    
    # Set environment variables for the REST server
    original_catalog_name = os.environ.get('ICEBERG_CATALOG_NAME')
    original_config_home = os.environ.get('HOME')
    
    os.environ['ICEBERG_CATALOG_NAME'] = 'test'
    
    # Create .pyiceberg.yaml in a temp directory
    temp_home = tempfile.mkdtemp()
    pyiceberg_yaml = os.path.join(temp_home, '.pyiceberg.yaml')
    with open(pyiceberg_yaml, 'w') as f:
        f.write(config_content)
    
    os.environ['HOME'] = temp_home
    
    try:
        # Import and create the FastAPI app
        # Note: This assumes the REST server main.py is importable
        # In a real scenario, we might need to adjust the import path
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../dev/rest-server'))
        
        from main import app
        
        client = TestClient(app)
        yield client
        
    finally:
        # Cleanup
        os.unlink(config_path)
        os.unlink(pyiceberg_yaml)
        os.rmdir(temp_home)
        
        if original_catalog_name:
            os.environ['ICEBERG_CATALOG_NAME'] = original_catalog_name
        else:
            os.environ.pop('ICEBERG_CATALOG_NAME', None)
            
        if original_config_home:
            os.environ['HOME'] = original_config_home


@pytest.fixture
def test_schema() -> Schema:
    """Create a test schema for table operations."""
    return Schema(
        fields=[
            PartitionField(name="id", field_id=1, type=LongType()),
            PartitionField(name="name", field_id=2, type=StringType()),
            PartitionField(name="category", field_id=3, type=StringType()),
        ]
    )


@pytest.fixture
def test_partition_spec() -> PartitionSpec:
    """Create a test partition spec."""
    return PartitionSpec(
        spec_id=0,
        fields=[
            PartitionField(
                source_id=3,
                field_id=1000,
                transform=IdentityTransform(),
                name="category"
            )
        ]
    )


# ============================================================================
# Health and Configuration Tests
# ============================================================================


def test_health_endpoint(rest_server_app: TestClient) -> None:
    """Test the health check endpoint."""
    response = rest_server_app.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"
    assert "catalog" in data
    assert data["catalog"] == "test"


def test_metrics_endpoint(rest_server_app: TestClient) -> None:
    """Test the metrics endpoint."""
    response = rest_server_app.get("/metrics")
    assert response.status_code == 200
    
    data = response.json()
    assert "uptime_seconds" in data
    assert "catalog_name" in data
    assert data["catalog_name"] == "test"


def test_get_config(rest_server_app: TestClient) -> None:
    """Test getting catalog configuration."""
    response = rest_server_app.get("/v1/config")
    assert response.status_code == 200
    
    data = response.json()
    assert "defaults" in data
    assert "overrides" in data

    # Check defaults contain catalog info
    defaults = data["defaults"]
    assert "warehouse" in defaults or "uri" in defaults


# ============================================================================
# Namespace Tests
# ============================================================================


def test_list_namespaces_empty(rest_server_app: TestClient) -> None:
    """Test listing namespaces when none exist."""
    response = rest_server_app.get("/v1/namespaces")
    assert response.status_code == 200
    
    data = response.json()
    assert "namespaces" in data
    assert isinstance(data["namespaces"], list)


def test_create_namespace(rest_server_app: TestClient) -> None:
    """Test creating a new namespace."""
    namespace_data = {
        "namespace": ["test_db"],
        "properties": {
            "owner": "test_user",
            "description": "Test database"
        }
    }
    
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    assert response.status_code == 200
    
    data = response.json()
    assert data["namespace"] == ["test_db"]
    assert data["properties"]["owner"] == "test_user"
    assert data["properties"]["description"] == "Test database"


def test_create_duplicate_namespace(rest_server_app: TestClient) -> None:
    """Test that creating a duplicate namespace returns an error."""
    namespace_data = {
        "namespace": ["duplicate_test"],
        "properties": {"owner": "user1"}
    }
    
    # Create first time
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    assert response.status_code == 200
    
    # Try to create again
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    assert response.status_code == 409  # Conflict


def test_list_namespaces_with_data(rest_server_app: TestClient) -> None:
    """Test listing namespaces after creating some."""
    # Create a namespace
    namespace_data = {
        "namespace": ["list_test_db"],
        "properties": {"owner": "test_user"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # List namespaces
    response = rest_server_app.get("/v1/namespaces")
    assert response.status_code == 200
    
    data = response.json()
    namespaces = data["namespaces"]
    assert any(ns == ["list_test_db"] for ns in namespaces)


def test_load_namespace(rest_server_app: TestClient) -> None:
    """Test loading a namespace by name."""
    # Create namespace first
    namespace_data = {
        "namespace": ["load_test_db"],
        "properties": {
            "owner": "test_user",
            "created_at": "2025-10-15"
        }
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Load namespace
    response = rest_server_app.get("/v1/namespaces/load_test_db")
    assert response.status_code == 200
    
    data = response.json()
    assert data["namespace"] == ["load_test_db"]
    assert data["properties"]["owner"] == "test_user"
    assert data["properties"]["created_at"] == "2025-10-15"


def test_load_nonexistent_namespace(rest_server_app: TestClient) -> None:
    """Test loading a namespace that doesn't exist."""
    response = rest_server_app.get("/v1/namespaces/nonexistent")
    assert response.status_code == 404


def test_update_namespace_properties(rest_server_app: TestClient) -> None:
    """Test updating namespace properties."""
    # Create namespace
    namespace_data = {
        "namespace": ["update_test_db"],
        "properties": {"owner": "original_user"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Update properties
    update_data = {
        "updates": {
            "owner": "new_user",
            "env": "production"
        }
    }
    response = rest_server_app.post(
        "/v1/namespaces/update_test_db/properties",
        json=update_data
    )
    assert response.status_code == 200
    
    data = response.json()
    assert "updated" in data
    assert "owner" in data["updated"]
    assert "env" in data["updated"]
    
    # Verify changes
    response = rest_server_app.get("/v1/namespaces/update_test_db")
    data = response.json()
    assert data["properties"]["owner"] == "new_user"
    assert data["properties"]["env"] == "production"


def test_update_namespace_properties_remove(rest_server_app: TestClient) -> None:
    """Test removing namespace properties."""
    # Create namespace with properties
    namespace_data = {
        "namespace": ["remove_prop_test"],
        "properties": {
            "owner": "user",
            "temp_property": "to_be_removed"
        }
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Remove property
    update_data = {
        "removals": ["temp_property"]
    }
    response = rest_server_app.post(
        "/v1/namespaces/remove_prop_test/properties",
        json=update_data
    )
    assert response.status_code == 200
    
    # Verify removal
    response = rest_server_app.get("/v1/namespaces/remove_prop_test")
    data = response.json()
    assert "temp_property" not in data["properties"]
    assert data["properties"]["owner"] == "user"


def test_drop_namespace(rest_server_app: TestClient) -> None:
    """Test dropping a namespace."""
    # Create namespace
    namespace_data = {
        "namespace": ["drop_test_db"],
        "properties": {"owner": "test_user"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Drop namespace
    response = rest_server_app.delete("/v1/namespaces/drop_test_db")
    assert response.status_code == 204
    
    # Verify it's gone
    response = rest_server_app.get("/v1/namespaces/drop_test_db")
    assert response.status_code == 404


def test_drop_nonexistent_namespace(rest_server_app: TestClient) -> None:
    """Test dropping a namespace that doesn't exist."""
    response = rest_server_app.delete("/v1/namespaces/nonexistent_namespace")
    assert response.status_code == 404


# ============================================================================
# Table Tests
# ============================================================================


def test_list_tables_empty(rest_server_app: TestClient) -> None:
    """Test listing tables in an empty namespace."""
    # Create namespace
    namespace_data = {
        "namespace": ["empty_table_ns"],
        "properties": {"owner": "test"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # List tables
    response = rest_server_app.get("/v1/namespaces/empty_table_ns/tables")
    assert response.status_code == 200
    
    data = response.json()
    assert "identifiers" in data
    assert data["identifiers"] == []


def test_list_tables_nonexistent_namespace(rest_server_app: TestClient) -> None:
    """Test listing tables in a namespace that doesn't exist."""
    response = rest_server_app.get("/v1/namespaces/nonexistent_ns/tables")
    assert response.status_code == 404


def test_table_exists_check(rest_server_app: TestClient) -> None:
    """Test checking if a table exists."""
    # Create namespace
    namespace_data = {
        "namespace": ["exists_check_ns"],
        "properties": {"owner": "test"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Check non-existent table
    response = rest_server_app.head("/v1/namespaces/exists_check_ns/tables/nonexistent")
    assert response.status_code == 404


def test_load_nonexistent_table(rest_server_app: TestClient) -> None:
    """Test loading a table that doesn't exist."""
    # Create namespace
    namespace_data = {
        "namespace": ["load_table_ns"],
        "properties": {"owner": "test"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Try to load table
    response = rest_server_app.get("/v1/namespaces/load_table_ns/tables/nonexistent_table")
    assert response.status_code == 404


def test_drop_nonexistent_table(rest_server_app: TestClient) -> None:
    """Test dropping a table that doesn't exist."""
    # Create namespace
    namespace_data = {
        "namespace": ["drop_table_ns"],
        "properties": {"owner": "test"}
    }
    rest_server_app.post("/v1/namespaces", json=namespace_data)
    
    # Try to drop table
    response = rest_server_app.delete("/v1/namespaces/drop_table_ns/tables/nonexistent_table")
    assert response.status_code == 404


# ============================================================================
# Multi-level Namespace Tests
# ============================================================================


def test_multi_level_namespace(rest_server_app: TestClient) -> None:
    """Test creating and managing multi-level namespaces."""
    # Create multi-level namespace
    namespace_data = {
        "namespace": ["org", "department", "team"],
        "properties": {"owner": "team_lead"}
    }
    
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    assert response.status_code == 200
    
    # Load it back
    response = rest_server_app.get("/v1/namespaces/org.department.team")
    assert response.status_code == 200
    
    data = response.json()
    assert data["namespace"] == ["org", "department", "team"]


def test_list_namespaces_with_parent(rest_server_app: TestClient) -> None:
    """Test listing namespaces under a parent namespace."""
    # Create parent namespace
    parent_data = {
        "namespace": ["parent"],
        "properties": {"type": "parent"}
    }
    rest_server_app.post("/v1/namespaces", json=parent_data)
    
    # Create child namespace
    child_data = {
        "namespace": ["parent", "child"],
        "properties": {"type": "child"}
    }
    rest_server_app.post("/v1/namespaces", json=child_data)
    
    # List with parent filter
    response = rest_server_app.get("/v1/namespaces?parent=parent")
    assert response.status_code == 200
    
    data = response.json()
    # Should only show child under parent
    assert any(ns == ["parent", "child"] for ns in data["namespaces"])


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_invalid_namespace_format(rest_server_app: TestClient) -> None:
    """Test creating a namespace with invalid format."""
    # Empty namespace array
    namespace_data = {
        "namespace": [],
        "properties": {}
    }
    
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    # Should return error (400 or 500 depending on validation)
    assert response.status_code >= 400


def test_missing_namespace_field(rest_server_app: TestClient) -> None:
    """Test creating a namespace without required fields."""
    namespace_data = {
        "properties": {"owner": "test"}
        # Missing "namespace" field
    }
    
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    assert response.status_code == 422  # Unprocessable Entity


def test_invalid_json(rest_server_app: TestClient) -> None:
    """Test sending invalid JSON to endpoints."""
    response = rest_server_app.post(
        "/v1/namespaces",
        data="not valid json",
        headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422


# ============================================================================
# Integration Tests
# ============================================================================


def test_complete_workflow(rest_server_app: TestClient) -> None:
    """
    Test a complete workflow: create namespace, create table, query, drop.
    
    This is an end-to-end test that validates the entire API flow.
    """
    # Step 1: Create namespace
    namespace_data = {
        "namespace": ["workflow_test"],
        "properties": {
            "owner": "data_team",
            "env": "test"
        }
    }
    response = rest_server_app.post("/v1/namespaces", json=namespace_data)
    assert response.status_code == 200
    
    # Step 2: Verify namespace exists
    response = rest_server_app.get("/v1/namespaces/workflow_test")
    assert response.status_code == 200
    
    # Step 3: List tables (should be empty)
    response = rest_server_app.get("/v1/namespaces/workflow_test/tables")
    assert response.status_code == 200
    assert response.json()["identifiers"] == []
    
    # Step 4: Update namespace properties
    update_data = {
        "updates": {
            "version": "1.0",
            "updated_by": "test_user"
        }
    }
    response = rest_server_app.post(
        "/v1/namespaces/workflow_test/properties",
        json=update_data
    )
    assert response.status_code == 200
    
    # Step 5: Verify updates
    response = rest_server_app.get("/v1/namespaces/workflow_test")
    data = response.json()
    assert data["properties"]["version"] == "1.0"
    assert data["properties"]["updated_by"] == "test_user"
    
    # Step 6: Drop namespace
    response = rest_server_app.delete("/v1/namespaces/workflow_test")
    assert response.status_code == 204
    
    # Step 7: Verify namespace is gone
    response = rest_server_app.get("/v1/namespaces/workflow_test")
    assert response.status_code == 404


def test_multiple_namespaces_isolation(rest_server_app: TestClient) -> None:
    """
    Test that multiple namespaces are properly isolated.
    
    Creates multiple namespaces and verifies operations on one
    don't affect others.
    """
    # Create multiple namespaces
    namespaces = ["ns1", "ns2", "ns3"]
    for ns in namespaces:
        namespace_data = {
            "namespace": [ns],
            "properties": {"owner": f"{ns}_owner"}
        }
        response = rest_server_app.post("/v1/namespaces", json=namespace_data)
        assert response.status_code == 200
    
    # Verify all exist
    response = rest_server_app.get("/v1/namespaces")
    data = response.json()
    namespace_names = [ns[0] for ns in data["namespaces"]]
    for ns in namespaces:
        assert ns in namespace_names
    
    # Update one namespace
    update_data = {"updates": {"modified": "true"}}
    response = rest_server_app.post(
        "/v1/namespaces/ns1/properties",
        json=update_data
    )
    assert response.status_code == 200
    
    # Verify others unchanged
    response = rest_server_app.get("/v1/namespaces/ns2")
    data = response.json()
    assert "modified" not in data["properties"]
    
    # Drop one namespace
    response = rest_server_app.delete("/v1/namespaces/ns1")
    assert response.status_code == 204
    
    # Verify others still exist
    response = rest_server_app.get("/v1/namespaces/ns2")
    assert response.status_code == 200
    response = rest_server_app.get("/v1/namespaces/ns3")
    assert response.status_code == 200


# ============================================================================
# Performance / Stress Tests (Optional)
# ============================================================================


def test_create_many_namespaces(rest_server_app: TestClient) -> None:
    """
    Test creating many namespaces to check performance.
    
    This is a basic stress test to ensure the server can handle
    multiple operations.
    """
    num_namespaces = 50
    
    for i in range(num_namespaces):
        namespace_data = {
            "namespace": [f"perf_test_{i}"],
            "properties": {"index": str(i)}
        }
        response = rest_server_app.post("/v1/namespaces", json=namespace_data)
        assert response.status_code == 200
    
    # Verify all exist
    response = rest_server_app.get("/v1/namespaces")
    assert response.status_code == 200
    
    data = response.json()
    namespace_names = [ns[0] for ns in data["namespaces"]]
    
    # Check at least some of our test namespaces exist
    count = sum(1 for ns in namespace_names if ns.startswith("perf_test_"))
    assert count == num_namespaces
