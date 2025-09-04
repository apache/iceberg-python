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
import threading
import uuid
from typing import Any, Dict, List
from unittest.mock import Mock
from uuid import uuid4

from pyiceberg.table.update.snapshot import ManageSnapshots


def test_manage_snapshots_thread_safety_fix() -> None:
    """Test that ManageSnapshots instances have isolated state."""
    # Create two mock transactions (representing different tables)
    transaction1 = Mock()
    transaction2 = Mock()

    # Create two ManageSnapshots instances
    manage1 = ManageSnapshots(transaction1)
    manage2 = ManageSnapshots(transaction2)

    # Note: Empty tuples () are singletons in Python, so we need to test with actual content
    # Mock the transaction method that would normally be called
    transaction1._set_ref_snapshot = Mock(return_value=(("update1",), ("req1",)))
    transaction2._set_ref_snapshot = Mock(return_value=(("update2",), ("req2",)))

    # Make different changes to each instance
    manage1.create_tag(snapshot_id=1001, tag_name="tag1")
    manage2.create_tag(snapshot_id=2001, tag_name="tag2")

    # NOW verify they have separate update tuples (this proves the fix works!)
    # Before fix: both would have the same content due to shared class attributes
    # After fix: they should have different content because they have separate instance attributes
    assert id(manage1._updates) != id(manage2._updates), (
        "ManageSnapshots instances are sharing the same _updates tuple - thread safety bug still exists"
    )

    assert id(manage1._requirements) != id(manage2._requirements), (
        "ManageSnapshots instances are sharing the same _requirements tuple - thread safety bug still exists"
    )

    # Verify no cross-contamination of updates
    assert manage1._updates != manage2._updates, "Updates are contaminated between instances"
    assert manage1._requirements != manage2._requirements, "Requirements are contaminated between instances"

    # Verify each instance has its expected content
    assert manage1._updates == ("update1",), f"manage1 should have ('update1',), got {manage1._updates}"
    assert manage2._updates == ("update2",), f"manage2 should have ('update2',), got {manage2._updates}"


def test_manage_snapshots_concurrent_operations() -> None:
    """Test concurrent operations with separate ManageSnapshots instances."""
    results: Dict[str, tuple] = {"manage1_updates": (), "manage2_updates": ()}

    def worker1() -> None:
        transaction1 = Mock()
        transaction1._set_ref_snapshot = Mock(return_value=(("update1",), ("req1",)))
        manage1 = ManageSnapshots(transaction1)
        manage1.create_tag(snapshot_id=1001, tag_name="tag1")
        results["manage1_updates"] = manage1._updates

    def worker2() -> None:
        transaction2 = Mock()
        transaction2._set_ref_snapshot = Mock(return_value=(("update2",), ("req2",)))
        manage2 = ManageSnapshots(transaction2)
        manage2.create_tag(snapshot_id=2001, tag_name="tag2")
        results["manage2_updates"] = manage2._updates

    # Run both workers concurrently
    thread1 = threading.Thread(target=worker1)
    thread2 = threading.Thread(target=worker2)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    # Check for cross-contamination
    expected_1 = ("update1",)
    expected_2 = ("update2",)

    assert results["manage1_updates"] == expected_1, "Worker 1 updates contaminated"
    assert results["manage2_updates"] == expected_2, "Worker 2 updates contaminated"


def test_manage_snapshots_concurrent_different_tables() -> None:
    """Test that concurrent ManageSnapshots operations on DIFFERENT tables work correctly.

    This test validates the thread safety fix by ensuring that concurrent
    operations on different table objects properly isolate their updates and requirements.
    """
    # Create two mock tables with different configurations
    table1 = Mock()
    table1.metadata = Mock()
    table1.metadata.table_uuid = uuid4()

    table2 = Mock()
    table2.metadata = Mock()
    table2.metadata.table_uuid = uuid4()

    # Track calls to each table's manage operations
    table1_operations = []
    table2_operations = []

    def create_table1_manage_mock():
        transaction_mock = Mock()
        
        def set_ref_snapshot_side_effect(**kwargs):
            table1_operations.append(kwargs)
            return (("table1_update",), ("table1_req",))
        
        transaction_mock._set_ref_snapshot = Mock(side_effect=set_ref_snapshot_side_effect)
        manage_mock = ManageSnapshots(transaction_mock)
        manage_mock.commit = Mock(return_value=None)
        return manage_mock

    def create_table2_manage_mock():
        transaction_mock = Mock()
        
        def set_ref_snapshot_side_effect(**kwargs):
            table2_operations.append(kwargs)
            return (("table2_update",), ("table2_req",))
        
        transaction_mock._set_ref_snapshot = Mock(side_effect=set_ref_snapshot_side_effect)
        manage_mock = ManageSnapshots(transaction_mock)
        manage_mock.commit = Mock(return_value=None)
        return manage_mock

    table1.manage_snapshots = Mock(side_effect=create_table1_manage_mock)
    table2.manage_snapshots = Mock(side_effect=create_table2_manage_mock)

    # Define different snapshot IDs and operations for each table
    table1_snapshot_id = 1001
    table2_snapshot_id = 2001

    def manage_table_snapshots(
        table_obj: Any, table_name: str, snapshot_id: int, tag_name: str, results: Dict[str, Any]
    ) -> None:
        """Manage snapshots for a specific table."""
        try:
            # Create tag operation (as in real usage)
            table_obj.manage_snapshots().create_tag(snapshot_id=snapshot_id, tag_name=tag_name).commit()

            results["success"] = True
            results["snapshot_id"] = snapshot_id
            results["tag_name"] = tag_name

        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    results1: Dict[str, Any] = {}
    results2: Dict[str, Any] = {}

    # Create threads to manage snapshots for different tables concurrently
    thread1 = threading.Thread(
        target=manage_table_snapshots, args=(table1, "table1", table1_snapshot_id, "tag1", results1)
    )
    thread2 = threading.Thread(
        target=manage_table_snapshots, args=(table2, "table2", table2_snapshot_id, "tag2", results2)
    )

    # Start threads concurrently
    thread1.start()
    thread2.start()

    # Wait for completion
    thread1.join()
    thread2.join()

    # Check results - both should succeed if thread safety is correct
    assert results1.get("success", False), f"Table1 management failed: {results1.get('error', 'Unknown error')}"
    assert results2.get("success", False), f"Table2 management failed: {results2.get('error', 'Unknown error')}"

    # CRITICAL: Verify that each table only received its own operations
    # This is the key test - if the bug exists, operations will cross-contaminate
    assert len(table1_operations) == 1, f"Table1 should have 1 operation, got {len(table1_operations)}"
    assert len(table2_operations) == 1, f"Table2 should have 1 operation, got {len(table2_operations)}"

    # Verify the operations contain the correct snapshot IDs
    assert table1_operations[0]["snapshot_id"] == table1_snapshot_id, "Table1 received wrong snapshot ID"
    assert table2_operations[0]["snapshot_id"] == table2_snapshot_id, "Table2 received wrong snapshot ID"

    # Verify tag names are correct
    assert table1_operations[0]["ref_name"] == "tag1", "Table1 received wrong tag name"
    assert table2_operations[0]["ref_name"] == "tag2", "Table2 received wrong tag name"


def test_manage_snapshots_cross_table_isolation() -> None:
    """Test that verifies operations don't get mixed up between different tables.

    This test validates the fix by ensuring that concurrent
    operations on different table objects properly isolate their updates and requirements.
    """

    # Create two mock table objects to simulate real usage
    table1 = Mock()
    table1.metadata = Mock()
    table1.metadata.table_uuid = uuid.uuid4()

    table2 = Mock()
    table2.metadata = Mock()
    table2.metadata.table_uuid = uuid.uuid4()

    # Track which operations each table's manage operation receives
    table1_manage_calls = []
    table2_manage_calls = []

    def mock_table1_manage():
        transaction_mock = Mock()
        
        def set_ref_side_effect(**kwargs):
            table1_manage_calls.append(kwargs)
            return (("update1",), ("req1",))
        
        transaction_mock._set_ref_snapshot = Mock(side_effect=set_ref_side_effect)
        manage_mock = ManageSnapshots(transaction_mock)
        manage_mock.commit = Mock(return_value=None)
        return manage_mock

    def mock_table2_manage():
        transaction_mock = Mock()
        
        def set_ref_side_effect(**kwargs):
            table2_manage_calls.append(kwargs)
            return (("update2",), ("req2",))
        
        transaction_mock._set_ref_snapshot = Mock(side_effect=set_ref_side_effect)
        manage_mock = ManageSnapshots(transaction_mock)
        manage_mock.commit = Mock(return_value=None)
        return manage_mock

    table1.manage_snapshots = Mock(side_effect=mock_table1_manage)
    table2.manage_snapshots = Mock(side_effect=mock_table2_manage)

    def manage_from_table(table: Any, table_name: str, operations: List[Dict], results: Dict[str, Any]) -> None:
        """Perform multiple manage operations on a specific table."""
        try:
            for op in operations:
                manager = table.manage_snapshots()
                if op["type"] == "tag":
                    manager.create_tag(snapshot_id=op["snapshot_id"], tag_name=op["name"]).commit()
                elif op["type"] == "branch":
                    manager.create_branch(snapshot_id=op["snapshot_id"], branch_name=op["name"]).commit()
            
            results["success"] = True
            results["operations"] = operations
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    # Prepare different operations for each table
    table1_operations = [
        {"type": "tag", "snapshot_id": 1001, "name": "tag1"},
        {"type": "branch", "snapshot_id": 1002, "name": "branch1"},
    ]
    table2_operations = [
        {"type": "tag", "snapshot_id": 2001, "name": "tag2"},
        {"type": "branch", "snapshot_id": 2002, "name": "branch2"},
    ]

    results1: Dict[str, Any] = {}
    results2: Dict[str, Any] = {}

    # Run concurrent management operations
    thread1 = threading.Thread(target=manage_from_table, args=(table1, "table1", table1_operations, results1))
    thread2 = threading.Thread(target=manage_from_table, args=(table2, "table2", table2_operations, results2))

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    # CRITICAL ASSERTION: Each table should only receive its own operations
    # If this fails, it means the thread safety bug exists

    # Table1 should only see table1 operations
    table1_snapshot_ids = [call["snapshot_id"] for call in table1_manage_calls]
    expected_table1_ids = [1001, 1002]
    
    assert table1_snapshot_ids == expected_table1_ids, (
        f"Table1 received unexpected snapshot IDs: {table1_snapshot_ids} (expected {expected_table1_ids})"
    )

    # Table2 should only see table2 operations
    table2_snapshot_ids = [call["snapshot_id"] for call in table2_manage_calls]
    expected_table2_ids = [2001, 2002]
    
    assert table2_snapshot_ids == expected_table2_ids, (
        f"Table2 received unexpected snapshot IDs: {table2_snapshot_ids} (expected {expected_table2_ids})"
    )

    # Verify no cross-contamination
    table1_received_table2_ids = [sid for sid in table1_snapshot_ids if sid in expected_table2_ids]
    table2_received_table1_ids = [sid for sid in table2_snapshot_ids if sid in expected_table1_ids]

    assert len(table1_received_table2_ids) == 0, f"Table1 incorrectly received Table2 snapshot IDs: {table1_received_table2_ids}"

    assert len(table2_received_table1_ids) == 0, f"Table2 incorrectly received Table1 snapshot IDs: {table2_received_table1_ids}"


def test_manage_snapshots_concurrent_same_table_different_operations() -> None:
    """Test that concurrent ManageSnapshots operations work correctly."""
    
    # Mock current snapshot ID
    current_snapshot_id = 12345

    # Create mock transactions that return the expected format
    def create_mock_transaction():
        transaction_mock = Mock()
        transaction_mock._set_ref_snapshot = Mock(return_value=(("update",), ("req",)))
        return transaction_mock

    def manage_snapshots_thread_func(operations: List[Dict], results: Dict[str, Any]) -> None:
        """Function to run in a thread that performs manage snapshot operations and captures results."""
        try:
            for op in operations:
                transaction = create_mock_transaction()
                manager = ManageSnapshots(transaction)
                if op["type"] == "tag":
                    manager.create_tag(snapshot_id=op["snapshot_id"], tag_name=op["name"])
                elif op["type"] == "branch":
                    manager.create_branch(snapshot_id=op["snapshot_id"], branch_name=op["name"])
                # Verify each manager has its own state
                results["updates"] = manager._updates
                results["requirements"] = manager._requirements
            results["success"] = True
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    # Define different operations for each thread
    operations1 = [{"type": "tag", "snapshot_id": current_snapshot_id, "name": "test_tag_1"}]
    operations2 = [{"type": "branch", "snapshot_id": current_snapshot_id, "name": "test_branch_2"}]

    # Prepare result dictionaries to capture thread outcomes
    results1: Dict[str, Any] = {}
    results2: Dict[str, Any] = {}

    # Create threads to perform different operations concurrently
    thread1 = threading.Thread(target=manage_snapshots_thread_func, args=(operations1, results1))
    thread2 = threading.Thread(target=manage_snapshots_thread_func, args=(operations2, results2))

    # Start and join threads
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    # Assert that both operations succeeded
    assert results1.get("success", False), f"Thread 1 management failed: {results1.get('error', 'Unknown error')}"
    assert results2.get("success", False), f"Thread 2 management failed: {results2.get('error', 'Unknown error')}"
    
    # Verify that each thread has its own isolated state
    assert results1["updates"] == ("update",), f"Thread 1 should have ('update',), got {results1['updates']}"
    assert results2["updates"] == ("update",), f"Thread 2 should have ('update',), got {results2['updates']}"
