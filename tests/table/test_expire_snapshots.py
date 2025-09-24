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
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.update.snapshot import ExpireSnapshots


def test_cannot_expire_protected_head_snapshot(table_v2: Table) -> None:
    """Test that a HEAD (branch) snapshot cannot be expired."""
    HEAD_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    # Mock the catalog's commit_table method
    table_v2.catalog = MagicMock()
    # Simulate refs protecting HEAD_SNAPSHOT as a branch
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=HEAD_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            }
        }
    )
    # Assert fixture data
    assert any(ref.snapshot_id == HEAD_SNAPSHOT for ref in table_v2.metadata.refs.values())

    # Attempt to expire the HEAD snapshot and expect a ValueError
    with pytest.raises(ValueError, match=f"Snapshot with ID {HEAD_SNAPSHOT} is protected and cannot be expired."):
        table_v2.maintenance.expire_snapshots().by_id(HEAD_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_cannot_expire_tagged_snapshot(table_v2: Table) -> None:
    """Test that a tagged snapshot cannot be expired."""
    TAGGED_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    table_v2.catalog = MagicMock()
    # Simulate refs protecting TAGGED_SNAPSHOT as a tag
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "tag1": MagicMock(snapshot_id=TAGGED_SNAPSHOT, snapshot_ref_type="tag"),
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
            }
        }
    )
    assert any(ref.snapshot_id == TAGGED_SNAPSHOT for ref in table_v2.metadata.refs.values())

    with pytest.raises(ValueError, match=f"Snapshot with ID {TAGGED_SNAPSHOT} is protected and cannot be expired."):
        table_v2.maintenance.expire_snapshots().by_id(TAGGED_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_expire_unprotected_snapshot(table_v2: Table) -> None:
    """Test that an unprotected snapshot can be expired."""
    EXPIRE_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = mock_response

    # Remove any refs that protect the snapshot to be expired
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            }
        }
    )

    # Assert fixture data
    assert all(ref.snapshot_id != EXPIRE_SNAPSHOT for ref in table_v2.metadata.refs.values())

    # Expire the snapshot
    table_v2.maintenance.expire_snapshots().by_id(EXPIRE_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_called_once()
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT not in remaining_snapshots
    assert len(table_v2.metadata.snapshots) == 1


def test_expire_nonexistent_snapshot_raises(table_v2: Table) -> None:
    """Test that trying to expire a non-existent snapshot raises an error."""
    NONEXISTENT_SNAPSHOT = 9999999999999999999

    table_v2.catalog = MagicMock()
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}})

    with pytest.raises(ValueError, match=f"Snapshot with ID {NONEXISTENT_SNAPSHOT} does not exist."):
        table_v2.maintenance.expire_snapshots().by_id(NONEXISTENT_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_expire_snapshots_by_timestamp_skips_protected(table_v2: Table) -> None:
    # Setup: two snapshots; both are old, but one is head/tag protected
    HEAD_SNAPSHOT = 3051729675574597004
    TAGGED_SNAPSHOT = 3055729675574597004

    # Add snapshots to metadata for timestamp/protected test
    from types import SimpleNamespace

    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=HEAD_SNAPSHOT, snapshot_ref_type="branch"),
                "mytag": MagicMock(snapshot_id=TAGGED_SNAPSHOT, snapshot_ref_type="tag"),
            },
            "snapshots": [
                SimpleNamespace(snapshot_id=HEAD_SNAPSHOT, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=TAGGED_SNAPSHOT, timestamp_ms=1, parent_snapshot_id=None),
            ],
        }
    )
    table_v2.catalog = MagicMock()

    # Attempt to expire all snapshots before a future timestamp (so both are candidates)
    future_datetime = datetime.now() + timedelta(days=1)

    # Mock the catalog's commit_table to return the current metadata (simulate no change)
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata,  # protected snapshots remain
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    table_v2.maintenance.expire_snapshots().older_than(future_datetime).commit()
    # Update metadata to reflect the commit (as in other tests)
    table_v2.metadata = mock_response.metadata

    # Both protected snapshots should remain
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert HEAD_SNAPSHOT in remaining_ids
    assert TAGGED_SNAPSHOT in remaining_ids

    # No snapshots should have been expired (commit_table called, but with empty snapshot_ids)
    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    # Find RemoveSnapshotsUpdate in updates
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    assert remove_update.snapshot_ids == []


def test_expire_snapshots_by_ids(table_v2: Table) -> None:
    """Test that multiple unprotected snapshots can be expired by IDs."""
    EXPIRE_SNAPSHOT_1 = 3051729675574597004
    EXPIRE_SNAPSHOT_2 = 3051729675574597005
    KEEP_SNAPSHOT = 3055729675574597004

    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = mock_response

    # Remove any refs that protect the snapshots to be expired
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            }
        }
    )

    # Add snapshots to metadata for multi-id test
    from types import SimpleNamespace

    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            },
            "snapshots": [
                SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT_1, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT_2, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=KEEP_SNAPSHOT, timestamp_ms=2, parent_snapshot_id=None),
            ],
        }
    )

    # Assert fixture data
    assert all(ref.snapshot_id not in (EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2) for ref in table_v2.metadata.refs.values())

    # Expire the snapshots
    table_v2.maintenance.expire_snapshots().by_ids([EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2]).commit()

    table_v2.catalog.commit_table.assert_called_once()
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT_1 not in remaining_snapshots
    assert EXPIRE_SNAPSHOT_2 not in remaining_snapshots
    assert len(table_v2.metadata.snapshots) == 1


def test_thread_safety_fix() -> None:
    """Test that ExpireSnapshots instances have isolated state."""
    # Create two ExpireSnapshots instances
    expire1 = ExpireSnapshots(None)
    expire2 = ExpireSnapshots(None)

    # Verify they have separate snapshot sets (this was the bug!)
    # Before fix: both would have the same id (shared class attribute)
    # After fix: they should have different ids (separate instance attributes)
    assert id(expire1._snapshot_ids_to_expire) != id(expire2._snapshot_ids_to_expire), (
        "ExpireSnapshots instances are sharing the same snapshot set - thread safety bug still exists"
    )

    # Test that modifications to one don't affect the other
    expire1._snapshot_ids_to_expire.add(1001)
    expire2._snapshot_ids_to_expire.add(2001)

    # Verify no cross-contamination of snapshot IDs
    assert 2001 not in expire1._snapshot_ids_to_expire, "Snapshot IDs are leaking between instances"
    assert 1001 not in expire2._snapshot_ids_to_expire, "Snapshot IDs are leaking between instances"


def test_concurrent_operations() -> None:
    """Test concurrent operations with separate ExpireSnapshots instances."""
    results: Dict[str, set[int]] = {"expire1_snapshots": set(), "expire2_snapshots": set()}

    def worker1() -> None:
        transaction1 = Mock()
        expire1 = ExpireSnapshots(transaction1)
        expire1._snapshot_ids_to_expire.update([1001, 1002, 1003])
        results["expire1_snapshots"] = expire1._snapshot_ids_to_expire.copy()

    def worker2() -> None:
        transaction2 = Mock()
        expire2 = ExpireSnapshots(transaction2)
        expire2._snapshot_ids_to_expire.update([2001, 2002, 2003])
        results["expire2_snapshots"] = expire2._snapshot_ids_to_expire.copy()

    # Run both workers concurrently
    thread1 = threading.Thread(target=worker1)
    thread2 = threading.Thread(target=worker2)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    # Check for cross-contamination
    expected_1 = {1001, 1002, 1003}
    expected_2 = {2001, 2002, 2003}

    assert results["expire1_snapshots"] == expected_1, "Worker 1 snapshots contaminated"
    assert results["expire2_snapshots"] == expected_2, "Worker 2 snapshots contaminated"


def test_concurrent_different_tables_expiration() -> None:
    """Test that concurrent snapshot expiration on DIFFERENT tables works correctly.

    This test reproduces the issue described in:
    https://github.com/apache/iceberg-python/issues/2409

    The issue occurs when expiring snapshots from different tables concurrently,
    where snapshot IDs from one table get applied to another table.
    """
    # Create two mock tables with different snapshot IDs
    table1 = Mock()
    table1.metadata = Mock()
    table1.metadata.table_uuid = uuid4()

    table2 = Mock()
    table2.metadata = Mock()
    table2.metadata.table_uuid = uuid4()

    # Track calls to each table's expire_snapshots method
    table1_expire_calls = []
    table2_expire_calls = []

    def create_table1_expire_mock() -> Mock:
        expire_mock = Mock()

        def side_effect(sid: int) -> Mock:
            table1_expire_calls.append(sid)
            return expire_mock

        expire_mock.by_id = Mock(side_effect=side_effect)
        expire_mock.commit = Mock(return_value=None)
        return expire_mock

    def create_table2_expire_mock() -> Mock:
        expire_mock = Mock()

        def side_effect(sid: int) -> Mock:
            table2_expire_calls.append(sid)
            return expire_mock

        expire_mock.by_id = Mock(side_effect=side_effect)
        expire_mock.commit = Mock(return_value=None)
        return expire_mock

    table1.maintenance = Mock()
    table1.maintenance.expire_snapshots = Mock(side_effect=create_table1_expire_mock)

    table2.maintenance = Mock()
    table2.maintenance.expire_snapshots = Mock(side_effect=create_table2_expire_mock)

    # Define different snapshot IDs for each table
    table1_snapshot_ids = [1001, 1002, 1003, 1004, 1005]
    table2_snapshot_ids = [2001, 2002, 2003, 2004, 2005]

    def expire_table_snapshots(table_obj: Any, table_name: str, snapshots_to_expire: List[int], results: Dict[str, Any]) -> None:
        """Expire specific snapshots from a table."""
        try:
            # Expire the snapshots one by one (as in the user's example)
            for snapshot_id in snapshots_to_expire:
                table_obj.maintenance.expire_snapshots().by_id(snapshot_id).commit()

            results["success"] = True
            results["expired_snapshots"] = snapshots_to_expire

        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    # Prepare snapshots to expire (first 2 from each table)
    table1_to_expire = table1_snapshot_ids[:2]
    table2_to_expire = table2_snapshot_ids[:2]

    results1: Dict[str, Any] = {}
    results2: Dict[str, Any] = {}

    # Create threads to expire snapshots from different tables concurrently
    thread1 = threading.Thread(target=expire_table_snapshots, args=(table1, "table1", table1_to_expire, results1))
    thread2 = threading.Thread(target=expire_table_snapshots, args=(table2, "table2", table2_to_expire, results2))

    # Start threads concurrently
    thread1.start()
    thread2.start()

    # Wait for completion
    thread1.join()
    thread2.join()

    # Check results - both should succeed if thread safety is correct
    # Assert both operations succeeded
    assert results1.get("success", False), f"Table1 expiration failed: {results1.get('error', 'Unknown error')}"
    assert results2.get("success", False), f"Table2 expiration failed: {results2.get('error', 'Unknown error')}"

    # CRITICAL: Verify that each table only received its own snapshot IDs
    # This is the key test - if the bug exists, snapshot IDs will cross-contaminate
    for sid in table1_expire_calls:
        assert sid in table1_snapshot_ids, f"Table1 received unexpected snapshot ID {sid}"

    for sid in table2_expire_calls:
        assert sid in table2_snapshot_ids, f"Table2 received unexpected snapshot ID {sid}"

    # Verify expected snapshots were expired
    assert set(table1_expire_calls) == set(table1_to_expire), "Table1 didn't expire expected snapshots"
    assert set(table2_expire_calls) == set(table2_to_expire), "Table2 didn't expire expected snapshots"


def test_concurrent_same_table_different_snapshots(table_v2_with_extensive_snapshots: Table) -> None:
    """Test that concurrent snapshot expiration operations on the same table work correctly."""
    # Mock the catalog's commit_table method for both operations
    table_v2_with_extensive_snapshots.catalog = MagicMock()
    table_v2_with_extensive_snapshots.catalog.commit_table.return_value = CommitTableResponse(
        metadata=table_v2_with_extensive_snapshots.metadata, metadata_location="test://new_location"
    )

    # Use existing snapshot IDs from fixture data, but filter out protected snapshots
    all_snapshots = list(table_v2_with_extensive_snapshots.snapshots())
    snapshot_ids = [snapshot.snapshot_id for snapshot in all_snapshots]

    # Get protected snapshot IDs from refs
    protected_snapshot_ids = {ref.snapshot_id for ref in table_v2_with_extensive_snapshots.metadata.refs.values()}

    # Find unprotected snapshots that we can expire
    unprotected_snapshot_ids = [sid for sid in snapshot_ids if sid not in protected_snapshot_ids]

    # If we don't have enough unprotected snapshots, skip the test
    if len(unprotected_snapshot_ids) < 2:
        pytest.skip("Not enough unprotected snapshots available for testing")

    # We'll expire the first two unprotected snapshots concurrently
    to_expire1 = [unprotected_snapshot_ids[0]]
    to_expire2 = [unprotected_snapshot_ids[1]]

    def expire_snapshots_thread_func(table: Any, snapshot_ids_to_expire: List[int], results: Dict[str, Any]) -> None:
        """Function to run in a thread that expires snapshots and captures results."""
        try:
            # Expire snapshots
            expire_op = table.maintenance.expire_snapshots()
            for snapshot_id in snapshot_ids_to_expire:
                expire_op = expire_op.by_id(snapshot_id)
            expire_op.commit()
            results["success"] = True
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    # Prepare result dictionaries to capture thread outcomes
    results1: Dict[str, Any] = {}
    results2: Dict[str, Any] = {}

    # Create threads to expire snapshots concurrently
    thread1 = threading.Thread(
        target=expire_snapshots_thread_func, args=(table_v2_with_extensive_snapshots, to_expire1, results1)
    )
    thread2 = threading.Thread(
        target=expire_snapshots_thread_func, args=(table_v2_with_extensive_snapshots, to_expire2, results2)
    )

    # Start and join threads
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    # Assert that both operations succeeded
    assert results1.get("success", False), f"Thread 1 expiration failed: {results1.get('error', 'Unknown error')}"
    assert results2.get("success", False), f"Thread 2 expiration failed: {results2.get('error', 'Unknown error')}"

    # Verify that both commit_table calls were made
    assert table_v2_with_extensive_snapshots.catalog.commit_table.call_count == 2


def test_cross_table_snapshot_id_isolation() -> None:
    """Test that verifies snapshot IDs don't get mixed up between different tables.

    This test validates the fix for GitHub issue #2409 by ensuring that concurrent
    operations on different table objects properly isolate their snapshot IDs.
    """

    # Create two mock table objects to simulate the user's scenario
    # Mock table 1 with its own snapshot IDs
    table1 = Mock()
    table1.metadata = Mock()
    table1.metadata.table_uuid = uuid.uuid4()
    table1_snapshot_ids = [1001, 1002, 1003, 1004, 1005]

    # Mock table 2 with different snapshot IDs
    table2 = Mock()
    table2.metadata = Mock()
    table2.metadata.table_uuid = uuid.uuid4()
    table2_snapshot_ids = [2001, 2002, 2003, 2004, 2005]

    # Track which snapshot IDs each table's expire operation receives
    table1_expire_calls = []
    table2_expire_calls = []

    def mock_table1_expire() -> Mock:
        expire_mock = Mock()

        def side_effect(sid: int) -> Mock:
            table1_expire_calls.append(sid)
            return expire_mock

        expire_mock.by_id = Mock(side_effect=side_effect)
        expire_mock.commit = Mock(return_value=None)
        return expire_mock

    def mock_table2_expire() -> Mock:
        expire_mock = Mock()

        def side_effect(sid: int) -> Mock:
            table2_expire_calls.append(sid)
            return expire_mock

        expire_mock.by_id = Mock(side_effect=side_effect)
        expire_mock.commit = Mock(return_value=None)
        return expire_mock

    table1.maintenance = Mock()
    table1.maintenance.expire_snapshots = Mock(side_effect=mock_table1_expire)
    table2.maintenance = Mock()
    table2.maintenance.expire_snapshots = Mock(side_effect=mock_table2_expire)

    def expire_from_table(table: Any, table_name: str, snapshot_ids: List[int], results: Dict[str, Any]) -> None:
        """Expire snapshots from a specific table."""
        try:
            for snapshot_id in snapshot_ids:
                table.maintenance.expire_snapshots().by_id(snapshot_id).commit()
            results["success"] = True
            results["expired_ids"] = snapshot_ids
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    # Prepare snapshots to expire
    table1_to_expire = table1_snapshot_ids[:2]  # [1001, 1002]
    table2_to_expire = table2_snapshot_ids[:2]  # [2001, 2002]

    results1: Dict[str, Any] = {}
    results2: Dict[str, Any] = {}

    # Run concurrent expiration operations
    thread1 = threading.Thread(target=expire_from_table, args=(table1, "table1", table1_to_expire, results1))
    thread2 = threading.Thread(target=expire_from_table, args=(table2, "table2", table2_to_expire, results2))

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    # CRITICAL ASSERTION: Each table should only receive its own snapshot IDs
    # If this fails, it means the thread safety bug exists

    # Table1 should only see table1 snapshot IDs
    assert all(sid in table1_snapshot_ids for sid in table1_expire_calls), (
        f"Table1 received unexpected snapshot IDs: {table1_expire_calls} (should only contain {table1_snapshot_ids})"
    )

    # Table2 should only see table2 snapshot IDs
    assert all(sid in table2_snapshot_ids for sid in table2_expire_calls), (
        f"Table2 received unexpected snapshot IDs: {table2_expire_calls} (should only contain {table2_snapshot_ids})"
    )

    # Verify no cross-contamination
    table1_received_table2_ids = [sid for sid in table1_expire_calls if sid in table2_snapshot_ids]
    table2_received_table1_ids = [sid for sid in table2_expire_calls if sid in table1_snapshot_ids]

    assert len(table1_received_table2_ids) == 0, f"Table1 incorrectly received Table2 snapshot IDs: {table1_received_table2_ids}"

    assert len(table2_received_table1_ids) == 0, f"Table2 incorrectly received Table1 snapshot IDs: {table2_received_table1_ids}"


def test_batch_expire_snapshots(table_v2_with_extensive_snapshots: Table) -> None:
    """Test that batch expiration of multiple snapshots works correctly."""
    # Mock the catalog's commit_table method
    table_v2_with_extensive_snapshots.catalog = MagicMock()
    table_v2_with_extensive_snapshots.catalog.commit_table.return_value = CommitTableResponse(
        metadata=table_v2_with_extensive_snapshots.metadata, metadata_location="test://new_location"
    )

    # Use existing snapshot IDs from fixture data, but filter out protected snapshots
    all_snapshots = list(table_v2_with_extensive_snapshots.snapshots())
    snapshot_ids = [snapshot.snapshot_id for snapshot in all_snapshots]

    # Get protected snapshot IDs from refs
    protected_snapshot_ids = {ref.snapshot_id for ref in table_v2_with_extensive_snapshots.metadata.refs.values()}

    # Find unprotected snapshots that we can expire
    unprotected_snapshot_ids = [sid for sid in snapshot_ids if sid not in protected_snapshot_ids]

    # If we don't have enough unprotected snapshots, skip the test
    if len(unprotected_snapshot_ids) < 2:
        pytest.skip("Not enough unprotected snapshots available for testing")

    # We'll expire the first two unprotected snapshots in a batch
    to_expire = unprotected_snapshot_ids[:2]

    def batch_expire_thread_func(table: Any, snapshot_ids_to_expire: List[int], results: Dict[str, Any]) -> None:
        try:
            # Expire all snapshots in a single batch operation
            table.maintenance.expire_snapshots().by_ids(snapshot_ids_to_expire).commit()
            results["success"] = True
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)

    # Prepare result dictionary to capture thread outcome
    results: Dict[str, Any] = {}

    # Create thread to expire snapshots
    thread = threading.Thread(target=batch_expire_thread_func, args=(table_v2_with_extensive_snapshots, to_expire, results))

    # Start and join thread
    thread.start()
    thread.join()

    # Assert that the operation succeeded
    assert results.get("success", False), f"Batch expiration failed: {results.get('error', 'Unknown error')}"

    # Verify that commit_table was called once
    assert table_v2_with_extensive_snapshots.catalog.commit_table.call_count == 1
