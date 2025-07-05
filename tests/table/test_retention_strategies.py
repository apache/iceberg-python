#!/usr/bin/env python3
"""
Test script to validate the retention strategies implementation in MaintenanceTable.

This script demonstrates the new retention features:
1. retain_last_n_snapshots() - Keep only the last N snapshots
2. expire_snapshots_older_than_with_retention() - Time-based expiration with retention constraints
3. expire_snapshots_with_retention_policy() - Comprehensive retention policy
"""

# Example usage (commented out since we don't have an actual table)
"""
from pyiceberg.table.maintenance import MaintenanceTable
from pyiceberg.table import Table

# Assume we have a table instance
table = Table(...)  # Initialize your table
maintenance = MaintenanceTable(table)

# Example 1: Keep only the last 5 snapshots regardless of age
# This is helpful when regular snapshot creation occurs and users always want 
# to keep the last few for rollback
maintenance.retain_last_n_snapshots(5)

# Example 2: Expire snapshots older than a timestamp but keep at least 3 total
# This acts as a guardrail to prevent aggressive expiration logic from removing too many snapshots
import time
one_week_ago = int((time.time() - 7 * 24 * 60 * 60) * 1000)  # 7 days ago in milliseconds
maintenance.expire_snapshots_older_than_with_retention(
    timestamp_ms=one_week_ago,
    min_snapshots_to_keep=3
)

# Example 3: Combined policy - expire old snapshots but keep last 10 and at least 5 total
# This provides comprehensive control combining both strategies
maintenance.expire_snapshots_with_retention_policy(
    timestamp_ms=one_week_ago,
    retain_last_n=10,
    min_snapshots_to_keep=5
)

# Example 4: Just keep the last 20 snapshots (no time constraint)
expired_ids = maintenance.expire_snapshots_with_retention_policy(retain_last_n=20)
print(f"Expired {len(expired_ids)} snapshots")
"""

import pytest
from unittest.mock import MagicMock
from uuid import uuid4
from types import SimpleNamespace
from pyiceberg.table import CommitTableResponse

def _make_snapshots(ids_and_timestamps):
    return [SimpleNamespace(snapshot_id=sid, timestamp_ms=ts, parent_snapshot_id=None) for sid, ts in ids_and_timestamps]

def test_retain_last_n_snapshots(table_v2):
    # Setup: 5 snapshots, keep last 3
    ids_and_ts = [
        (1, 1000),
        (2, 2000),
        (3, 3000),
        (4, 4000),
        (5, 5000),
    ]
    snapshots = _make_snapshots(ids_and_ts)
    table_v2.metadata = table_v2.metadata.model_copy(update={"snapshots": snapshots, "refs": {}})
    table_v2.catalog = MagicMock()
    # Simulate commit response with only last 3 snapshots
    keep_ids = [3, 4, 5]
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [s for s in snapshots if s.snapshot_id in keep_ids]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response
    table_v2.maintenance.retain_last_n_snapshots(3)
    table_v2.catalog.commit_table.assert_called_once()
    # Update metadata to reflect commit
    table_v2.metadata = mock_response.metadata
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == set(keep_ids)

def test_min_snapshots_to_keep(table_v2):
    # Setup: 5 snapshots, expire all older than 4500, but keep at least 3
    ids_and_ts = [
        (1, 1000),
        (2, 2000),
        (3, 3000),
        (4, 4000),
        (5, 5000),
    ]
    snapshots = _make_snapshots(ids_and_ts)
    table_v2.metadata = table_v2.metadata.model_copy(update={"snapshots": snapshots, "refs": {}})
    table_v2.catalog = MagicMock()
    # Only 1,2 should be expired (to keep 3 total)
    keep_ids = [3, 4, 5]
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [s for s in snapshots if s.snapshot_id in keep_ids]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response
    table_v2.maintenance.expire_snapshots_older_than_with_retention(timestamp_ms=4500, min_snapshots_to_keep=3)
    table_v2.catalog.commit_table.assert_called_once()
    table_v2.metadata = mock_response.metadata
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == set(keep_ids)

def test_combined_constraints(table_v2):
    # Setup: 5 snapshots, expire all older than 3500, keep last 2, min 4 total
    ids_and_ts = [
        (1, 1000),
        (2, 2000),
        (3, 3000),
        (4, 4000),
        (5, 5000),
    ]
    snapshots = _make_snapshots(ids_and_ts)
    table_v2.metadata = table_v2.metadata.model_copy(update={"snapshots": snapshots, "refs": {}})
    table_v2.catalog = MagicMock()
    # Only 1 should be expired (to keep last 2 and min 4 total)
    keep_ids = [2, 3, 4, 5]
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [s for s in snapshots if s.snapshot_id in keep_ids]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response
    table_v2.maintenance.expire_snapshots_with_retention_policy(
        timestamp_ms=3500, retain_last_n=2, min_snapshots_to_keep=4
    )
    table_v2.catalog.commit_table.assert_called_once()
    table_v2.metadata = mock_response.metadata
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == set(keep_ids)

def test_cannot_expire_protected_head_snapshot(table_v2) -> None:
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
        table_v2.maintenance.expire_snapshot_by_id(HEAD_SNAPSHOT)
        
    table_v2.catalog.commit_table.assert_not_called()

def test_cannot_expire_tagged_snapshot(table_v2) -> None:
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
        table_v2.maintenance.expire_snapshot_by_id(TAGGED_SNAPSHOT)

    table_v2.catalog.commit_table.assert_not_called()

def test_expire_unprotected_snapshot(table_v2) -> None:
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
    table_v2.maintenance.expire_snapshot_by_id(EXPIRE_SNAPSHOT)

    table_v2.catalog.commit_table.assert_called_once()
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT not in remaining_snapshots
    assert len(table_v2.metadata.snapshots) == 1

def test_expire_nonexistent_snapshot_raises(table_v2) -> None:
    """Test that trying to expire a non-existent snapshot raises an error."""
    NONEXISTENT_SNAPSHOT = 9999999999999999999

    table_v2.catalog = MagicMock()
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}})

    with pytest.raises(ValueError, match=f"Snapshot with ID {NONEXISTENT_SNAPSHOT} does not exist."):
        table_v2.maintenance.expire_snapshot_by_id(NONEXISTENT_SNAPSHOT)

    table_v2.catalog.commit_table.assert_not_called()

def test_expire_snapshots_by_timestamp_skips_protected(table_v2) -> None:
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
    future_timestamp = 9999999999999  # Far in the future, after any real snapshot

    # Mock the catalog's commit_table to return the current metadata (simulate no change)
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata,  # protected snapshots remain
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    table_v2.maintenance.expire_snapshots_older_than(future_timestamp)
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

def test_expire_snapshots_by_ids(table_v2) -> None:
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
    table_v2.maintenance.expire_snapshots_by_ids([EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2])

    table_v2.catalog.commit_table.assert_called_once()
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT_1 not in remaining_snapshots
    assert EXPIRE_SNAPSHOT_2 not in remaining_snapshots
    assert len(table_v2.metadata.snapshots) == 1
