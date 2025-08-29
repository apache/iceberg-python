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
import datetime
from unittest.mock import MagicMock
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
    future_datetime = datetime.datetime.now() + datetime.timedelta(days=1)

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


def test_retain_last_n_with_protection(table_v2: Table) -> None:
    """Test retain_last_n keeps most recent snapshots plus protected ones."""
    from types import SimpleNamespace

    # Clear shared state set on the class between tests
    ExpireSnapshots._snapshot_ids_to_expire.clear()

    PROTECTED_SNAPSHOT = 3051729675574597101  # oldest (also protected)
    EXPIRE_SNAPSHOT = 3051729675574597102
    KEEP_SNAPSHOT_1 = 3051729675574597103
    KEEP_SNAPSHOT_2 = 3051729675574597104  # newest

    # Protected PROTECTED_SNAPSHOT as branch head
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=PROTECTED_SNAPSHOT, snapshot_ref_type="branch"),
            },
            "snapshots": [
                SimpleNamespace(snapshot_id=PROTECTED_SNAPSHOT, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT, timestamp_ms=2, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=KEEP_SNAPSHOT_1, timestamp_ms=3, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=KEEP_SNAPSHOT_2, timestamp_ms=4, parent_snapshot_id=None),
            ],
        }
    )

    table_v2.catalog = MagicMock()
    kept_ids = {PROTECTED_SNAPSHOT, KEEP_SNAPSHOT_1, KEEP_SNAPSHOT_2}  # retain_last_n=2 keeps newest plus protected
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": list(kept_ids)}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    table_v2.maintenance.expire_snapshots().retain_last_n(2).commit()
    table_v2.metadata = mock_response.metadata

    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    # Only EXPIRE_SNAPSHOT should be expired
    assert set(remove_update.snapshot_ids) == {EXPIRE_SNAPSHOT}
    assert EXPIRE_SNAPSHOT not in table_v2.metadata.snapshots


def test_retain_last_n_validation(table_v2: Table) -> None:
    """Test retain_last_n validates n >= 1."""
    ExpireSnapshots._snapshot_ids_to_expire.clear()

    with pytest.raises(ValueError, match="Number of snapshots to retain must be at least 1"):
        table_v2.maintenance.expire_snapshots().retain_last_n(0)

    with pytest.raises(ValueError, match="Number of snapshots to retain must be at least 1"):
        table_v2.maintenance.expire_snapshots().retain_last_n(-1)


def test_with_retention_policy_validation(table_v2: Table) -> None:
    """Test with_retention_policy validates parameter ranges."""
    ExpireSnapshots._snapshot_ids_to_expire.clear()

    with pytest.raises(ValueError, match="retain_last_n must be at least 1"):
        table_v2.maintenance.expire_snapshots().with_retention_policy(retain_last_n=0).commit()

    with pytest.raises(ValueError, match="min_snapshots_to_keep must be at least 1"):
        table_v2.maintenance.expire_snapshots().with_retention_policy(min_snapshots_to_keep=0).commit()


def test_with_retention_policy_no_criteria_does_nothing(table_v2: Table) -> None:
    """Test with_retention_policy does nothing when no criteria provided."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    SNAPSHOT_1, SNAPSHOT_2 = 3051729675574597501, 3051729675574597502
    snapshots = [
        SimpleNamespace(snapshot_id=SNAPSHOT_1, timestamp_ms=100, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=SNAPSHOT_2, timestamp_ms=200, parent_snapshot_id=None),
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}, "snapshots": snapshots, "properties": {}})
    table_v2.catalog = MagicMock()

    # Should be a no-op
    result = table_v2.maintenance.expire_snapshots().with_retention_policy()
    assert result._snapshot_ids_to_expire == set()


def test_get_expiration_properties_missing_values(table_v2: Table) -> None:
    """Test _get_expiration_properties handles missing properties gracefully."""
    ExpireSnapshots._snapshot_ids_to_expire.clear()

    # No expiration properties set
    table_v2.metadata = table_v2.metadata.model_copy(update={"properties": {}})
    expire = table_v2.maintenance.expire_snapshots()
    max_age, min_snaps, max_ref_age = expire._get_expiration_properties()

    assert max_age is None
    assert min_snaps is None
    assert max_ref_age is None


def test_retain_last_n_fewer_snapshots_than_requested(table_v2: Table) -> None:
    """Test retain_last_n when table has fewer snapshots than requested."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    SNAPSHOT_1, SNAPSHOT_2 = 3051729675574597601, 3051729675574597602
    snapshots = [
        SimpleNamespace(snapshot_id=SNAPSHOT_1, timestamp_ms=100, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=SNAPSHOT_2, timestamp_ms=200, parent_snapshot_id=None),
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}, "snapshots": snapshots})
    table_v2.catalog = MagicMock()

    # Request to keep 5 snapshots, but only 2 exist
    result = table_v2.maintenance.expire_snapshots().retain_last_n(5)
    # Should not expire anything
    assert result._snapshot_ids_to_expire == set()


def test_older_than_with_retention_edge_cases(table_v2: Table) -> None:
    """Test edge cases for older_than_with_retention."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    # Test with retain_last_n and a valid timestamp
    EXPIRE_SNAPSHOT, KEEP_SNAPSHOT_1, KEEP_SNAPSHOT_2 = 3051729675574597701, 3051729675574597702, 3051729675574597703
    snapshots = [
        SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT, timestamp_ms=100, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=KEEP_SNAPSHOT_1, timestamp_ms=200, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=KEEP_SNAPSHOT_2, timestamp_ms=300, parent_snapshot_id=None),
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}, "snapshots": snapshots})
    table_v2.catalog = MagicMock()

    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT_1, KEEP_SNAPSHOT_2]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    # Use a timestamp that would include all snapshots, but retain_last_n=2 should limit to keeping KEEP_SNAPSHOT_1, KEEP_SNAPSHOT_2
    table_v2.maintenance.expire_snapshots().older_than_with_retention(
        timestamp_ms=400, retain_last_n=2, min_snapshots_to_keep=None
    ).commit()

    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    assert set(remove_update.snapshot_ids) == {EXPIRE_SNAPSHOT}


def test_with_retention_policy_partial_properties(table_v2: Table) -> None:
    """Test with_retention_policy with only some properties set."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    # Only max-snapshot-age-ms set, no min-snapshots-to-keep
    properties = {"history.expire.max-snapshot-age-ms": "250"}
    EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2, KEEP_SNAPSHOT = 3051729675574597801, 3051729675574597802, 3051729675574597803
    snapshots = [
        SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT_1, timestamp_ms=100, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT_2, timestamp_ms=200, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=KEEP_SNAPSHOT, timestamp_ms=300, parent_snapshot_id=None),
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}, "snapshots": snapshots, "properties": properties})
    table_v2.catalog = MagicMock()

    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    # Should expire EXPIRE_SNAPSHOT_1,EXPIRE_SNAPSHOT_2 (older than 250ms), keep KEEP_SNAPSHOT
    table_v2.maintenance.expire_snapshots().with_retention_policy().commit()

    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    assert set(remove_update.snapshot_ids) == {EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2}
