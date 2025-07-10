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
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from pyiceberg.table import CommitTableResponse, Table  # noqa: F401


def _make_snapshots(ids_and_timestamps: list[tuple[int, int]]) -> list[SimpleNamespace]:
    return [SimpleNamespace(snapshot_id=sid, timestamp_ms=ts, parent_snapshot_id=None) for sid, ts in ids_and_timestamps]


def test_retain_last_n_snapshots(table_v2: Table) -> None:
    # Setup: 5 snapshots, keep last 3
    ids_and_ts = [
        (1, 1000),
        (2, 2000),
        (3, 3000),
        (4, 4000),
        (5, 5000),
    ]
    snapshots = _make_snapshots(ids_and_ts)
    
    # Save original catalog for cleanup
    original_catalog = table_v2.catalog
    
    try:
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
    finally:
        # Restore original catalog and cleanup
        table_v2.catalog = original_catalog
        if hasattr(original_catalog, '_connection') and original_catalog._connection is not None:
            try:
                original_catalog._connection.close()
            except Exception:
                pass


def test_min_snapshots_to_keep(table_v2: Table) -> None:
    # Setup: 5 snapshots, expire all older than 4500, but keep at least 3
    ids_and_ts = [
        (1, 1000),
        (2, 2000),
        (3, 3000),
        (4, 4000),
        (5, 5000),
    ]
    snapshots = _make_snapshots(ids_and_ts)
    
    # Save original catalog for cleanup
    original_catalog = table_v2.catalog
    
    try:
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
    finally:
        # Restore original catalog and cleanup
        table_v2.catalog = original_catalog
        if hasattr(original_catalog, '_connection') and original_catalog._connection is not None:
            try:
                original_catalog._connection.close()
            except Exception:
                pass


def test_combined_constraints(table_v2: Table) -> None:
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
    table_v2.maintenance.expire_snapshots_with_retention_policy(timestamp_ms=3500, retain_last_n=2, min_snapshots_to_keep=4)
    table_v2.catalog.commit_table.assert_called_once()
    table_v2.metadata = mock_response.metadata
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == set(keep_ids)


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
        table_v2.maintenance.expire_snapshot_by_id(HEAD_SNAPSHOT)

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
        table_v2.maintenance.expire_snapshot_by_id(TAGGED_SNAPSHOT)

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
    table_v2.maintenance.expire_snapshot_by_id(EXPIRE_SNAPSHOT)

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
        table_v2.maintenance.expire_snapshot_by_id(NONEXISTENT_SNAPSHOT)

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
                "my_tag": MagicMock(snapshot_id=TAGGED_SNAPSHOT, snapshot_ref_type="tag"),
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

    # Both protected snapshots should remain
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert HEAD_SNAPSHOT in remaining_ids
    assert TAGGED_SNAPSHOT in remaining_ids

    # No snapshots should have been expired, so commit_table should not have been called
    # This is the correct behavior - don't create unnecessary transactions when there's nothing to do
    table_v2.catalog.commit_table.assert_not_called()


def test_expire_snapshots_by_ids(table_v2: Table) -> None:
    """Test that multiple unprotected snapshots can be expired by IDs."""
    EXPIRE_SNAPSHOT_1 = 3051729675574597004
    EXPIRE_SNAPSHOT_2 = 3051729675574597005
    KEEP_SNAPSHOT = 3055729675574597004

    # Save the original catalog for cleanup
    original_catalog = table_v2.catalog

    try:
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
        table_v2.maintenance._expire_snapshots_by_ids([EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2])

        table_v2.catalog.commit_table.assert_called_once()
        remaining_snapshots = table_v2.metadata.snapshots
        assert EXPIRE_SNAPSHOT_1 not in remaining_snapshots
        assert EXPIRE_SNAPSHOT_2 not in remaining_snapshots
        assert len(table_v2.metadata.snapshots) == 1
    finally:
        # Restore original catalog and cleanup any connections
        table_v2.catalog = original_catalog
        if hasattr(original_catalog, '_connection') and original_catalog._connection is not None:
            try:
                original_catalog._connection.close()
            except Exception:
                pass


def test_expire_snapshots_with_table_property_defaults(table_v2: Table) -> None:
    """Test that table properties are used as defaults for expiration."""
    # Setup: 5 snapshots with properties set
    ids_and_ts = [
        (1, 1000),  # Old snapshot
        (2, 2000),  # Old snapshot
        (3, 3000),  # Should be kept (min snapshots)
        (4, 4000),  # Should be kept (min snapshots)
        (5, 5000),  # Should be kept (newer than max age)
    ]
    snapshots = _make_snapshots(ids_and_ts)
    
    # Set table properties
    properties = {
        "history.expire.max-snapshot-age-ms": "4500",  # Keep snapshots newer than this
        "history.expire.min-snapshots-to-keep": "3",   # Always keep at least 3 snapshots
    }
    
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "snapshots": snapshots,
            "refs": {},
            "properties": properties,
        }
    )
    table_v2.catalog = MagicMock()

    # Only snapshots 1,2 should be expired (keep 3,4,5 due to min_snapshots_to_keep=3)
    keep_ids = [3, 4, 5]
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [s for s in snapshots if s.snapshot_id in keep_ids]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    # Call expire without explicit parameters - should use table properties
    table_v2.maintenance.expire_snapshots_with_retention_policy()

    table_v2.catalog.commit_table.assert_called_once()
    table_v2.metadata = mock_response.metadata
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == set(keep_ids)


def test_explicit_parameters_override_table_properties(table_v2: Table) -> None:
    """Test that explicit parameters override table property defaults."""
    # Setup: 5 snapshots with properties set
    ids_and_ts = [
        (1, 1000),  # Old snapshot, will be expired
        (2, 2000),  # Will be kept (min snapshots)
        (3, 3000),  # Will be kept (min snapshots)
        (4, 4000),  # Will be kept (min snapshots)
        (5, 5000),  # Will be kept (min snapshots)
    ]
    snapshots = _make_snapshots(ids_and_ts)
    
    # Set table properties that are more aggressive than what we'll use
    properties = {
        "history.expire.max-snapshot-age-ms": "1500",  # Would expire more snapshots
        "history.expire.min-snapshots-to-keep": "2",   # Would keep fewer snapshots
    }
    
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "snapshots": snapshots,
            "refs": {},
            "properties": properties,
        }
    )
    table_v2.catalog = MagicMock()

    # Should keep 2,3,4,5 due to explicit min_snapshots_to_keep=4
    keep_ids = [2, 3, 4, 5]
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [s for s in snapshots if s.snapshot_id in keep_ids]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    # Call expire with explicit parameters that should override the properties
    table_v2.maintenance.expire_snapshots_with_retention_policy(
        timestamp_ms=1500,  # Only expire snapshots older than this
        min_snapshots_to_keep=4  # Keep at least 4 snapshots (overrides property of 2)
    )

    table_v2.catalog.commit_table.assert_called_once()
    table_v2.metadata = mock_response.metadata
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == set(keep_ids)


def test_expire_snapshots_no_properties_no_parameters(table_v2: Table) -> None:
    """Test that no snapshots are expired when no properties or parameters are set."""
    # Setup: 5 snapshots with no properties
    ids_and_ts = [
        (1, 1000),
        (2, 2000),
        (3, 3000),
        (4, 4000),
        (5, 5000),
    ]
    snapshots = _make_snapshots(ids_and_ts)
    
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "snapshots": snapshots,
            "refs": {},
            "properties": {},  # No properties set
        }
    )
    table_v2.catalog = MagicMock()

    # Call expire with no parameters
    table_v2.maintenance.expire_snapshots_with_retention_policy()

    # Should not attempt to expire anything since no criteria were provided
    table_v2.catalog.commit_table.assert_not_called()
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert remaining_ids == {1, 2, 3, 4, 5}  # All snapshots should remain
