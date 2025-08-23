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

    S1 = 101  # oldest (also protected)
    S2 = 102
    S3 = 103
    S4 = 104  # newest

    # Protected S1 as branch head
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=S1, snapshot_ref_type="branch"),
            },
            "snapshots": [
                SimpleNamespace(snapshot_id=S1, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=S2, timestamp_ms=2, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=S3, timestamp_ms=3, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=S4, timestamp_ms=4, parent_snapshot_id=None),
            ],
        }
    )

    table_v2.catalog = MagicMock()
    kept_ids = {S1, S3, S4}  # retain_last_n=2 keeps S4,S3 plus protected S1
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
    # Only S2 should be expired
    assert set(remove_update.snapshot_ids) == {S2}
    assert S2 not in table_v2.metadata.snapshots


def test_older_than_with_retention_combination(table_v2: Table) -> None:
    """Test older_than_with_retention combining timestamp, retain_last_n and min_snapshots_to_keep."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    # Create 5 snapshots with increasing timestamps
    S1, S2, S3, S4, S5 = 201, 202, 203, 204, 205
    snapshots = [
        SimpleNamespace(snapshot_id=S1, timestamp_ms=100, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S2, timestamp_ms=200, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S3, timestamp_ms=300, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S4, timestamp_ms=400, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S5, timestamp_ms=500, parent_snapshot_id=None),
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}, "snapshots": snapshots})
    table_v2.catalog = MagicMock()

    # Expect to expire S1,S2,S3 ; keep S4 (due to min snapshots) and S5 (retain_last_n=1)
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [S4, S5]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    table_v2.maintenance.expire_snapshots().older_than_with_retention(
        timestamp_ms=450, retain_last_n=1, min_snapshots_to_keep=2
    ).commit()
    table_v2.metadata = mock_response.metadata

    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    assert set(remove_update.snapshot_ids) == {S1, S2, S3}
    assert set(table_v2.metadata.snapshots) == {S4, S5}


def test_with_retention_policy_defaults(table_v2: Table) -> None:
    """Test with_retention_policy uses table property defaults when arguments omitted."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    # Properties: expire snapshots older than 350ms, keep at least 3 snapshots
    properties = {
        "history.expire.max-snapshot-age-ms": "350",
        "history.expire.min-snapshots-to-keep": "3",
    }
    S1, S2, S3, S4, S5 = 301, 302, 303, 304, 305
    snapshots = [
        SimpleNamespace(snapshot_id=S1, timestamp_ms=100, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S2, timestamp_ms=200, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S3, timestamp_ms=300, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S4, timestamp_ms=400, parent_snapshot_id=None),
        SimpleNamespace(snapshot_id=S5, timestamp_ms=500, parent_snapshot_id=None),
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}, "snapshots": snapshots, "properties": properties})
    table_v2.catalog = MagicMock()

    # Expect S1,S2 expired; S3 kept due to min_snapshots_to_keep
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [S3, S4, S5]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    table_v2.maintenance.expire_snapshots().with_retention_policy().commit()
    table_v2.metadata = mock_response.metadata

    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    assert set(remove_update.snapshot_ids) == {S1, S2}
    assert set(table_v2.metadata.snapshots) == {S3, S4, S5}


def test_get_expiration_properties(table_v2: Table) -> None:
    """Test retrieval of expiration properties from table metadata."""
    ExpireSnapshots._snapshot_ids_to_expire.clear()
    properties = {
        "history.expire.max-snapshot-age-ms": "60000",
        "history.expire.min-snapshots-to-keep": "5",
        "history.expire.max-ref-age-ms": "120000",
    }
    table_v2.metadata = table_v2.metadata.model_copy(update={"properties": properties})
    expire = table_v2.maintenance.expire_snapshots()
    max_age, min_snaps, max_ref_age = expire._get_expiration_properties()
    assert max_age == 60000
    assert min_snaps == 5
    assert max_ref_age == 120000


def test_get_snapshots_to_expire_with_retention_respects_protection(table_v2: Table) -> None:
    """Internal helper should not select protected snapshots for expiration."""
    from types import SimpleNamespace

    ExpireSnapshots._snapshot_ids_to_expire.clear()

    P = 401  # protected
    A = 402
    B = 403
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {"main": MagicMock(snapshot_id=P, snapshot_ref_type="branch")},
            "snapshots": [
                SimpleNamespace(snapshot_id=P, timestamp_ms=10, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=A, timestamp_ms=20, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=B, timestamp_ms=30, parent_snapshot_id=None),
            ],
        }
    )
    expire = table_v2.maintenance.expire_snapshots()
    to_expire = expire._get_snapshots_to_expire_with_retention(timestamp_ms=100, retain_last_n=None, min_snapshots_to_keep=1)
    # Protected snapshot P should not be in list; both A and B can expire respecting min keep
    assert P not in to_expire
    assert set(to_expire) == {A, B}
