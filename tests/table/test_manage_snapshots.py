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
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from pyiceberg.io import load_file_io
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.update import SetSnapshotRefUpdate, TableUpdate


def _mock_commit_response(table: Table) -> CommitTableResponse:
    return CommitTableResponse(
        metadata=table.metadata,
        metadata_location="s3://bucket/tbl",
        uuid=uuid4(),
    )


def _get_updates(mock_catalog: MagicMock) -> tuple[TableUpdate, ...]:
    args, _ = mock_catalog.commit_table.call_args
    return args[2]


def test_set_current_snapshot_basic(table_v2: Table) -> None:
    snapshot_one = 3051729675574597004

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().set_current_snapshot(snapshot_id=snapshot_one).commit()

    table_v2.catalog.commit_table.assert_called_once()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.snapshot_id == snapshot_one
    assert update.ref_name == "main"
    assert update.type == "branch"


def test_set_current_snapshot_unknown_id(table_v2: Table) -> None:
    invalid_snapshot_id = 1234567890000
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot set current snapshot to unknown snapshot id"):
        table_v2.manage_snapshots().set_current_snapshot(snapshot_id=invalid_snapshot_id).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_set_current_snapshot_to_current(table_v2: Table) -> None:
    current_snapshot = table_v2.current_snapshot()
    assert current_snapshot is not None

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().set_current_snapshot(snapshot_id=current_snapshot.snapshot_id).commit()

    table_v2.catalog.commit_table.assert_called_once()


def test_set_current_snapshot_chained_with_tag(table_v2: Table) -> None:
    snapshot_one = 3051729675574597004
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    (table_v2.manage_snapshots().set_current_snapshot(snapshot_id=snapshot_one).create_tag(snapshot_one, "my-tag").commit())

    table_v2.catalog.commit_table.assert_called_once()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 2
    assert {u.ref_name for u in set_ref_updates} == {"main", "my-tag"}


def test_set_current_snapshot_with_extensive_snapshots(table_v2_with_extensive_snapshots: Table) -> None:
    snapshots = table_v2_with_extensive_snapshots.metadata.snapshots
    assert len(snapshots) > 100

    target_snapshot = snapshots[50].snapshot_id

    table_v2_with_extensive_snapshots.catalog = MagicMock()
    table_v2_with_extensive_snapshots.catalog.commit_table.return_value = _mock_commit_response(table_v2_with_extensive_snapshots)

    table_v2_with_extensive_snapshots.manage_snapshots().set_current_snapshot(snapshot_id=target_snapshot).commit()

    table_v2_with_extensive_snapshots.catalog.commit_table.assert_called_once()

    updates = _get_updates(table_v2_with_extensive_snapshots.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].snapshot_id == target_snapshot


def test_set_current_snapshot_by_ref_name(table_v2: Table) -> None:
    current_snapshot = table_v2.current_snapshot()
    assert current_snapshot is not None

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().set_current_snapshot(ref_name="main").commit()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].snapshot_id == current_snapshot.snapshot_id
    assert set_ref_updates[0].ref_name == "main"


def test_set_current_snapshot_unknown_ref(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot find matching snapshot ID for ref: nonexistent"):
        table_v2.manage_snapshots().set_current_snapshot(ref_name="nonexistent").commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_set_current_snapshot_requires_one_argument(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Either snapshot_id or ref_name must be provided, not both"):
        table_v2.manage_snapshots().set_current_snapshot().commit()

    with pytest.raises(ValueError, match="Either snapshot_id or ref_name must be provided, not both"):
        table_v2.manage_snapshots().set_current_snapshot(snapshot_id=123, ref_name="main").commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_set_current_snapshot_chained_with_create_tag(table_v2: Table) -> None:
    snapshot_one = 3051729675574597004
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    # create a tag and immediately use it to set current snapshot
    (
        table_v2.manage_snapshots()
        .create_tag(snapshot_id=snapshot_one, tag_name="new-tag")
        .set_current_snapshot(ref_name="new-tag")
        .commit()
    )

    table_v2.catalog.commit_table.assert_called_once()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    # should have the tag and the main branch update
    assert len(set_ref_updates) == 2
    assert {u.ref_name for u in set_ref_updates} == {"new-tag", "main"}

    # The main branch should point to the same snapshot as the tag
    main_update = next(u for u in set_ref_updates if u.ref_name == "main")
    assert main_update.snapshot_id == snapshot_one


def test_rollback_to_snapshot(table_v2: Table) -> None:
    ancestor_snapshot_id = 3051729675574597004

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().rollback_to_snapshot(snapshot_id=ancestor_snapshot_id).commit()

    table_v2.catalog.commit_table.assert_called_once()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.snapshot_id == ancestor_snapshot_id
    assert update.ref_name == "main"
    assert update.type == "branch"


def test_rollback_to_snapshot_unknown_id(table_v2: Table) -> None:
    invalid_snapshot_id = 1234567890000
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot roll back to unknown snapshot id"):
        table_v2.manage_snapshots().rollback_to_snapshot(snapshot_id=invalid_snapshot_id).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_rollback_to_snapshot_not_ancestor(table_v2: Table) -> None:
    from pyiceberg.table.metadata import TableMetadataV2

    # create a table with a branching snapshot history:
    snapshot_a = 1
    snapshot_b = 2  # current
    snapshot_c = 3  # branch from a, not ancestor of b

    metadata_dict = {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 3,
        "last-updated-ms": 1602638573590,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "current-snapshot-id": snapshot_b,
        "snapshots": [
            {
                "snapshot-id": snapshot_a,
                "timestamp-ms": 1000,
                "sequence-number": 1,
                "manifest-list": "s3://a/1.avro",
            },
            {
                "snapshot-id": snapshot_b,
                "parent-snapshot-id": snapshot_a,
                "timestamp-ms": 2000,
                "sequence-number": 2,
                "manifest-list": "s3://a/2.avro",
            },
            {
                "snapshot-id": snapshot_c,
                "parent-snapshot-id": snapshot_a,
                "timestamp-ms": 3000,
                "sequence-number": 3,
                "manifest-list": "s3://a/3.avro",
            },
        ],
    }

    from pyiceberg.table import Table

    branching_table = Table(
        identifier=("db", "table"),
        metadata=TableMetadataV2(**metadata_dict),
        metadata_location="s3://bucket/test/metadata.json",
        io=load_file_io(),
        catalog=MagicMock(),
    )

    # snapshot_c exists but is not an ancestor of snapshot_b (current)
    with pytest.raises(ValueError, match="Cannot roll back to snapshot, not an ancestor of the current state"):
        branching_table.manage_snapshots().rollback_to_snapshot(snapshot_id=snapshot_c).commit()


def test_rollback_to_snapshot_chained_with_tag(table_v2: Table) -> None:
    ancestor_snapshot_id = 3051729675574597004

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    (
        table_v2.manage_snapshots()
        .create_tag(snapshot_id=ancestor_snapshot_id, tag_name="before-rollback")
        .rollback_to_snapshot(snapshot_id=ancestor_snapshot_id)
        .commit()
    )

    table_v2.catalog.commit_table.assert_called_once()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 2
    ref_names = {u.ref_name for u in set_ref_updates}
    assert ref_names == {"before-rollback", "main"}


def test_rollback_to_current_snapshot(table_v2: Table) -> None:
    current_snapshot = table_v2.current_snapshot()
    assert current_snapshot is not None

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().rollback_to_snapshot(snapshot_id=current_snapshot.snapshot_id).commit()
    table_v2.catalog.commit_table.assert_called_once()


def test_rollback_to_timestamp() -> None:
    from pyiceberg.table.metadata import TableMetadataV2

    metadata_dict = {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 4,
        "last-updated-ms": 1602638573590,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": 4,
        "snapshots": [
            {"snapshot-id": 1, "timestamp-ms": 1000, "sequence-number": 1, "manifest-list": "s3://a/1.avro"},
            {
                "snapshot-id": 2,
                "parent-snapshot-id": 1,
                "timestamp-ms": 2000,
                "sequence-number": 2,
                "manifest-list": "s3://a/2.avro",
            },
            {
                "snapshot-id": 3,
                "parent-snapshot-id": 2,
                "timestamp-ms": 3000,
                "sequence-number": 3,
                "manifest-list": "s3://a/3.avro",
            },
            {
                "snapshot-id": 4,
                "parent-snapshot-id": 3,
                "timestamp-ms": 4000,
                "sequence-number": 4,
                "manifest-list": "s3://a/4.avro",
            },
        ],
    }

    mock_catalog = MagicMock()
    table = Table(
        identifier=("db", "table"),
        metadata=TableMetadataV2(**metadata_dict),
        metadata_location="s3://bucket/test/metadata.json",
        io=load_file_io(),
        catalog=mock_catalog,
    )
    mock_catalog.commit_table.return_value = _mock_commit_response(table)

    # verify we find the ancestor before timestamp 2500
    table.manage_snapshots().rollback_to_timestamp(timestamp_ms=2500).commit()

    updates = _get_updates(mock_catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].snapshot_id == 2
    assert set_ref_updates[0].ref_name == "main"


def test_rollback_to_timestamp_no_valid_snapshot(table_v2: Table) -> None:
    # The oldest snapshot is at timestamp 1515100955770
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot roll back, no valid snapshot older than"):
        table_v2.manage_snapshots().rollback_to_timestamp(timestamp_ms=1515100955770).commit()

    table_v2.catalog.commit_table.assert_not_called()
