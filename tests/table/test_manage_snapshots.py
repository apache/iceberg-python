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

from pyiceberg.exceptions import (
    NoSuchSnapshotRefError,
    NotAncestorError,
    SnapshotRefTypeError,
)
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.refs import SnapshotRefType
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


def test_fast_forward_branch_advances_to_descendant(table_v2: Table) -> None:
    parent_snapshot_id = 3051729675574597004
    child_snapshot_id = 3055729675574597004

    # Create a lagging branch at the parent snapshot, then reset the mock so
    # the next commit's updates are the fast-forward alone.
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)
    table_v2.manage_snapshots().create_branch(snapshot_id=parent_snapshot_id, branch_name="lagging").commit()
    table_v2.catalog.commit_table.reset_mock()

    table_v2.manage_snapshots().fast_forward_branch(from_branch="lagging", to_ref="main").commit()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.ref_name == "lagging"
    assert update.snapshot_id == child_snapshot_id
    assert update.type == "branch"


def test_fast_forward_branch_creates_missing_from(table_v2: Table) -> None:
    current_snapshot = table_v2.current_snapshot()
    assert current_snapshot is not None
    main_snapshot_id = current_snapshot.snapshot_id

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="brand-new", to_ref="main").commit()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].ref_name == "brand-new"
    assert set_ref_updates[0].snapshot_id == main_snapshot_id
    assert set_ref_updates[0].type == "branch"


def test_fast_forward_branch_noop_when_already_equal(table_v2: Table) -> None:
    # The no-op check operates on committed metadata only (see
    # ``fast_forward_branch`` docstring — intra-chain Java-parity is not
    # implemented). Inject a second branch pointing at main's snapshot
    # directly into metadata, then confirm the fast-forward stages nothing.
    from pyiceberg.table.refs import SnapshotRef

    current_snapshot = table_v2.current_snapshot()
    assert current_snapshot is not None
    main_snapshot_id = current_snapshot.snapshot_id
    table_v2.metadata.refs["peer"] = SnapshotRef(
        snapshot_id=main_snapshot_id,
        snapshot_ref_type="branch",
    )

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="peer", to_ref="main").commit()

    # A pure no-op stages no updates; commit_transaction short-circuits.
    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_rejects_tag_as_source(table_v2: Table) -> None:
    # Precondition: the fixture provides a tag named "test".
    assert table_v2.metadata.refs["test"].snapshot_ref_type == SnapshotRefType.TAG
    table_v2.catalog = MagicMock()

    with pytest.raises(SnapshotRefTypeError, match="Ref test is a tag, not a branch"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="test", to_ref="main").commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_rejects_missing_to_ref(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()

    with pytest.raises(NoSuchSnapshotRefError, match="Ref does not exist: nonexistent"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="nonexistent").commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_rejects_non_ancestor(table_v2: Table) -> None:
    # Non-ancestor check operates on committed metadata only (see
    # ``fast_forward_branch`` docstring — intra-chain Java-parity is not
    # implemented). Inject "ahead" branch directly into metadata so the
    # ancestry check can observe it.
    from pyiceberg.table.refs import SnapshotRef

    newer_snapshot_id = 3055729675574597004  # main's current snapshot; "test" tag points at older snapshot

    table_v2.metadata.refs["ahead"] = SnapshotRef(
        snapshot_id=newer_snapshot_id,
        snapshot_ref_type="branch",
    )

    table_v2.catalog = MagicMock()

    # Try to fast-forward "ahead" (at newer) backwards to "test" (at older).
    with pytest.raises(NotAncestorError, match="Cannot fast-forward: ahead is not an ancestor of test"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="ahead", to_ref="test").commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_preserves_retention_fields(table_v2: Table) -> None:
    from pyiceberg.table.refs import SnapshotRef

    parent_snapshot_id = 3051729675574597004
    child_snapshot_id = 3055729675574597004

    # Inject a branch with all three retention fields set into the metadata.
    table_v2.metadata.refs["retained"] = SnapshotRef(
        snapshot_id=parent_snapshot_id,
        snapshot_ref_type="branch",
        max_ref_age_ms=1000,
        max_snapshot_age_ms=2000,
        min_snapshots_to_keep=3,
    )

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="retained", to_ref="main").commit()

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.ref_name == "retained"
    assert update.snapshot_id == child_snapshot_id
    assert update.max_ref_age_ms == 1000
    assert update.max_snapshot_age_ms == 2000
    assert update.min_snapshots_to_keep == 3


def test_fast_forward_branch_chains(table_v2: Table) -> None:
    parent_snapshot_id = 3051729675574597004

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    with table_v2.manage_snapshots() as ms:
        ms.create_branch(snapshot_id=parent_snapshot_id, branch_name="stream").fast_forward_branch(
            from_branch="stream", to_ref="main"
        ).create_tag(snapshot_id=parent_snapshot_id, tag_name="stream-v1")

    updates = _get_updates(table_v2.catalog)
    set_ref_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate)]

    ref_names = {u.ref_name for u in set_ref_updates}
    assert "stream" in ref_names  # from create_branch AND fast_forward_branch
    assert "stream-v1" in ref_names  # from create_tag

    # There should be two updates for `stream` (create at parent, then fast-forward to child)
    # and one for `stream-v1`. The commit protocol accepts multiple updates for the same ref.
    stream_updates = [u for u in set_ref_updates if u.ref_name == "stream"]
    assert len(stream_updates) == 2
    assert stream_updates[0].snapshot_id == parent_snapshot_id
    assert stream_updates[1].snapshot_id == 3055729675574597004
