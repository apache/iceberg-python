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

from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.update import AssertRefSnapshotId, SetSnapshotRefUpdate, TableRequirement, TableUpdate

PARENT_SNAPSHOT_ID = 3051729675574597004
CHILD_SNAPSHOT_ID = 3055729675574597004


def _mock_commit_response(table: Table) -> CommitTableResponse:
    return CommitTableResponse(
        metadata=table.metadata,
        metadata_location="s3://bucket/tbl",
        uuid=uuid4(),
    )


def _get_updates(mock_catalog: MagicMock) -> tuple[TableUpdate, ...]:
    args, _ = mock_catalog.commit_table.call_args
    return args[2]


def _get_requirements(mock_catalog: MagicMock) -> tuple[TableRequirement, ...]:
    args, _ = mock_catalog.commit_table.call_args
    return args[1]


def _get_set_ref_updates(mock_catalog: MagicMock) -> list[SetSnapshotRefUpdate]:
    return [update for update in _get_updates(mock_catalog) if isinstance(update, SetSnapshotRefUpdate)]


def test_set_current_snapshot_basic(table_v2: Table) -> None:
    snapshot_one = 3051729675574597004

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().set_current_snapshot(snapshot_id=snapshot_one).commit()

    table_v2.catalog.commit_table.assert_called_once()

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)

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

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)

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

    set_ref_updates = _get_set_ref_updates(table_v2_with_extensive_snapshots.catalog)

    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].snapshot_id == target_snapshot


def test_set_current_snapshot_by_ref_name(table_v2: Table) -> None:
    current_snapshot = table_v2.current_snapshot()
    assert current_snapshot is not None

    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().set_current_snapshot(ref_name="main").commit()

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)

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

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)

    # should have the tag and the main branch update
    assert len(set_ref_updates) == 2
    assert {u.ref_name for u in set_ref_updates} == {"new-tag", "main"}

    # The main branch should point to the same snapshot as the tag
    main_update = next(u for u in set_ref_updates if u.ref_name == "main")
    assert main_update.snapshot_id == snapshot_one


def test_fast_forward_branch_basic(table_v2: Table) -> None:
    table_v2.metadata.refs["lagging"] = SnapshotRef(
        snapshot_id=PARENT_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.BRANCH,
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="lagging", to_ref="main").commit()

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)
    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].ref_name == "lagging"
    assert set_ref_updates[0].snapshot_id == CHILD_SNAPSHOT_ID
    assert set_ref_updates[0].type == SnapshotRefType.BRANCH


def test_fast_forward_branch_to_tag(table_v2: Table) -> None:
    table_v2.metadata.refs["lagging"] = SnapshotRef(
        snapshot_id=PARENT_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.BRANCH,
    )
    table_v2.metadata.refs["release"] = SnapshotRef(
        snapshot_id=CHILD_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.TAG,
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="lagging", to_ref="release").commit()

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)
    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].ref_name == "lagging"
    assert set_ref_updates[0].snapshot_id == CHILD_SNAPSHOT_ID
    assert set_ref_updates[0].type == SnapshotRefType.BRANCH


def test_fast_forward_branch_creates_branch(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="brand-new", to_ref="main").commit()

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)
    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.ref_name == "brand-new"
    assert update.snapshot_id == CHILD_SNAPSHOT_ID
    assert update.type == SnapshotRefType.BRANCH
    # a branch created by the fast-forward gets the default retention
    assert update.max_ref_age_ms is None
    assert update.max_snapshot_age_ms is None
    assert update.min_snapshots_to_keep is None


def test_fast_forward_branch_noop(table_v2: Table) -> None:
    table_v2.metadata.refs["peer"] = SnapshotRef(
        snapshot_id=CHILD_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.BRANCH,
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="peer", to_ref="main").commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_preserves_retention(table_v2: Table) -> None:
    table_v2.metadata.refs["lagging"] = SnapshotRef(
        snapshot_id=PARENT_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.BRANCH,
        max_ref_age_ms=3_600_000,
        max_snapshot_age_ms=7_200_000,
        min_snapshots_to_keep=5,
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="lagging", to_ref="main").commit()

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)
    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.snapshot_id == CHILD_SNAPSHOT_ID
    assert update.max_ref_age_ms == 3_600_000
    assert update.max_snapshot_age_ms == 7_200_000
    assert update.min_snapshots_to_keep == 5


def test_fast_forward_branch_chained_with_create_branch(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    with table_v2.manage_snapshots() as ms:
        ms.create_branch(
            snapshot_id=PARENT_SNAPSHOT_ID,
            branch_name="staging",
            max_ref_age_ms=3_600_000,
            max_snapshot_age_ms=7_200_000,
            min_snapshots_to_keep=5,
        ).fast_forward_branch(from_branch="staging", to_ref="main")

    # the fast-forward sees the branch created in the same chain and replaces its update
    set_ref_updates = _get_set_ref_updates(table_v2.catalog)
    assert len(set_ref_updates) == 1
    update = set_ref_updates[0]
    assert update.ref_name == "staging"
    assert update.snapshot_id == CHILD_SNAPSHOT_ID
    assert update.max_ref_age_ms == 3_600_000
    assert update.max_snapshot_age_ms == 7_200_000
    assert update.min_snapshots_to_keep == 5


def test_fast_forward_branch_unknown_ref(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Ref does not exist: nonexistent"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="nonexistent")

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_from_tag(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Ref test is a tag not a branch"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="test", to_ref="main")

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_not_ancestor(table_v2: Table) -> None:
    # the tag points at the parent of the current snapshot
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot fast-forward: main is not an ancestor of test"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="test")

    table_v2.catalog.commit_table.assert_not_called()


def test_fast_forward_branch_chained_with_remove_branch(table_v2: Table) -> None:
    table_v2.metadata.refs["stale"] = SnapshotRef(
        snapshot_id=PARENT_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.BRANCH,
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    # the branch removed earlier in the chain is created again at main
    with table_v2.manage_snapshots() as ms:
        ms.remove_branch("stale").fast_forward_branch(from_branch="stale", to_ref="main")

    set_ref_updates = _get_set_ref_updates(table_v2.catalog)
    assert len(set_ref_updates) == 1
    assert set_ref_updates[0].ref_name == "stale"
    assert set_ref_updates[0].snapshot_id == CHILD_SNAPSHOT_ID


def test_fast_forward_branch_requirement(table_v2: Table) -> None:
    table_v2.metadata.refs["lagging"] = SnapshotRef(
        snapshot_id=PARENT_SNAPSHOT_ID,
        snapshot_ref_type=SnapshotRefType.BRANCH,
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    table_v2.manage_snapshots().fast_forward_branch(from_branch="lagging", to_ref="main").commit()

    # the requirement asserts the committed snapshot of the branch
    ref_requirements = [r for r in _get_requirements(table_v2.catalog) if isinstance(r, AssertRefSnapshotId)]
    assert len(ref_requirements) == 1
    assert ref_requirements[0].ref == "lagging"
    assert ref_requirements[0].snapshot_id == PARENT_SNAPSHOT_ID


def test_fast_forward_branch_chained_requirement(table_v2: Table) -> None:
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

    with table_v2.manage_snapshots() as ms:
        ms.create_branch(snapshot_id=PARENT_SNAPSHOT_ID, branch_name="staging").fast_forward_branch(
            from_branch="staging", to_ref="main"
        )

    ref_requirements = [r for r in _get_requirements(table_v2.catalog) if isinstance(r, AssertRefSnapshotId)]
    assert len(ref_requirements) == 1
    assert ref_requirements[0].ref == "staging"
    assert ref_requirements[0].snapshot_id is None


def test_fast_forward_branch_unknown_snapshot(table_v2: Table) -> None:
    table_v2.metadata.refs["dangling"] = SnapshotRef(
        snapshot_id=1234567890000,
        snapshot_ref_type=SnapshotRefType.BRANCH,
    )
    table_v2.catalog = MagicMock()

    with pytest.raises(ValueError, match="Cannot fast-forward to unknown snapshot id: 1234567890000"):
        table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="dangling")

    table_v2.catalog.commit_table.assert_not_called()
