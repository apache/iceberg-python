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
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from pyiceberg.exceptions import (
    NoSuchSnapshotRefError,
    NotAncestorError,
    SnapshotRefTypeError,
)
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.update import SetSnapshotRefUpdate, TableRequirement, TableUpdate

# The two snapshots in the table_v2 fixture: CHILD's parent is PARENT.
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


@pytest.fixture
def table_v2_main_behind(example_table_metadata_v2: dict[str, Any]) -> Table:
    """``table_v2`` with main rewound to the parent snapshot and an "audit" branch at the head.

    This is the write-audit-publish mid-flight state: main lags, the side branch is ahead, so
    fast-forwarding *main* onto it is a real advance — the direction the API docs document.
    Built before parsing because ``current_snapshot_id`` is frozen on the metadata model.
    """
    metadata = TableMetadataUtil.parse_obj(
        {
            **example_table_metadata_v2,
            "current-snapshot-id": PARENT_SNAPSHOT_ID,
            "refs": {
                "main": {"snapshot-id": PARENT_SNAPSHOT_ID, "type": "branch"},
                "audit": {"snapshot-id": CHILD_SNAPSHOT_ID, "type": "branch"},
                "test": {"snapshot-id": PARENT_SNAPSHOT_ID, "type": "tag", "max-ref-age-ms": 10000000},
            },
        }
    )
    return Table(
        identifier=("database", "table"),
        metadata=metadata,
        metadata_location="s3://bucket/test/location/metadata/v1.json",
        io=PyArrowFileIO(),
        catalog=MagicMock(),
    )


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


class TestFastForwardBranchSuccess:
    def test_fast_forward_branch__with_existing_from_branch__advances_to_descendant_branch(
        self, table_v2_main_behind: Table
    ) -> None:
        """Publishing main onto a side branch that is ahead of it — the documented direction."""
        table = table_v2_main_behind
        table.catalog = MagicMock()
        table.catalog.commit_table.return_value = _mock_commit_response(table)

        table.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="audit").commit()

        set_ref_updates = [u for u in _get_updates(table.catalog) if isinstance(u, SetSnapshotRefUpdate)]

        assert len(set_ref_updates) == 1
        update = set_ref_updates[0]
        assert update.ref_name == "main"
        assert update.snapshot_id == CHILD_SNAPSHOT_ID
        assert update.type == "branch"

    def test_fast_forward_branch__with_existing_from_branch__advances_to_descendant_tag(
        self, table_v2_main_behind: Table
    ) -> None:
        """A tag is a valid fast-forward target, even though a tag is rejected as the source."""
        table = table_v2_main_behind

        # Tag the head so main can be published onto a tag rather than a branch. The fixture's
        # own "test" tag points at the parent, so a new one is needed here.
        table.metadata.refs["at-head"] = SnapshotRef(
            snapshot_id=CHILD_SNAPSHOT_ID,
            snapshot_ref_type="tag",
        )

        table.catalog = MagicMock()
        table.catalog.commit_table.return_value = _mock_commit_response(table)

        table.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="at-head").commit()

        set_ref_updates = [u for u in _get_updates(table.catalog) if isinstance(u, SetSnapshotRefUpdate)]

        assert len(set_ref_updates) == 1
        update = set_ref_updates[0]
        assert update.ref_name == "main"
        assert update.snapshot_id == CHILD_SNAPSHOT_ID
        assert update.type == "branch"

    def test_fast_forward_branch__with_no_from_branch__creates_missing_from_branch(self, table_v2: Table) -> None:
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

    def test_fast_forward_branch__when_refs_equal__does_not_commit(self, table_v2: Table) -> None:
        """A fast-forward between two refs already at the same snapshot never reaches the catalog.

        "audit" is injected straight into ``metadata.refs`` at main's own head, so the equality
        short-circuit is reached without any earlier staged operation in the chain.

        This is the transaction-level guarantee: the whole transaction ends up empty, so
        ``commit_transaction`` short-circuits. That the *operation* staged nothing is pinned
        separately by ``..._when_noop_and_other_chained_updates__stages_only_the_other``.
        """
        table_v2.metadata.refs["audit"] = SnapshotRef(
            snapshot_id=table_v2.metadata.refs["main"].snapshot_id,
            snapshot_ref_type="branch",
        )

        table_v2.catalog = MagicMock()
        table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

        table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="audit").commit()

        # A pure no-op stages no updates; commit_transaction short-circuits.
        table_v2.catalog.commit_table.assert_not_called()

    def test_fast_forward_branch__with_operation_chaining__succeeds(self, table_v2: Table) -> None:
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

    def test_fast_forward_branch__when_noop_and_other_chained_updates__stages_only_the_other(self, table_v2: Table) -> None:
        """A no-op fast-forward contributes neither an update nor a requirement."""

        # "audit" is put at the very snapshot "main" already points to, so publishing main is a no-op.
        table_v2.metadata.refs["audit"] = SnapshotRef(
            snapshot_id=table_v2.metadata.refs["main"].snapshot_id,
            snapshot_ref_type="branch",
        )

        table_v2.catalog = MagicMock()
        table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

        # The tag is the unrelated real update that keeps the transaction non-empty; its
        # snapshot just needs to exist and is deliberately not main's.
        tagged_snapshot_id = 3051729675574597004

        (
            table_v2.manage_snapshots()
            .fast_forward_branch(from_branch="main", to_ref="audit")
            .create_tag(snapshot_id=tagged_snapshot_id, tag_name="t1")
            .commit()
        )

        set_ref_updates = [u for u in _get_updates(table_v2.catalog) if isinstance(u, SetSnapshotRefUpdate)]

        # Only the tag is staged; the no-op fast-forward adds nothing.
        assert len(set_ref_updates) == 1
        assert set_ref_updates[0].ref_name == "t1"

        # No requirement either. A stray AssertRefSnapshotId for "main" would make this
        # transaction conflict with a concurrent writer for no reason.
        assert all(getattr(r, "ref", None) != "main" for r in _get_requirements(table_v2.catalog))

    def test_fast_forward_branch__when_applied_twice_in_one_chain__second_is_noop(self, table_v2_main_behind: Table) -> None:
        """The second fast-forward observes the first via ``_effective_refs`` and stages nothing."""
        table = table_v2_main_behind
        table.catalog = MagicMock()
        table.catalog.commit_table.return_value = _mock_commit_response(table)

        (
            table.manage_snapshots()
            .fast_forward_branch(from_branch="main", to_ref="audit")
            .fast_forward_branch(from_branch="main", to_ref="audit")  # this is a noop
            .commit()
        )

        main_updates = [u for u in _get_updates(table.catalog) if isinstance(u, SetSnapshotRefUpdate) and u.ref_name == "main"]

        assert len(main_updates) == 1
        assert main_updates[0].snapshot_id == CHILD_SNAPSHOT_ID


class TestFastForwardRejectionCases:
    def test_fast_forward_branch__with_tag_as_source__rejects(self, table_v2: Table) -> None:
        # Precondition: the fixture provides a tag named "test".
        assert table_v2.metadata.refs["test"].snapshot_ref_type == SnapshotRefType.TAG
        table_v2.catalog = MagicMock()

        with pytest.raises(SnapshotRefTypeError, match="Ref test is a tag, not a branch"):
            table_v2.manage_snapshots().fast_forward_branch(from_branch="test", to_ref="main").commit()

        table_v2.catalog.commit_table.assert_not_called()

    def test_fast_forward_branch__with_missing_to_ref__rejects(self, table_v2: Table) -> None:
        table_v2.catalog = MagicMock()

        with pytest.raises(NoSuchSnapshotRefError, match="Ref does not exist: nonexistent"):
            table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="nonexistent").commit()

        table_v2.catalog.commit_table.assert_not_called()

    def test_fast_forward_branch__with_non_ancestor__rejects(self, table_v2: Table) -> None:
        """A branch ahead of ``to_ref`` cannot be fast-forwarded backwards onto it.

        No setup is needed: main already sits at the head while the "test" tag points at the
        parent, so publishing main onto that tag is a backwards move.
        """
        table_v2.catalog = MagicMock()

        with pytest.raises(NotAncestorError, match="Cannot fast-forward: main is not an ancestor of test"):
            table_v2.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="test").commit()

        table_v2.catalog.commit_table.assert_not_called()

    def test_fast_forward_branch__with_tagging_in_operation_chaining__rejects(self, table_v2: Table) -> None:
        """
        A tag staged earlier in the same chain must be observed as a tag by a later fast_forward_branch.

        Without _effective_refs, the tag wouldn't appear in refs and fast_forward_branch's auto-create path would
        silently create a branch of the same name, subverting the tag.
        """
        parent_snapshot_id = 3051729675574597004

        table_v2.catalog = MagicMock()

        with pytest.raises(SnapshotRefTypeError, match="Ref mytag is a tag, not a branch"):
            (
                table_v2.manage_snapshots()
                .create_tag(snapshot_id=parent_snapshot_id, tag_name="mytag")
                .fast_forward_branch(from_branch="mytag", to_ref="main")
                .commit()
            )

        table_v2.catalog.commit_table.assert_not_called()


class TestFastForwardRetentionCases:
    def test_fast_forward_branch__with_retention_fields__fields_get_preserved(self, table_v2_main_behind: Table) -> None:
        """Retention configured on the branch being published survives the advance."""
        table = table_v2_main_behind

        # Give main all three retention fields, leaving it at the parent so
        # current-snapshot-id and the main ref stay in agreement.
        table.metadata.refs["main"] = SnapshotRef(
            snapshot_id=PARENT_SNAPSHOT_ID,
            snapshot_ref_type="branch",
            max_ref_age_ms=1000,
            max_snapshot_age_ms=2000,
            min_snapshots_to_keep=3,
        )

        table.catalog = MagicMock()
        table.catalog.commit_table.return_value = _mock_commit_response(table)

        table.manage_snapshots().fast_forward_branch(from_branch="main", to_ref="audit").commit()

        set_ref_updates = [u for u in _get_updates(table.catalog) if isinstance(u, SetSnapshotRefUpdate)]

        assert len(set_ref_updates) == 1
        update = set_ref_updates[0]
        assert update.ref_name == "main"
        assert update.snapshot_id == CHILD_SNAPSHOT_ID
        assert update.max_ref_age_ms == 1000
        assert update.max_snapshot_age_ms == 2000
        assert update.min_snapshots_to_keep == 3

    def test_fast_forward_branch__with_operation_chaining__preserves_retention(self, table_v2: Table) -> None:
        """
        With _effective_refs, a fast-forward that observes a same-chain create_branch preserves the branch's retention fields.
        Without _effective_refs, these fields would be ignored on the fast_forward branch creation.
        """
        parent_snapshot_id = 3051729675574597004
        child_snapshot_id = 3055729675574597004  # main's current snapshot

        table_v2.catalog = MagicMock()
        table_v2.catalog.commit_table.return_value = _mock_commit_response(table_v2)

        (
            table_v2.manage_snapshots()
            .create_branch(
                snapshot_id=parent_snapshot_id,
                branch_name="feature",
                max_ref_age_ms=5000,
                max_snapshot_age_ms=6000,
                min_snapshots_to_keep=7,
            )
            .fast_forward_branch(from_branch="feature", to_ref="main")
            .commit()
        )

        updates = _get_updates(table_v2.catalog)
        feature_updates = [u for u in updates if isinstance(u, SetSnapshotRefUpdate) and u.ref_name == "feature"]
        assert len(feature_updates) == 2

        # First: create_branch stages "feature" at parent with retention.
        assert feature_updates[0].snapshot_id == parent_snapshot_id
        assert feature_updates[0].max_ref_age_ms == 5000
        assert feature_updates[0].max_snapshot_age_ms == 6000
        assert feature_updates[0].min_snapshots_to_keep == 7

        # Second: fast_forward_branch observes the staged branch via _effective_refs,
        #   advances it to main's snapshot, and preserves retention fields.
        assert feature_updates[1].snapshot_id == child_snapshot_id
        assert feature_updates[1].max_ref_age_ms == 5000
        assert feature_updates[1].max_snapshot_age_ms == 6000
        assert feature_updates[1].min_snapshots_to_keep == 7
