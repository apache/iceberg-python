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

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
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


def test_branch_write_preserves_retention(catalog_with_warehouse: Catalog) -> None:
    """Writing to a branch keeps the retention policy it was created with."""
    catalog_with_warehouse.create_namespace("branch_retention")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("branch_retention.tbl", schema=schema)
    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("branch_retention.tbl")

    snapshot_id = tbl.metadata.current_snapshot_id
    assert snapshot_id is not None
    tbl.manage_snapshots().create_branch(
        snapshot_id=snapshot_id,
        branch_name="audit",
        max_ref_age_ms=86400000,
        max_snapshot_age_ms=3600000,
        min_snapshots_to_keep=5,
    ).commit()

    for i in range(3):
        tbl = catalog_with_warehouse.load_table("branch_retention.tbl")
        tbl.append(pa.table({"id": [i + 2]}, schema=schema), branch="audit")

    ref = catalog_with_warehouse.load_table("branch_retention.tbl").metadata.refs["audit"]
    assert ref.max_ref_age_ms == 86400000
    assert ref.max_snapshot_age_ms == 3600000
    assert ref.min_snapshots_to_keep == 5


def test_branch_write_without_retention_stays_unset(catalog_with_warehouse: Catalog) -> None:
    """A branch created without a retention policy does not gain one from a write."""
    catalog_with_warehouse.create_namespace("branch_no_retention")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("branch_no_retention.tbl", schema=schema)
    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("branch_no_retention.tbl")

    snapshot_id = tbl.metadata.current_snapshot_id
    assert snapshot_id is not None
    tbl.manage_snapshots().create_branch(snapshot_id=snapshot_id, branch_name="plain").commit()

    tbl = catalog_with_warehouse.load_table("branch_no_retention.tbl")
    tbl.append(pa.table({"id": [2]}, schema=schema), branch="plain")

    ref = catalog_with_warehouse.load_table("branch_no_retention.tbl").metadata.refs["plain"]
    assert ref.max_ref_age_ms is None
    assert ref.max_snapshot_age_ms is None
    assert ref.min_snapshots_to_keep is None


def test_rollback_preserves_retention(catalog_with_warehouse: Catalog) -> None:
    """Moving a ref keeps its retention policy; rollback and set_current_snapshot move main."""
    catalog_with_warehouse.create_namespace("rollback_retention")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("rollback_retention.tbl", schema=schema)
    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("rollback_retention.tbl")
    first = tbl.metadata.current_snapshot_id
    assert first is not None

    tbl.append(pa.table({"id": [2]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("rollback_retention.tbl")
    head = tbl.metadata.current_snapshot_id
    assert head is not None

    tbl.manage_snapshots().create_branch(
        snapshot_id=head,
        branch_name="main",
        max_ref_age_ms=99999,
        max_snapshot_age_ms=1234,
        min_snapshots_to_keep=7,
    ).commit()

    tbl = catalog_with_warehouse.load_table("rollback_retention.tbl")
    tbl.manage_snapshots().rollback_to_snapshot(first).commit()

    ref = catalog_with_warehouse.load_table("rollback_retention.tbl").metadata.refs["main"]
    assert ref.snapshot_id == first
    assert ref.max_ref_age_ms == 99999
    assert ref.max_snapshot_age_ms == 1234
    assert ref.min_snapshots_to_keep == 7


def test_rollback_without_retention_stays_unset(catalog_with_warehouse: Catalog) -> None:
    """A ref with no retention policy does not gain one from being moved."""
    catalog_with_warehouse.create_namespace("rollback_plain")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("rollback_plain.tbl", schema=schema)
    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("rollback_plain.tbl")
    first = tbl.metadata.current_snapshot_id
    assert first is not None

    tbl.append(pa.table({"id": [2]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("rollback_plain.tbl")
    tbl.manage_snapshots().rollback_to_snapshot(first).commit()

    ref = catalog_with_warehouse.load_table("rollback_plain.tbl").metadata.refs["main"]
    assert ref.max_ref_age_ms is None
    assert ref.max_snapshot_age_ms is None
    assert ref.min_snapshots_to_keep is None
