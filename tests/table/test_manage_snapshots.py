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


def _staged_wap_table(
    catalog_with_warehouse: Catalog,
    namespace: str,
    advance_main: bool = True,
    wap_id: str | None = None,
) -> tuple[Table, int]:
    """Build the WAP shape: a staged branch write, with main optionally moved on since.

    Returns the reloaded table and the staged snapshot id.
    """
    catalog_with_warehouse.create_namespace(namespace)
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table(f"{namespace}.tbl", schema=schema)

    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table(f"{namespace}.tbl")

    tbl.manage_snapshots().create_branch(snapshot_id=_current_snapshot_id(tbl), branch_name="audit").commit()
    tbl = catalog_with_warehouse.load_table(f"{namespace}.tbl")

    tbl.append(
        pa.table({"id": [2, 3]}, schema=schema),
        branch="audit",
        snapshot_properties={"wap.id": wap_id} if wap_id else {},
    )
    tbl = catalog_with_warehouse.load_table(f"{namespace}.tbl")
    staged = tbl.metadata.refs["audit"].snapshot_id

    if advance_main:
        tbl.append(pa.table({"id": [9]}, schema=schema))
        tbl = catalog_with_warehouse.load_table(f"{namespace}.tbl")

    return tbl, staged


def _ids(tbl: Table) -> list[int]:
    return sorted(tbl.scan().to_arrow().column("id").to_pylist())


def _current_summary_props(tbl: Table) -> dict[str, str]:
    snapshot = tbl.current_snapshot()
    assert snapshot is not None and snapshot.summary is not None
    return snapshot.summary.additional_properties


def _current_snapshot_id(tbl: Table) -> int:
    snapshot_id = tbl.metadata.current_snapshot_id
    assert snapshot_id is not None
    return snapshot_id


def test_cherry_pick_snapshot_replays_staged_append(catalog_with_warehouse: Catalog) -> None:
    """The staged rows land on main even though main moved on after the branch was cut."""
    tbl, staged = _staged_wap_table(catalog_with_warehouse, "cp_replay")
    assert _ids(tbl) == [1, 9]

    tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()

    tbl = catalog_with_warehouse.load_table("cp_replay.tbl")
    assert _ids(tbl) == [1, 2, 3, 9]
    assert _current_summary_props(tbl)["source-snapshot-id"] == str(staged)


def test_cherry_pick_snapshot_records_published_wap_id(catalog_with_warehouse: Catalog) -> None:
    tbl, staged = _staged_wap_table(catalog_with_warehouse, "cp_wap", wap_id="etl-001")

    tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()

    tbl = catalog_with_warehouse.load_table("cp_wap.tbl")
    assert _current_summary_props(tbl)["published-wap-id"] == "etl-001"


def test_cherry_pick_snapshot_rejects_duplicate_wap_publish(catalog_with_warehouse: Catalog) -> None:
    """A wap id may only be published once."""
    tbl, staged = _staged_wap_table(catalog_with_warehouse, "cp_dup", wap_id="etl-001")
    tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()

    tbl = catalog_with_warehouse.load_table("cp_dup.tbl")
    with pytest.raises(ValueError, match="Duplicate request to cherry pick wap id"):
        tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()


def test_cherry_pick_snapshot_replays_append_even_when_parent_is_current(
    catalog_with_warehouse: Catalog,
) -> None:
    """Appends are always replayed, so the wap trail is recorded even on an unmoved table."""
    tbl, staged = _staged_wap_table(catalog_with_warehouse, "cp_ff", advance_main=False, wap_id="etl-001")
    assert _ids(tbl) == [1]

    tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()

    tbl = catalog_with_warehouse.load_table("cp_ff.tbl")
    assert _ids(tbl) == [1, 2, 3]
    assert tbl.metadata.current_snapshot_id != staged
    assert _current_summary_props(tbl)["published-wap-id"] == "etl-001"


def test_cherry_pick_snapshot_fast_forwards_non_append(catalog_with_warehouse: Catalog) -> None:
    """A non-append whose parent is current cannot be replayed, so the ref advances to it."""
    catalog_with_warehouse.create_namespace("cp_ffna")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("cp_ffna.tbl", schema=schema)
    tbl.append(pa.table({"id": [1, 2]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("cp_ffna.tbl")

    tbl.manage_snapshots().create_branch(snapshot_id=_current_snapshot_id(tbl), branch_name="audit").commit()
    tbl = catalog_with_warehouse.load_table("cp_ffna.tbl")
    tbl.delete("id = 1", branch="audit")
    tbl = catalog_with_warehouse.load_table("cp_ffna.tbl")
    staged = tbl.metadata.refs["audit"].snapshot_id

    tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()

    tbl = catalog_with_warehouse.load_table("cp_ffna.tbl")
    assert tbl.metadata.current_snapshot_id == staged
    assert _ids(tbl) == [2]


def test_cherry_pick_snapshot_is_noop_for_ancestor(catalog_with_warehouse: Catalog) -> None:
    """Picking a snapshot already in main's history changes nothing."""
    catalog_with_warehouse.create_namespace("cp_anc")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("cp_anc.tbl", schema=schema)
    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("cp_anc.tbl")
    first = _current_snapshot_id(tbl)
    tbl.append(pa.table({"id": [2]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("cp_anc.tbl")
    before = _current_snapshot_id(tbl)

    tbl.manage_snapshots().cherry_pick_snapshot(first).commit()

    tbl = catalog_with_warehouse.load_table("cp_anc.tbl")
    assert tbl.metadata.current_snapshot_id == before
    assert _ids(tbl) == [1, 2]


def test_cherry_pick_snapshot_rejects_unknown_snapshot(catalog_with_warehouse: Catalog) -> None:
    catalog_with_warehouse.create_namespace("cp_unknown")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("cp_unknown.tbl", schema=schema)
    tbl.append(pa.table({"id": [1]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("cp_unknown.tbl")

    with pytest.raises(ValueError, match="Cannot cherry-pick unknown snapshot id"):
        tbl.manage_snapshots().cherry_pick_snapshot(1234567890).commit()


def test_cherry_pick_snapshot_rejects_non_append(catalog_with_warehouse: Catalog) -> None:
    """Only append snapshots can be replayed; anything else must say so rather than silently skip."""
    catalog_with_warehouse.create_namespace("cp_nonappend")
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = catalog_with_warehouse.create_table("cp_nonappend.tbl", schema=schema)
    tbl.append(pa.table({"id": [1, 2]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("cp_nonappend.tbl")

    tbl.manage_snapshots().create_branch(snapshot_id=_current_snapshot_id(tbl), branch_name="audit").commit()
    tbl = catalog_with_warehouse.load_table("cp_nonappend.tbl")
    tbl.delete("id = 1", branch="audit")
    tbl = catalog_with_warehouse.load_table("cp_nonappend.tbl")
    staged = tbl.metadata.refs["audit"].snapshot_id

    tbl.append(pa.table({"id": [9]}, schema=schema))
    tbl = catalog_with_warehouse.load_table("cp_nonappend.tbl")

    with pytest.raises(ValueError, match="not append, dynamic overwrite, or fast-forward"):
        tbl.manage_snapshots().cherry_pick_snapshot(staged).commit()
