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
import uuid
from collections.abc import Generator

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRef


@pytest.fixture
def table_with_snapshots(session_catalog: Catalog) -> Generator[Table, None, None]:
    session_catalog.create_namespace_if_not_exists("default")
    identifier = f"default.test_table_snapshot_ops_{uuid.uuid4().hex[:8]}"

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("data", pa.string(), nullable=True),
        ]
    )

    tbl = session_catalog.create_table(identifier=identifier, schema=arrow_schema)

    data1 = pa.Table.from_pylist([{"id": 1, "data": "a"}, {"id": 2, "data": "b"}], schema=arrow_schema)
    tbl.append(data1)

    data2 = pa.Table.from_pylist([{"id": 3, "data": "c"}, {"id": 4, "data": "d"}], schema=arrow_schema)
    tbl.append(data2)

    tbl = session_catalog.load_table(identifier)

    yield tbl

    session_catalog.drop_table(identifier)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_tag(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    tag_snapshot_id = tbl.history()[-3].snapshot_id
    tbl.manage_snapshots().create_tag(snapshot_id=tag_snapshot_id, tag_name="tag123").commit()
    assert tbl.metadata.refs["tag123"] == SnapshotRef(snapshot_id=tag_snapshot_id, snapshot_ref_type="tag")


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_branch(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2
    branch_snapshot_id = tbl.history()[-2].snapshot_id
    tbl.manage_snapshots().create_branch(snapshot_id=branch_snapshot_id, branch_name="branch123").commit()
    assert tbl.metadata.refs["branch123"] == SnapshotRef(snapshot_id=branch_snapshot_id, snapshot_ref_type="branch")


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_tag(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    # first, create the tag to remove
    tag_name = "tag_to_remove"
    tag_snapshot_id = tbl.history()[-3].snapshot_id
    tbl.manage_snapshots().create_tag(snapshot_id=tag_snapshot_id, tag_name=tag_name).commit()
    assert tbl.metadata.refs[tag_name] == SnapshotRef(snapshot_id=tag_snapshot_id, snapshot_ref_type="tag")
    # now, remove the tag
    tbl.manage_snapshots().remove_tag(tag_name=tag_name).commit()
    assert tbl.metadata.refs.get(tag_name, None) is None


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_branch(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2
    # first, create the branch to remove
    branch_name = "branch_to_remove"
    branch_snapshot_id = tbl.history()[-2].snapshot_id
    tbl.manage_snapshots().create_branch(snapshot_id=branch_snapshot_id, branch_name=branch_name).commit()
    assert tbl.metadata.refs[branch_name] == SnapshotRef(snapshot_id=branch_snapshot_id, snapshot_ref_type="branch")
    # now, remove the branch
    tbl.manage_snapshots().remove_branch(branch_name=branch_name).commit()
    assert tbl.metadata.refs.get(branch_name, None) is None


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_set_current_snapshot(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2

    # first get the current snapshot and an older one
    current_snapshot_id = tbl.history()[-1].snapshot_id
    older_snapshot_id = tbl.history()[-2].snapshot_id

    # set the current snapshot to the older one
    tbl.manage_snapshots().set_current_snapshot(snapshot_id=older_snapshot_id).commit()

    tbl = catalog.load_table(identifier)
    updated_snapshot = tbl.current_snapshot()
    assert updated_snapshot and updated_snapshot.snapshot_id == older_snapshot_id

    # restore table
    tbl.manage_snapshots().set_current_snapshot(snapshot_id=current_snapshot_id).commit()
    tbl = catalog.load_table(identifier)
    restored_snapshot = tbl.current_snapshot()
    assert restored_snapshot and restored_snapshot.snapshot_id == current_snapshot_id


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_set_current_snapshot_by_ref(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2

    # first get the current snapshot and an older one
    current_snapshot_id = tbl.history()[-1].snapshot_id
    older_snapshot_id = tbl.history()[-2].snapshot_id
    assert older_snapshot_id != current_snapshot_id

    # create a tag pointing to the older snapshot
    tag_name = "my-tag"
    tbl.manage_snapshots().create_tag(snapshot_id=older_snapshot_id, tag_name=tag_name).commit()

    # set current snapshot using the tag name
    tbl = catalog.load_table(identifier)
    tbl.manage_snapshots().set_current_snapshot(ref_name=tag_name).commit()

    tbl = catalog.load_table(identifier)
    updated_snapshot = tbl.current_snapshot()
    assert updated_snapshot and updated_snapshot.snapshot_id == older_snapshot_id

    # restore table
    tbl.manage_snapshots().set_current_snapshot(snapshot_id=current_snapshot_id).commit()
    tbl = catalog.load_table(identifier)
    tbl.manage_snapshots().remove_tag(tag_name=tag_name).commit()
    assert tbl.metadata.refs.get(tag_name, None) is None


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_set_current_snapshot_chained_with_create_tag(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2

    current_snapshot_id = tbl.history()[-1].snapshot_id
    older_snapshot_id = tbl.history()[-2].snapshot_id
    assert older_snapshot_id != current_snapshot_id

    # create a tag and use it to set current snapshot
    tag_name = "my-tag"
    (
        tbl.manage_snapshots()
        .create_tag(snapshot_id=older_snapshot_id, tag_name=tag_name)
        .set_current_snapshot(ref_name=tag_name)
        .commit()
    )

    tbl = catalog.load_table(identifier)
    updated_snapshot = tbl.current_snapshot()
    assert updated_snapshot
    assert updated_snapshot.snapshot_id == older_snapshot_id

    # restore table
    tbl.manage_snapshots().set_current_snapshot(snapshot_id=current_snapshot_id).commit()
    tbl = catalog.load_table(identifier)
    tbl.manage_snapshots().remove_tag(tag_name=tag_name).commit()
    assert tbl.metadata.refs.get(tag_name, None) is None


@pytest.mark.integration
def test_rollback_to_snapshot(table_with_snapshots: Table) -> None:
    history = table_with_snapshots.history()
    assert len(history) >= 2

    ancestor_snapshot_id = history[-2].snapshot_id

    table_with_snapshots.manage_snapshots().rollback_to_snapshot(snapshot_id=ancestor_snapshot_id).commit()

    updated = table_with_snapshots.current_snapshot()
    assert updated is not None
    assert updated.snapshot_id == ancestor_snapshot_id


@pytest.mark.integration
def test_rollback_to_current_snapshot(table_with_snapshots: Table) -> None:
    current = table_with_snapshots.current_snapshot()
    assert current is not None

    table_with_snapshots.manage_snapshots().rollback_to_snapshot(snapshot_id=current.snapshot_id).commit()

    updated = table_with_snapshots.current_snapshot()
    assert updated is not None
    assert updated.snapshot_id == current.snapshot_id


@pytest.mark.integration
def test_rollback_to_snapshot_chained_with_tag(table_with_snapshots: Table) -> None:
    history = table_with_snapshots.history()
    assert len(history) >= 2

    ancestor_snapshot_id = history[-2].snapshot_id
    tag_name = "my-tag"

    (
        table_with_snapshots.manage_snapshots()
        .create_tag(snapshot_id=ancestor_snapshot_id, tag_name=tag_name)
        .rollback_to_snapshot(snapshot_id=ancestor_snapshot_id)
        .commit()
    )

    updated = table_with_snapshots.current_snapshot()
    assert updated is not None
    assert updated.snapshot_id == ancestor_snapshot_id
    assert table_with_snapshots.metadata.refs[tag_name] == SnapshotRef(snapshot_id=ancestor_snapshot_id, snapshot_ref_type="tag")


@pytest.mark.integration
def test_rollback_to_snapshot_not_ancestor(table_with_snapshots: Table) -> None:
    history = table_with_snapshots.history()
    assert len(history) >= 2

    snapshot_a = history[-2].snapshot_id

    branch_name = "my-branch"
    table_with_snapshots.manage_snapshots().create_branch(snapshot_id=snapshot_a, branch_name=branch_name).commit()

    data = pa.Table.from_pylist([{"id": 5, "data": "e"}], schema=table_with_snapshots.schema().as_arrow())
    table_with_snapshots.append(data, branch=branch_name)

    snapshot_c = table_with_snapshots.metadata.snapshot_by_name(branch_name)
    assert snapshot_c is not None
    assert snapshot_c.snapshot_id != snapshot_a

    with pytest.raises(ValueError, match="not an ancestor"):
        table_with_snapshots.manage_snapshots().rollback_to_snapshot(snapshot_id=snapshot_c.snapshot_id).commit()


@pytest.mark.integration
def test_rollback_to_snapshot_unknown_id(table_with_snapshots: Table) -> None:
    invalid_snapshot_id = 1234567890000

    with pytest.raises(ValueError, match="Cannot roll back to unknown snapshot id"):
        table_with_snapshots.manage_snapshots().rollback_to_snapshot(snapshot_id=invalid_snapshot_id).commit()


@pytest.mark.integration
def test_rollback_to_timestamp_no_valid_snapshot(table_with_snapshots: Table) -> None:
    history = table_with_snapshots.history()
    assert len(history) >= 1

    oldest_timestamp = history[0].timestamp_ms

    with pytest.raises(ValueError, match="Cannot roll back, no valid snapshot older than"):
        table_with_snapshots.manage_snapshots().rollback_to_timestamp(timestamp_ms=oldest_timestamp).commit()


@pytest.mark.integration
def test_rollback_to_timestamp(table_with_snapshots: Table) -> None:
    current_snapshot = table_with_snapshots.current_snapshot()
    assert current_snapshot is not None
    assert current_snapshot.parent_snapshot_id is not None

    parent_snapshot_id = current_snapshot.parent_snapshot_id

    table_with_snapshots.manage_snapshots().rollback_to_timestamp(timestamp_ms=current_snapshot.timestamp_ms).commit()

    updated_snapshot = table_with_snapshots.current_snapshot()
    assert updated_snapshot is not None
    assert updated_snapshot.snapshot_id == parent_snapshot_id


@pytest.mark.integration
def test_rollback_to_timestamp_current_snapshot(table_with_snapshots: Table) -> None:
    current_snapshot = table_with_snapshots.current_snapshot()
    assert current_snapshot is not None

    timestamp_after_current = current_snapshot.timestamp_ms + 100
    table_with_snapshots.manage_snapshots().rollback_to_timestamp(timestamp_ms=timestamp_after_current).commit()

    updated_snapshot = table_with_snapshots.current_snapshot()
    assert updated_snapshot is not None
    assert updated_snapshot.snapshot_id == current_snapshot.snapshot_id


@pytest.mark.integration
def test_rollback_to_timestamp_chained_with_tag(table_with_snapshots: Table) -> None:
    current_snapshot = table_with_snapshots.current_snapshot()
    assert current_snapshot is not None
    assert current_snapshot.parent_snapshot_id is not None

    parent_snapshot_id = current_snapshot.parent_snapshot_id
    tag_name = "my-tag"

    (
        table_with_snapshots.manage_snapshots()
        .create_tag(snapshot_id=current_snapshot.snapshot_id, tag_name=tag_name)
        .rollback_to_timestamp(timestamp_ms=current_snapshot.timestamp_ms)
        .commit()
    )

    updated_snapshot = table_with_snapshots.current_snapshot()
    assert updated_snapshot is not None
    assert updated_snapshot.snapshot_id == parent_snapshot_id
    assert table_with_snapshots.metadata.refs[tag_name] == SnapshotRef(
        snapshot_id=current_snapshot.snapshot_id, snapshot_ref_type="tag"
    )
