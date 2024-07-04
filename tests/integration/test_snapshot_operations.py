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
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import ancestors_of


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_tag(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    tag_snapshot_id = tbl.history()[-3].snapshot_id
    tbl.manage_snapshots().create_tag(snapshot_id=tag_snapshot_id, tag_name="tag123").commit()
    assert tbl.metadata.refs["tag123"] == SnapshotRef(snapshot_id=tag_snapshot_id, snapshot_ref_type=str(SnapshotRefType.TAG))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_branch(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2
    branch_snapshot_id = tbl.history()[-2].snapshot_id
    tbl.manage_snapshots().create_branch(snapshot_id=branch_snapshot_id, branch_name="branch123").commit()
    assert tbl.metadata.refs["branch123"] == SnapshotRef(
        snapshot_id=branch_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_manage_snapshots_context_manager(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    current_snapshot_id = tbl.current_snapshot().snapshot_id  # type: ignore
    expected_snapshot_id = tbl.history()[-4].snapshot_id
    with tbl.manage_snapshots() as ms:
        ms.create_tag(snapshot_id=current_snapshot_id, tag_name="testing")
        ms.set_current_snapshot(snapshot_id=expected_snapshot_id)
        ms.create_branch(snapshot_id=expected_snapshot_id, branch_name="testing2")
    assert tbl.current_snapshot().snapshot_id != current_snapshot_id  # type: ignore
    assert tbl.metadata.refs["testing"].snapshot_id == current_snapshot_id
    assert tbl.metadata.refs[MAIN_BRANCH] == SnapshotRef(
        snapshot_id=expected_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )
    assert tbl.metadata.refs["testing2"].snapshot_id == expected_snapshot_id


# Maintain relative order of tests for following apis like rollback, set_current_snapshot, etc.
@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_rollback_to_snapshot(catalog: Catalog) -> None:
    identifier = "default.test_table_rollback_to_snapshot_id"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    rollback_snapshot_id = tbl.current_snapshot().parent_snapshot_id  # type: ignore
    current_snapshot_id = tbl.current_snapshot().snapshot_id  # type: ignore
    tbl.manage_snapshots().rollback_to_snapshot(snapshot_id=rollback_snapshot_id).commit()  # type: ignore
    assert tbl.current_snapshot().snapshot_id != current_snapshot_id  # type: ignore
    assert tbl.metadata.refs[MAIN_BRANCH] == SnapshotRef(
        snapshot_id=rollback_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_rollback_to_timestamp(catalog: Catalog) -> None:
    identifier = "default.test_table_rollback_to_snapshot_id"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 4
    ancestors = list(ancestor for ancestor in ancestors_of(tbl.current_snapshot(), tbl.metadata))  # noqa
    ancestor_to_rollback_to = ancestors[-1]
    expected_snapshot_id, timestamp = ancestor_to_rollback_to.snapshot_id, ancestor_to_rollback_to.timestamp_ms + 1
    # not inclusive of rollback_timestamp
    tbl.manage_snapshots().rollback_to_timestamp(timestamp=timestamp).commit()
    assert tbl.metadata.refs[MAIN_BRANCH] == SnapshotRef(
        snapshot_id=expected_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_set_current_snapshot_with_snapshot_id(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    current_snapshot_id = tbl.current_snapshot().snapshot_id  # type: ignore
    expected_snapshot_id = tbl.history()[-3].snapshot_id
    tbl.manage_snapshots().set_current_snapshot(snapshot_id=expected_snapshot_id).commit()
    assert tbl.current_snapshot().snapshot_id != current_snapshot_id  # type: ignore
    assert tbl.metadata.refs[MAIN_BRANCH] == SnapshotRef(
        snapshot_id=expected_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_set_current_snapshot_with_ref_name(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    current_snapshot_id = tbl.current_snapshot().snapshot_id  # type: ignore
    expected_snapshot_id = tbl.history()[-3].snapshot_id
    tbl.manage_snapshots().create_tag(snapshot_id=expected_snapshot_id, tag_name="test-tag").commit()
    tbl.manage_snapshots().set_current_snapshot(ref_name="test-tag").commit()
    assert tbl.current_snapshot().snapshot_id != current_snapshot_id  # type: ignore
    assert tbl.metadata.refs[MAIN_BRANCH] == SnapshotRef(
        snapshot_id=expected_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )


# Always test set_ref_snapshot last.
@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_set_ref_snapshot(catalog: Catalog) -> None:
    identifier = "default.test_table_snapshot_operations"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 3
    target_snapshot_id = tbl.history()[-2].snapshot_id
    tbl.manage_snapshots()._set_ref_snapshot(
        snapshot_id=target_snapshot_id, ref_name=MAIN_BRANCH, type=str(SnapshotRefType.BRANCH)
    ).commit()
    assert tbl.metadata.refs[MAIN_BRANCH] == SnapshotRef(
        snapshot_id=target_snapshot_id, snapshot_ref_type=str(SnapshotRefType.BRANCH)
    )
