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
from pyiceberg.table.refs import SnapshotRef


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
