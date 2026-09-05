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
from pathlib import Path

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.manifest import ManifestContent, ManifestFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation


@pytest.fixture
def catalog(tmp_path: Path) -> Catalog:
    catalog = InMemoryCatalog("test.rewrite_manifests", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("default")
    return catalog


def _arrow_table(offset: int = 0) -> pa.Table:
    return pa.table({"id": pa.array([offset + 1, offset + 2, offset + 3], type=pa.int64())})


def _create_table_with_appends(catalog: Catalog, appends: int = 3) -> Table:
    table = catalog.create_table("default.test_rewrite", schema=pa.schema([pa.field("id", pa.int64())]))
    for i in range(appends):
        table.append(_arrow_table(offset=i * 3))
    return table


def _data_manifests(table: Table) -> list[ManifestFile]:
    snapshot = table.current_snapshot()
    assert snapshot is not None
    return [m for m in snapshot.manifests(table.io) if m.content == ManifestContent.DATA]


def test_rewrite_manifests_merges_data_manifests(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=3)
    assert len(_data_manifests(table)) == 3
    rows_before = table.scan().to_arrow().sort_by("id")

    table.maintenance.rewrite_manifests().commit()

    table = catalog.load_table("default.test_rewrite")
    manifests = _data_manifests(table)
    assert len(manifests) == 1
    # entries are rewritten as EXISTING
    assert manifests[0].existing_files_count == 3
    assert manifests[0].added_files_count == 0

    # data is unchanged
    assert table.scan().to_arrow().sort_by("id") == rows_before

    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary.operation == Operation.REPLACE
    assert snapshot.summary["manifests-created"] == "1"
    assert snapshot.summary["manifests-replaced"] == "3"
    assert snapshot.summary["entries-processed"] == "3"
    # totals carry over unchanged
    assert snapshot.summary["total-data-files"] == "3"
    assert snapshot.summary["total-records"] == "9"


def _sequence_numbers_by_file(table: Table) -> dict[str, int]:
    result: dict[str, int] = {}
    for manifest in _data_manifests(table):
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
            assert entry.sequence_number is not None
            result[entry.data_file.file_path] = entry.sequence_number
    return result


def test_rewrite_manifests_preserves_sequence_numbers(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=3)
    entries_before = _sequence_numbers_by_file(table)

    table.maintenance.rewrite_manifests().commit()

    table = catalog.load_table("default.test_rewrite")
    entries_after = _sequence_numbers_by_file(table)
    assert entries_after == entries_before
    # the merged manifest keeps the min sequence number of its entries
    assert _data_manifests(table)[0].min_sequence_number == min(entries_before.values())


def test_rewrite_manifests_single_manifest_is_noop(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=1)
    snapshot_before = table.current_snapshot()
    assert snapshot_before is not None
    manifest_path_before = _data_manifests(table)[0].manifest_path

    table.maintenance.rewrite_manifests().commit()

    table = catalog.load_table("default.test_rewrite")
    # nothing to merge: no new snapshot is committed and the manifest is untouched
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.snapshot_id == snapshot_before.snapshot_id
    assert _data_manifests(table)[0].manifest_path == manifest_path_before


def test_rewrite_manifests_respects_target_size(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=4)
    max_manifest_length = max(m.manifest_length for m in _data_manifests(table))

    # allow two source manifests per group (2x fits, 3x exceeds), robust to small size variations
    with table.transaction() as tx:
        tx.set_properties({"commit.manifest.target-size-bytes": str(int(max_manifest_length * 2.5))})

    table = catalog.load_table("default.test_rewrite")
    table.maintenance.rewrite_manifests().commit()

    table = catalog.load_table("default.test_rewrite")
    manifests = _data_manifests(table)
    assert len(manifests) == 2
    assert all(m.existing_files_count == 2 for m in manifests)


def test_rewrites_needed(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=1)
    assert table.maintenance.rewrite_manifests().rewrites_needed() is False

    table.append(_arrow_table(offset=3))
    table = catalog.load_table("default.test_rewrite")
    assert table.maintenance.rewrite_manifests().rewrites_needed() is True


def test_rewrite_manifests_with_predicate_selective(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=3)
    manifests_before = _data_manifests(table)
    assert len(manifests_before) == 3

    # 1. Extract all manifest paths and underlying data file paths before rewrite
    paths_before = [m.manifest_path for m in manifests_before]
    target_path = paths_before[0]
    kept_paths_before = set(paths_before[1:])
    rows_before = table.scan().to_arrow().sort_by("id")
    data_files_before = [
        entry.data_file.file_path for m in manifests_before for entry in m.fetch_manifest_entry(table.io, discard_deleted=True)
    ]

    # 2. Execute selective rewrite
    table.maintenance.rewrite_manifests().rewrite_if(lambda m: m.manifest_path == target_path).commit()

    # 3. Reload table and extract new paths for comprehensive verification
    table = catalog.load_table("default.test_rewrite")
    manifests_after = _data_manifests(table)
    paths_after = {m.manifest_path for m in manifests_after}

    assert len(manifests_after) == 3
    # Manifest paths not rewritten remain unchanged
    assert kept_paths_before.issubset(paths_after)
    # Old path of rewritten target manifest is gone
    assert target_path not in paths_after
    # Exactly one new manifest was created
    new_manifest_paths = paths_after - kept_paths_before
    assert len(new_manifest_paths) == 1

    # Verify table data and underlying data files are completely preserved
    assert table.scan().to_arrow().sort_by("id") == rows_before
    data_files_after = [
        entry.data_file.file_path for m in manifests_after for entry in m.fetch_manifest_entry(table.io, discard_deleted=True)
    ]
    assert set(data_files_before) == set(data_files_after)

    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary["manifests-created"] == "1"
    assert snapshot.summary["manifests-replaced"] == "1"
    assert snapshot.summary["manifests-kept"] == "2"
    assert snapshot.summary["entries-processed"] == "1"


def test_rewrite_manifests_single_manifest_with_predicate(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=1)
    snapshot_before = table.current_snapshot()
    assert snapshot_before is not None
    manifest_path_before = _data_manifests(table)[0].manifest_path

    # with predicate, even single manifest should be rewritten
    table.maintenance.rewrite_manifests().rewrite_if(lambda m: True).commit()

    table = catalog.load_table("default.test_rewrite")
    manifests_after = _data_manifests(table)
    assert len(manifests_after) == 1
    assert manifests_after[0].manifest_path != manifest_path_before
    assert manifests_after[0].existing_files_count == 1
    assert manifests_after[0].added_files_count == 0

    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.snapshot_id != snapshot_before.snapshot_id
    assert snapshot.summary is not None
    assert snapshot.summary["manifests-created"] == "1"
    assert snapshot.summary["manifests-replaced"] == "1"
    assert snapshot.summary["manifests-kept"] == "0"
    assert snapshot.summary["entries-processed"] == "1"


def test_rewrites_needed_with_predicate(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=1)
    # single manifest without predicate: False
    assert table.maintenance.rewrite_manifests().rewrites_needed() is False
    # single manifest with matching predicate: True
    assert table.maintenance.rewrite_manifests().rewrite_if(lambda m: True).rewrites_needed() is True
    # single manifest with non-matching predicate: False
    assert table.maintenance.rewrite_manifests().rewrite_if(lambda m: False).rewrites_needed() is False


def test_rewrite_manifests_with_fully_deleted_manifest(catalog: Catalog) -> None:
    table = catalog.create_table("default.test_fully_deleted", schema=pa.schema([pa.field("id", pa.int64())]))
    table.append(_arrow_table(offset=0))  # manifest 1: id 1, 2, 3
    table.append(_arrow_table(offset=3))  # manifest 2: id 4, 5, 6
    table.delete("id <= 3")  # deletes id 1, 2, 3

    snapshot_before = table.current_snapshot()
    manifests_before = _data_manifests(table)
    assert len(manifests_before) == 2
    paths_before = [m.manifest_path for m in manifests_before]

    # Target rewrite for manifest containing only deleted entries
    table.maintenance.rewrite_manifests().rewrite_if(lambda m: (m.deleted_files_count or 0) > 0).commit()

    table = catalog.load_table("default.test_fully_deleted")
    assert table.current_snapshot() == snapshot_before

    # Verify both manifest paths remain unchanged
    manifests_after = _data_manifests(table)
    assert len(manifests_after) == 2
    assert [m.manifest_path for m in manifests_after] == paths_before

    # Verify the second manifest with live data (ids 4, 5, 6) is intact and readable
    assert table.scan().to_arrow().sort_by("id") == _arrow_table(offset=3)
    live_entries_manifest2 = manifests_after[1].fetch_manifest_entry(table.io, discard_deleted=True)
    assert len(live_entries_manifest2) == 1
    assert live_entries_manifest2[0].data_file.record_count == 3


def test_rewrite_manifests_predicate_matching_nothing(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=2)
    snapshot_before = table.current_snapshot()
    table.maintenance.rewrite_manifests().rewrite_if(lambda m: False).commit()
    table = catalog.load_table("default.test_rewrite")
    assert table.current_snapshot() == snapshot_before


def test_rewrite_manifests_merges_live_and_fully_deleted_manifests(catalog: Catalog) -> None:
    table = catalog.create_table("default.test_merge_deleted", schema=pa.schema([pa.field("id", pa.int64())]))
    table.append(_arrow_table(offset=0))  # manifest 1: id 1, 2, 3
    table.append(_arrow_table(offset=3))  # manifest 2: id 4, 5, 6
    table.delete("id <= 3")  # fully deletes manifest 1

    manifests_before = _data_manifests(table)
    assert len(manifests_before) == 2

    # Plain rewrite_manifests() without predicate should merge live and fully-deleted manifests into 1
    assert table.maintenance.rewrite_manifests().rewrites_needed() is True
    table.maintenance.rewrite_manifests().commit()

    table = catalog.load_table("default.test_merge_deleted")
    manifests_after = _data_manifests(table)
    assert len(manifests_after) == 1
    assert manifests_after[0].existing_files_count == 1
    assert manifests_after[0].added_files_count == 0

    # Verify table data is fully preserved and matches remaining live records
    assert table.scan().to_arrow().sort_by("id") == _arrow_table(offset=3)


def test_rewrites_needed_is_false_when_every_group_is_a_single_manifest(catalog: Catalog) -> None:
    table = _create_table_with_appends(catalog, appends=3)
    # A tiny target size puts every manifest in its own group, so nothing can be merged
    with table.transaction() as tx:
        tx.set_properties({"commit.manifest.target-size-bytes": "1"})

    table = catalog.load_table("default.test_rewrite")
    assert len(_data_manifests(table)) == 3

    snapshot_before = table.current_snapshot()
    assert table.maintenance.rewrite_manifests().rewrites_needed() is False

    table.maintenance.rewrite_manifests().commit()

    table = catalog.load_table("default.test_rewrite")
    assert table.current_snapshot() == snapshot_before
    assert len(_data_manifests(table)) == 3
