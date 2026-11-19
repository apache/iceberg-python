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
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock
from urllib.parse import urlparse
from urllib.request import url2pathname
from uuid import uuid4

import pyarrow as pa
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException
from pyiceberg.io import FileIO, InputFile, OutputFile
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriterV2,
)
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table, Transaction
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.statistics import PartitionStatisticsFile, StatisticsFile
from pyiceberg.table.update import RemoveSnapshotsUpdate, SetPartitionStatisticsUpdate, update_table_metadata
from pyiceberg.table.update.snapshot import ExpireSnapshots
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField


def test_cannot_expire_protected_head_snapshot(table_v2: Table) -> None:
    """Test that a HEAD (branch) snapshot cannot be expired."""
    HEAD_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    # Mock the catalog's commit_table method
    table_v2.catalog = MagicMock()
    # Simulate refs protecting HEAD_SNAPSHOT as a branch
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=HEAD_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            }
        }
    )
    # Assert fixture data
    assert any(ref.snapshot_id == HEAD_SNAPSHOT for ref in table_v2.metadata.refs.values())

    # Attempt to expire the HEAD snapshot and expect a ValueError
    with pytest.raises(ValueError, match=f"Snapshot with ID {HEAD_SNAPSHOT} is protected and cannot be expired."):
        table_v2.maintenance.expire_snapshots().by_id(HEAD_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_cannot_expire_tagged_snapshot(table_v2: Table) -> None:
    """Test that a tagged snapshot cannot be expired."""
    TAGGED_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    table_v2.catalog = MagicMock()
    # Simulate refs protecting TAGGED_SNAPSHOT as a tag
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "tag1": MagicMock(snapshot_id=TAGGED_SNAPSHOT, snapshot_ref_type="tag"),
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
            }
        }
    )
    assert any(ref.snapshot_id == TAGGED_SNAPSHOT for ref in table_v2.metadata.refs.values())

    with pytest.raises(ValueError, match=f"Snapshot with ID {TAGGED_SNAPSHOT} is protected and cannot be expired."):
        table_v2.maintenance.expire_snapshots().by_id(TAGGED_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_expire_unprotected_snapshot(table_v2: Table) -> None:
    """Test that an unprotected snapshot can be expired."""
    EXPIRE_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = mock_response

    # Remove any refs that protect the snapshot to be expired
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            }
        }
    )

    # Assert fixture data
    assert all(ref.snapshot_id != EXPIRE_SNAPSHOT for ref in table_v2.metadata.refs.values())

    # Expire the snapshot
    table_v2.maintenance.expire_snapshots().by_id(EXPIRE_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_called_once()
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT not in remaining_snapshots
    assert len(table_v2.metadata.snapshots) == 1


def test_expire_nonexistent_snapshot_raises(table_v2: Table) -> None:
    """Test that trying to expire a non-existent snapshot raises an error."""
    NONEXISTENT_SNAPSHOT = 9999999999999999999

    table_v2.catalog = MagicMock()
    table_v2.metadata = table_v2.metadata.model_copy(update={"refs": {}})

    with pytest.raises(ValueError, match=f"Snapshot with ID {NONEXISTENT_SNAPSHOT} does not exist."):
        table_v2.maintenance.expire_snapshots().by_id(NONEXISTENT_SNAPSHOT).commit()

    table_v2.catalog.commit_table.assert_not_called()


def test_expire_snapshots_by_timestamp_skips_protected(table_v2: Table) -> None:
    # Setup: two snapshots; both are old, but one is head/tag protected
    HEAD_SNAPSHOT = 3051729675574597004
    TAGGED_SNAPSHOT = 3055729675574597004

    # Add snapshots to metadata for timestamp/protected test
    from types import SimpleNamespace

    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=HEAD_SNAPSHOT, snapshot_ref_type="branch"),
                "mytag": MagicMock(snapshot_id=TAGGED_SNAPSHOT, snapshot_ref_type="tag"),
            },
            "snapshots": [
                SimpleNamespace(snapshot_id=HEAD_SNAPSHOT, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=TAGGED_SNAPSHOT, timestamp_ms=1, parent_snapshot_id=None),
            ],
        }
    )
    table_v2.catalog = MagicMock()

    # Attempt to expire all snapshots before a future timestamp (so both are candidates)
    future_datetime = datetime.now() + timedelta(days=1)

    # Mock the catalog's commit_table to return the current metadata (simulate no change)
    mock_response = CommitTableResponse(
        metadata=table_v2.metadata,  # protected snapshots remain
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog.commit_table.return_value = mock_response

    table_v2.maintenance.expire_snapshots().older_than(future_datetime).commit()
    # Update metadata to reflect the commit (as in other tests)
    table_v2.metadata = mock_response.metadata

    # Both protected snapshots should remain
    remaining_ids = {s.snapshot_id for s in table_v2.metadata.snapshots}
    assert HEAD_SNAPSHOT in remaining_ids
    assert TAGGED_SNAPSHOT in remaining_ids

    # No snapshots should have been expired (commit_table called, but with empty snapshot_ids)
    args, kwargs = table_v2.catalog.commit_table.call_args
    updates = args[2] if len(args) > 2 else ()
    # Find RemoveSnapshotsUpdate in updates
    remove_update = next((u for u in updates if getattr(u, "action", None) == "remove-snapshots"), None)
    assert remove_update is not None
    assert remove_update.snapshot_ids == []


def test_expire_snapshots_by_ids(table_v2: Table) -> None:
    """Test that multiple unprotected snapshots can be expired by IDs."""
    EXPIRE_SNAPSHOT_1 = 3051729675574597004
    EXPIRE_SNAPSHOT_2 = 3051729675574597005
    KEEP_SNAPSHOT = 3055729675574597004

    mock_response = CommitTableResponse(
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = mock_response

    # Remove any refs that protect the snapshots to be expired
    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            }
        }
    )

    # Add snapshots to metadata for multi-id test
    from types import SimpleNamespace

    table_v2.metadata = table_v2.metadata.model_copy(
        update={
            "refs": {
                "main": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="branch"),
                "tag1": MagicMock(snapshot_id=KEEP_SNAPSHOT, snapshot_ref_type="tag"),
            },
            "snapshots": [
                SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT_1, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=EXPIRE_SNAPSHOT_2, timestamp_ms=1, parent_snapshot_id=None),
                SimpleNamespace(snapshot_id=KEEP_SNAPSHOT, timestamp_ms=2, parent_snapshot_id=None),
            ],
        }
    )

    # Assert fixture data
    assert all(ref.snapshot_id not in (EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2) for ref in table_v2.metadata.refs.values())

    # Expire the snapshots
    table_v2.maintenance.expire_snapshots().by_ids([EXPIRE_SNAPSHOT_1, EXPIRE_SNAPSHOT_2]).commit()

    table_v2.catalog.commit_table.assert_called_once()
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT_1 not in remaining_snapshots
    assert EXPIRE_SNAPSHOT_2 not in remaining_snapshots
    assert len(table_v2.metadata.snapshots) == 1


def test_thread_safety_fix() -> None:
    """Test that ExpireSnapshots instances have isolated state."""
    # Create two ExpireSnapshots instances
    expire1 = ExpireSnapshots(Mock())
    expire2 = ExpireSnapshots(Mock())

    # Verify they have separate snapshot sets (this was the bug!)
    # Before fix: both would have the same id (shared class attribute)
    # After fix: they should have different ids (separate instance attributes)
    assert id(expire1._snapshot_ids_to_expire) != id(expire2._snapshot_ids_to_expire), (
        "ExpireSnapshots instances are sharing the same snapshot set - thread safety bug still exists"
    )

    # Test that modifications to one don't affect the other
    expire1._snapshot_ids_to_expire.add(1001)
    expire2._snapshot_ids_to_expire.add(2001)

    # Verify no cross-contamination of snapshot IDs
    assert 2001 not in expire1._snapshot_ids_to_expire, "Snapshot IDs are leaking between instances"
    assert 1001 not in expire2._snapshot_ids_to_expire, "Snapshot IDs are leaking between instances"


def test_concurrent_operations() -> None:
    """Test concurrent operations with separate ExpireSnapshots instances."""
    results: dict[str, set[int]] = {"expire1_snapshots": set(), "expire2_snapshots": set()}

    def worker1() -> None:
        expire1 = ExpireSnapshots(Mock())
        expire1._snapshot_ids_to_expire.update([1001, 1002, 1003])
        results["expire1_snapshots"] = expire1._snapshot_ids_to_expire.copy()

    def worker2() -> None:
        expire2 = ExpireSnapshots(Mock())
        expire2._snapshot_ids_to_expire.update([2001, 2002, 2003])
        results["expire2_snapshots"] = expire2._snapshot_ids_to_expire.copy()

    # Run both workers concurrently
    thread1 = threading.Thread(target=worker1)
    thread2 = threading.Thread(target=worker2)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    # Check for cross-contamination
    expected_1 = {1001, 1002, 1003}
    expected_2 = {2001, 2002, 2003}

    assert results["expire1_snapshots"] == expected_1, "Worker 1 snapshots contaminated"
    assert results["expire2_snapshots"] == expected_2, "Worker 2 snapshots contaminated"


def test_update_remove_snapshots_with_statistics(table_v2_with_statistics: Table) -> None:
    """
    Test removing snapshots from a table that has statistics.

    This test exercises the code path where RemoveStatisticsUpdate is instantiated
    within the RemoveSnapshotsUpdate handler. Before the fix for #2558, this would
    fail with: TypeError: BaseModel.__init__() takes 1 positional argument but 2 were given
    """
    # The table has 2 snapshots with IDs: 3051729675574597004 and 3055729675574597004
    # Both snapshots have statistics associated with them
    REMOVE_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004

    # Verify fixture assumptions
    assert len(table_v2_with_statistics.metadata.snapshots) == 2
    assert len(table_v2_with_statistics.metadata.statistics) == 2
    assert any(stat.snapshot_id == REMOVE_SNAPSHOT for stat in table_v2_with_statistics.metadata.statistics), (
        "Snapshot to remove should have statistics"
    )

    # This should trigger RemoveStatisticsUpdate instantiation for the removed snapshot
    update = RemoveSnapshotsUpdate(snapshot_ids=[REMOVE_SNAPSHOT])
    new_metadata = update_table_metadata(table_v2_with_statistics.metadata, (update,))

    # Verify the snapshot was removed
    assert len(new_metadata.snapshots) == 1
    assert new_metadata.snapshots[0].snapshot_id == KEEP_SNAPSHOT

    # Verify the statistics for the removed snapshot were also removed
    assert len(new_metadata.statistics) == 1
    assert new_metadata.statistics[0].snapshot_id == KEEP_SNAPSHOT
    assert not any(stat.snapshot_id == REMOVE_SNAPSHOT for stat in new_metadata.statistics), (
        "Statistics for removed snapshot should be gone"
    )


def _expiration_table(path: Path, appends_only: bool = False) -> Table:
    catalog = InMemoryCatalog("expiration", warehouse=path.as_uri(), **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
    catalog.create_namespace("default")
    table = catalog.create_table("default.expiration", schema=Schema(NestedField(1, "x", LongType(), required=False)))
    table.append(pa.table({"x": [1]}))
    table.append(pa.table({"x": [2]}))
    if appends_only:
        table.append(pa.table({"x": [3]}))
    else:
        table.overwrite(pa.table({"x": [3]}))
    return table


@pytest.fixture
def local_expiration_table(tmp_path: Path) -> Table:
    return _expiration_table(tmp_path)


def _exists(location: str) -> bool:
    return Path(url2pathname(urlparse(location).path)).exists()


def _snapshot_files(table: Table, snapshots: list[Snapshot]) -> set[str]:
    paths: set[str] = set()
    for snapshot in snapshots:
        paths.add(snapshot.manifest_list)
        for manifest in snapshot.manifests(table.io):
            paths.add(manifest.manifest_path)
            paths.update(entry.data_file.file_path for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False))
    return paths


def _ids(snapshots: list[Snapshot]) -> list[int]:
    return [snapshot.snapshot_id for snapshot in snapshots]


def _forbid_manifest_list_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    # pytest.fail, not an assertion: the cleanup logs and swallows any Exception.
    monkeypatch.setattr(Snapshot, "manifests", lambda self, io: pytest.fail("unexpected manifest list read"))


def _record_manifest_reads(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, bool]]:
    original_fetch = ManifestFile.fetch_manifest_entry
    reads: list[tuple[str, bool]] = []

    def record_read(self: ManifestFile, io: FileIO, discard_deleted: bool = True, **kwargs: Any) -> list[ManifestEntry]:
        reads.append((self.manifest_path, discard_deleted))
        return original_fetch(self, io, discard_deleted=discard_deleted, **kwargs)

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", record_read)
    return reads


def _run_after_next_load(table: Table, monkeypatch: pytest.MonkeyPatch, change: Any) -> None:
    """Run `change` as another writer right after the expiration reloads the table metadata."""
    original_load_table = table.catalog.load_table
    calls: list[Any] = []

    def load_then_change(identifier: Any) -> Table:
        loaded = original_load_table(identifier)
        if not calls:
            calls.append(identifier)
            change(original_load_table(identifier))
        return loaded

    monkeypatch.setattr(table.catalog, "load_table", load_then_change)


def test_expiration_preserves_files_by_default(local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch) -> None:
    table = local_expiration_table
    first, second, *_ = table.snapshots()
    paths = _snapshot_files(table, [first, second])
    _forbid_manifest_list_reads(monkeypatch)

    table.maintenance.expire_snapshots().by_id(first.snapshot_id).commit()
    table.maintenance.expire_snapshots().clean_expired_files().clean_expired_files(enabled=False).by_id(
        second.snapshot_id
    ).commit()

    assert len(table.snapshots()) == 2
    assert all(_exists(path) for path in paths)


def test_expiration_cleans_unreferenced_files(
    local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    table = local_expiration_table
    *expired, current = table.snapshots()
    paths = _snapshot_files(table, expired) - _snapshot_files(table, [current])
    metadata_location = table.metadata_location
    original_delete = table.io.delete
    attempted: list[str] = []

    def fail_first_delete(location: str | InputFile | OutputFile) -> None:
        attempted.append(location if isinstance(location, str) else location.location)
        if len(attempted) == 1:
            raise OSError("injected deletion failure")
        original_delete(location)

    monkeypatch.setattr(table.io, "delete", fail_first_delete)
    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert _ids(table.snapshots()) == [current.snapshot_id]
    assert sorted(attempted) == sorted(paths)
    assert [path for path in paths if _exists(path)] == [attempted[0]]
    assert attempted[0] in caplog.text
    assert _exists(metadata_location)
    assert table.scan().to_arrow().to_pydict() == {"x": [3]}


def test_expiration_keeps_files_of_tagged_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The manifest shared with the tagged snapshot is read once, and only for its live entries.
    table = _expiration_table(tmp_path)
    first, tagged, deleted, _ = table.snapshots()
    table.manage_snapshots().create_tag(tagged.snapshot_id, "tagged").commit()
    shared = _snapshot_files(table, [first]) - {first.manifest_list}
    shared_manifests = {manifest.manifest_path for manifest in first.manifests(table.io)}
    reads = _record_manifest_reads(monkeypatch)

    table.maintenance.expire_snapshots().clean_expired_files().by_ids([first.snapshot_id, deleted.snapshot_id]).commit()

    assert not _exists(first.manifest_list)
    assert all(_exists(path) for path in shared)
    assert len(reads) == len(set(reads))
    assert all(discard_deleted for path, discard_deleted in reads if path in shared_manifests)
    assert sorted(table.scan(snapshot_id=tagged.snapshot_id).to_arrow().column("x").to_pylist()) == [1, 2]


def test_expiration_cleans_files_from_deleted_entries(local_expiration_table: Table) -> None:
    table = local_expiration_table
    first, second, deleted, _ = table.snapshots()
    entries = [entry for manifest in deleted.manifests(table.io) for entry in manifest.fetch_manifest_entry(table.io, False)]
    assert entries and all(entry.status == ManifestEntryStatus.DELETED for entry in entries)
    table.maintenance.expire_snapshots().by_ids([first.snapshot_id, second.snapshot_id]).commit()

    table.maintenance.expire_snapshots().clean_expired_files().by_id(deleted.snapshot_id).commit()

    assert not any(_exists(entry.data_file.file_path) for entry in entries)


def test_expiration_on_append_only_table_reads_no_manifests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    table = _expiration_table(tmp_path, appends_only=True)
    *expired, current = table.snapshots()
    reads = _record_manifest_reads(monkeypatch)

    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert reads == []
    assert not any(_exists(snapshot.manifest_list) for snapshot in expired)
    assert sorted(table.scan().to_arrow().column("x").to_pylist()) == [1, 2, 3]


class _DeleteManifestWriter(ManifestWriterV2):
    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


def _delete_file(table: Table, name: str) -> DataFile:
    # The cleanup only looks at manifest entries, so the file body does not need to be a valid delete file.
    path = f"{table.location()}/data/{name}"
    with table.io.new_output(path).create() as stream:
        stream.write(b"delete file")
    return DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=11,
    )


def _commit_delete_manifest(table: Table, added: DataFile, replaced: ManifestFile | None = None) -> ManifestFile:
    """Commit a snapshot with a delete manifest that adds `added` and marks the entries of `replaced` as deleted."""
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        writer = _DeleteManifestWriter(
            table.spec(), table.schema(), append.new_manifest_output(), append._snapshot_id, append._compression
        )
        with writer:
            writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, data_file=added))
            if replaced is not None:
                for entry in replaced.fetch_manifest_entry(table.io):
                    writer.delete(entry)
        manifest = writer.to_manifest_file()
        kept = [existing for existing in append._existing_manifests() if existing != replaced] + [manifest]
        append._existing_manifests = lambda: kept  # type: ignore[method-assign]
    return manifest


def test_expiration_cleans_delete_files(local_expiration_table: Table) -> None:
    table = local_expiration_table
    expired_deletes = _delete_file(table, "expired-deletes.parquet")
    retained_deletes = _delete_file(table, "retained-deletes.parquet")
    expired_manifest = _commit_delete_manifest(table, expired_deletes)
    retained_manifest = _commit_delete_manifest(table, retained_deletes, replaced=expired_manifest)
    *expired, current = table.snapshots()

    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert not _exists(expired_deletes.file_path)
    assert not _exists(expired_manifest.manifest_path)
    assert _exists(retained_deletes.file_path)
    assert _exists(retained_manifest.manifest_path)


def _add_statistics(table: Table, snapshot: Snapshot, path: str, partition: bool = False) -> None:
    if not _exists(path):
        with table.io.new_output(path).create() as stream:
            stream.write(b"statistics")
    if partition:
        statistics = PartitionStatisticsFile(snapshot_id=snapshot.snapshot_id, statistics_path=path, file_size_in_bytes=10)
        with table.transaction() as transaction:
            transaction._apply((SetPartitionStatisticsUpdate(partition_statistics=statistics),))
    else:
        statistics_file = StatisticsFile(
            snapshot_id=snapshot.snapshot_id,
            statistics_path=path,
            file_size_in_bytes=10,
            file_footer_size_in_bytes=4,
            blob_metadata=[],
        )
        table.update_statistics().set_statistics(statistics_file).commit()


def test_expiration_cleans_statistics_files(local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch) -> None:
    # A statistics file of an expired snapshot is kept while the table metadata after the commit still lists its path.
    table = local_expiration_table
    first, second, deleted, current = table.snapshots()
    expired, expired_partition, shared, shared_with_partition = (
        f"{table.location()}/metadata/{name}.stats" for name in ("a", "b", "c", "d")
    )
    _add_statistics(table, first, expired)
    _add_statistics(table, first, expired_partition, partition=True)
    _add_statistics(table, second, shared)
    _add_statistics(table, current, shared)
    _add_statistics(table, deleted, shared_with_partition)
    _add_statistics(table, current, shared_with_partition, partition=True)
    original_commit_table = table.catalog.commit_table

    def commit_dropping_partition_statistics(*args: Any, **kwargs: Any) -> CommitTableResponse:
        # Like a REST catalog that also drops the partition statistics of removed snapshots.
        response = original_commit_table(*args, **kwargs)
        snapshot_ids = set(_ids(response.metadata.snapshots))
        partition_statistics = [s for s in response.metadata.partition_statistics if s.snapshot_id in snapshot_ids]
        metadata = response.metadata.model_copy(update={"partition_statistics": partition_statistics})
        return CommitTableResponse(metadata=metadata, metadata_location=response.metadata_location)

    monkeypatch.setattr(table.catalog, "commit_table", commit_dropping_partition_statistics)
    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids([first, second, deleted])).commit()

    assert not _exists(expired)
    assert not _exists(expired_partition)
    assert _exists(shared)
    assert _exists(shared_with_partition)


@pytest.mark.parametrize("error_type", [CommitFailedException, CommitStateUnknownException])
def test_expiration_does_not_delete_when_commit_fails(
    local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    table = local_expiration_table
    expired = table.snapshots()[:-1]
    paths = _snapshot_files(table, expired)
    monkeypatch.setattr("pyiceberg.table.Transaction.commit_transaction", Mock(side_effect=error_type("injected commit failure")))

    with pytest.raises(error_type, match="injected commit failure"):
        table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert all(_exists(path) for path in paths)


def test_expiration_stays_committed_when_retained_manifest_is_corrupt(
    local_expiration_table: Table, caplog: pytest.LogCaptureFixture
) -> None:
    table = local_expiration_table
    *expired, current = table.snapshots()
    paths = _snapshot_files(table, expired)
    (retained_manifest,) = current.manifests(table.io)
    with table.io.new_output(retained_manifest.manifest_path).create(overwrite=True) as stream:
        stream.write(b"truncated")

    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert _ids(table.catalog.load_table(table.name()).snapshots()) == [current.snapshot_id]
    assert all(_exists(path) for path in paths)
    assert "Failed to clean up expired snapshot files" in caplog.text


def test_expiration_cleanup_requires_autocommit(local_expiration_table: Table) -> None:
    with pytest.raises(ValueError, match="autocommit"):
        ExpireSnapshots(Transaction(local_expiration_table)).clean_expired_files().commit()


def test_expiration_cleanup_with_no_selected_snapshots(local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch) -> None:
    _forbid_manifest_list_reads(monkeypatch)
    local_expiration_table.maintenance.expire_snapshots().clean_expired_files().commit()
    assert len(local_expiration_table.snapshots()) == 4


def test_expiration_respects_gc_enabled_in_latest_metadata(local_expiration_table: Table) -> None:
    table = local_expiration_table
    *expired, current = table.snapshots()
    paths = _snapshot_files(table, table.snapshots())
    with table.catalog.load_table(table.name()).transaction() as transaction:
        transaction.set_properties({"gc.enabled": "false"})

    with pytest.raises(ValueError, match="gc.enabled is false"):
        table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()
    assert len(table.catalog.load_table(table.name()).snapshots()) == 4

    table.maintenance.expire_snapshots().by_ids(_ids(expired)).commit()
    assert _ids(table.snapshots()) == [current.snapshot_id]
    assert all(_exists(path) for path in paths)


@pytest.mark.parametrize("change", ["tag", "rollback", "tag_then_refresh"])
def test_expiration_cleanup_rejects_snapshot_referenced_in_latest_metadata(local_expiration_table: Table, change: str) -> None:
    # Without the check, the catalog drops a ref that another writer put on the snapshot, and its files are deleted.
    table = local_expiration_table
    first = table.snapshots()[0]
    paths = _snapshot_files(table, table.snapshots())
    expiration = table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(table.snapshots()[:-1]))
    other_writer = table.catalog.load_table(table.name())
    if change == "rollback":
        other_writer.manage_snapshots().rollback_to_snapshot(first.snapshot_id).commit()
    else:
        other_writer.manage_snapshots().create_tag(first.snapshot_id, "keep").commit()
    if change == "tag_then_refresh":
        table.refresh()

    with pytest.raises(ValueError, match=rf"references in the latest table metadata: \[{first.snapshot_id}\]"):
        expiration.commit()

    assert len(table.catalog.load_table(table.name()).snapshots()) == 4
    assert all(_exists(path) for path in paths)


def test_expiration_cleanup_fails_when_ref_moves_after_reload(
    local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = local_expiration_table
    first, retained, _, current = table.snapshots()
    table.manage_snapshots().create_tag(current.snapshot_id, "other").create_tag(retained.snapshot_id, "keep").commit()
    paths = _snapshot_files(table, table.snapshots())

    def move_tag(other_writer: Table) -> None:
        other_writer.manage_snapshots().remove_tag("keep").commit()
        other_writer.manage_snapshots().create_tag(first.snapshot_id, "keep").commit()

    _run_after_next_load(table, monkeypatch, move_tag)
    with pytest.raises(CommitFailedException, match="keep has changed"):
        table.maintenance.expire_snapshots().clean_expired_files().by_id(first.snapshot_id).commit()

    assert len(table.catalog.load_table(table.name()).snapshots()) == 4
    assert all(_exists(path) for path in paths)


def test_expiration_cleanup_allows_concurrent_append_to_main(
    local_expiration_table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = local_expiration_table
    expired = table.snapshots()[:-1]
    _run_after_next_load(table, monkeypatch, lambda other_writer: other_writer.append(pa.table({"x": [4]})))

    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert len(table.snapshots()) == 2
    assert sorted(table.scan().to_arrow().column("x").to_pylist()) == [3, 4]


def test_expiration_keeps_files_of_concurrently_committed_snapshots(local_expiration_table: Table) -> None:
    # The files to keep come from the metadata the commit returned, including snapshots and refs other writers committed.
    table = local_expiration_table
    *expired, _ = table.snapshots()
    (entry,) = [entry for manifest in expired[0].manifests(table.io) for entry in manifest.fetch_manifest_entry(table.io)]
    with (
        table.catalog.load_table(table.name()).transaction() as transaction,
        transaction.update_snapshot().fast_append() as append,
    ):
        append.append_data_file(entry.data_file)
    other_writer = table.catalog.load_table(table.name())
    other_writer.manage_snapshots().create_tag(other_writer.snapshots()[-1].snapshot_id, "other").commit()

    table.maintenance.expire_snapshots().clean_expired_files().by_ids(_ids(expired)).commit()

    assert _exists(entry.data_file.file_path)
    assert sorted(table.scan().to_arrow().column("x").to_pylist()) == [1, 3]
