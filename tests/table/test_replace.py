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
from typing import cast

import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import ValidationException
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.typedef import Record


def _create_dummy_data_file(
    file_path: str,
    record_count: int,
    file_size_in_bytes: int = 1024,
    content: DataFileContent = DataFileContent.DATA,
    partition: Record | None = None,
    spec_id: int = 0,
) -> DataFile:
    if partition is None:
        partition = Record()
    df = DataFile.from_args(
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=record_count,
        file_size_in_bytes=file_size_in_bytes,
        content=content,
    )
    df.spec_id = spec_id
    return df


def test_replace_internally(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace",
        schema=Schema(),
    )

    file_to_delete = _create_dummy_data_file(
        file_path="s3://bucket/test/data/deleted.parquet",
        record_count=100,
    )

    file_to_keep = _create_dummy_data_file(
        file_path="s3://bucket/test/data/kept.parquet",
        record_count=50,
        file_size_in_bytes=512,
    )

    file_to_add = _create_dummy_data_file(
        file_path="s3://bucket/test/data/added.parquet",
        record_count=100,
    )

    # Initially append both files
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)
            append_snapshot.append_data_file(file_to_keep)

    old_snapshot = cast(Snapshot, table.current_snapshot())
    old_snapshot_id = old_snapshot.snapshot_id
    old_sequence_number = cast(int, old_snapshot.sequence_number)

    # Call the replace API
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_to_delete)
            rewrite.append_data_file(file_to_add)

    snapshot = cast(Snapshot, table.current_snapshot())
    summary = cast(Summary, snapshot.summary)

    assert snapshot.snapshot_id is not None
    assert snapshot.snapshot_id != old_snapshot_id
    assert snapshot.parent_snapshot_id == old_snapshot_id
    assert snapshot.sequence_number == old_sequence_number + 1
    assert summary["operation"] == Operation.REPLACE
    assert snapshot.manifest_list is not None
    assert isinstance(snapshot.manifest_list, str)

    # Summary counts
    assert summary["added-data-files"] == "1"
    assert summary["deleted-data-files"] == "1"
    assert summary["added-records"] == "100"
    assert summary["deleted-records"] == "100"
    assert summary["total-records"] == "150"

    # Fetch all entries from the new manifests
    manifest_files = snapshot.manifests(table.io)
    entries: list[ManifestEntry] = []
    for manifest in manifest_files:
        entries.extend(manifest.fetch_manifest_entry(table.io, discard_deleted=False))

    assert len(entries) == 3

    added_entries = [e for e in entries if e.status == ManifestEntryStatus.ADDED]
    assert len(added_entries) == 1
    assert added_entries[0].data_file.file_path == file_to_add.file_path
    assert added_entries[0].snapshot_id == snapshot.snapshot_id

    deleted_entries = [e for e in entries if e.status == ManifestEntryStatus.DELETED]
    assert len(deleted_entries) == 1
    assert deleted_entries[0].data_file.file_path == file_to_delete.file_path
    assert deleted_entries[0].snapshot_id == snapshot.snapshot_id

    existing_entries = [e for e in entries if e.status == ManifestEntryStatus.EXISTING]
    assert len(existing_entries) == 1
    assert existing_entries[0].data_file.file_path == file_to_keep.file_path
    assert existing_entries[0].snapshot_id == old_snapshot_id
    assert existing_entries[0].sequence_number == old_sequence_number


def test_replace_reuses_unaffected_manifests(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_reuse_manifest",
        schema=Schema(),
    )

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=10,
        file_size_in_bytes=100,
    )

    file_b = _create_dummy_data_file(
        file_path="s3://bucket/test/data/b.parquet",
        record_count=10,
        file_size_in_bytes=100,
    )

    file_c = _create_dummy_data_file(
        file_path="s3://bucket/test/data/c.parquet",
        record_count=10,
        file_size_in_bytes=100,
    )

    # Commit 1: Append file A (Creates Manifest 1)
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    # Commit 2: Append file B (Creates Manifest 2)
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_b)

    snapshot_before = cast(Snapshot, table.current_snapshot())
    manifests_before = snapshot_before.manifests(table.io)
    assert len(manifests_before) == 2

    # Identify which manifest belongs to file_b and file_a
    manifest_b_path = None
    manifest_a_path = None
    for m in manifests_before:
        entries = m.fetch_manifest_entry(table.io, discard_deleted=False)
        if any(e.data_file.file_path == file_b.file_path for e in entries):
            manifest_b_path = m.manifest_path
        if any(e.data_file.file_path == file_a.file_path for e in entries):
            manifest_a_path = m.manifest_path

    assert manifest_b_path is not None
    assert manifest_a_path is not None

    # Commit 3: Replace file A with file C
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_a)
            rewrite.append_data_file(file_c)

    snapshot_after = cast(Snapshot, table.current_snapshot())
    assert snapshot_after is not None
    manifests_after = snapshot_after.manifests(table.io)

    # We expect 3 manifests:
    # 1. The reused one for file B
    # 2. The newly rewritten one marking file A as DELETED
    # 3. The new one for file C (ADDED)
    assert len(manifests_after) == 3

    manifest_paths_after = [m.manifest_path for m in manifests_after]

    # ASSERTION 1: The untouched manifest is reused (path matches exactly)
    assert manifest_b_path in manifest_paths_after

    # ASSERTION 2: File A's manifest was rewritten
    assert manifest_a_path not in manifest_paths_after


def test_replace_empty_files(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_empty",
        schema=Schema(),
    )

    # No-op replace should not produce a snapshot
    with table.transaction() as tx:
        with tx.update_snapshot().replace():
            pass

    assert len(table.history()) == 0
    assert table.current_snapshot() is None


def test_replace_missing_file_abort(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_missing",
        schema=Schema(),
    )

    fake_data_file = _create_dummy_data_file(
        file_path="s3://bucket/test/data/does_not_exist.parquet",
        record_count=100,
    )

    new_data_file = _create_dummy_data_file(
        file_path="s3://bucket/test/data/new.parquet",
        record_count=100,
    )

    # Ensure it aborts when trying to replace a file that isn't in the table
    with pytest.raises(ValidationException, match="missing data files to be rewritten"):
        with table.transaction() as tx:
            with tx.update_snapshot().replace() as rewrite:
                rewrite.delete_data_file(fake_data_file)
                rewrite.append_data_file(new_data_file)


def test_replace_invariant_violation(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_invariant",
        schema=Schema(),
    )

    file_to_delete = _create_dummy_data_file(
        file_path="s3://bucket/test/data/deleted.parquet",
        record_count=100,
    )

    # Create a new file with MORE records than the one we are deleting
    too_many_records_file = _create_dummy_data_file(
        file_path="s3://bucket/test/data/too_many.parquet",
        record_count=101,
    )

    # Initially append to have something to replace
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)

    # Ensure it enforces the invariant: records added <= records removed
    with pytest.raises(ValidationException, match=r"Invalid replace: records added \(101\) exceeds records removed \(100\)"):
        with table.transaction() as tx:
            with tx.update_snapshot().replace() as rewrite:
                rewrite.delete_data_file(file_to_delete)
                rewrite.append_data_file(too_many_records_file)


def test_replace_allows_shrinking_for_soft_deletes(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_shrink",
        schema=Schema(),
    )

    # Old data file has 100 records
    file_to_delete = _create_dummy_data_file(
        file_path="s3://bucket/test/data/deleted.parquet",
        record_count=100,
    )

    # New data file only has 90 records (simulating 10 records were soft-deleted)
    shrunk_file_to_add = _create_dummy_data_file(
        file_path="s3://bucket/test/data/shrunk.parquet",
        record_count=90,
        file_size_in_bytes=900,
    )

    # Initially append
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)

    # This should succeed without throwing an invariant violation
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_to_delete)
            rewrite.append_data_file(shrunk_file_to_add)

    snapshot = cast(Snapshot, table.current_snapshot())
    summary = cast(Summary, snapshot.summary)

    assert summary["operation"] == Operation.REPLACE
    assert summary["added-records"] == "90"
    assert summary["deleted-records"] == "100"


def test_replace_passes_through_delete_manifests(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_delete_manifests",
        schema=Schema(),
        properties={"format-version": "2"},
    )

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=10,
        file_size_in_bytes=100,
    )

    file_a_deletes = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a_deletes.parquet",
        record_count=2,
        file_size_in_bytes=50,
        content=DataFileContent.POSITION_DELETES,
    )

    file_b = _create_dummy_data_file(
        file_path="s3://bucket/test/data/b.parquet",
        record_count=10,
        file_size_in_bytes=100,
    )

    # Commit 1: Append the data file
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    # Commit 2: Append the delete file
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a_deletes)

    # Find the path of the delete manifest so we can verify it survives
    snapshot_before = cast(Snapshot, table.current_snapshot())
    manifests_before = snapshot_before.manifests(table.io)

    delete_manifest_path = None
    for m in manifests_before:
        entries = m.fetch_manifest_entry(table.io, discard_deleted=False)
        if any(e.data_file.file_path == file_a_deletes.file_path for e in entries):
            delete_manifest_path = m.manifest_path
            break

    assert delete_manifest_path is not None

    # Commit 3: Replace data file A with data file B
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_a)
            rewrite.append_data_file(file_b)

    # Verify the delete manifest was passed through unchanged
    snapshot_after = cast(Snapshot, table.current_snapshot())
    assert snapshot_after is not None
    manifests_after = snapshot_after.manifests(table.io)
    manifest_paths_after = [m.manifest_path for m in manifests_after]

    assert delete_manifest_path in manifest_paths_after


def test_replace_multiple_files(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_multiple",
        schema=Schema(),
    )

    file_1 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/1.parquet",
        record_count=100,
    )

    file_2 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/2.parquet",
        record_count=100,
    )

    file_1_new = _create_dummy_data_file(
        file_path="s3://bucket/test/data/1_new.parquet",
        record_count=50,
        file_size_in_bytes=512,
    )

    file_2_new = _create_dummy_data_file(
        file_path="s3://bucket/test/data/2_new.parquet",
        record_count=50,
        file_size_in_bytes=512,
    )

    # Append initial files
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_1)
            append_snapshot.append_data_file(file_2)

    # Replace both files with new ones
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_1)
            rewrite.delete_data_file(file_2)
            rewrite.append_data_file(file_1_new)
            rewrite.append_data_file(file_2_new)

    snapshot = cast(Snapshot, table.current_snapshot())
    summary = cast(Summary, snapshot.summary)

    assert summary["added-data-files"] == "2"
    assert summary["deleted-data-files"] == "2"
    assert summary["added-records"] == "100"
    assert summary["deleted-records"] == "200"
    assert summary["total-records"] == "100"


def test_replace_partitioned_table(catalog: Catalog) -> None:
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField, StringType

    # Setup a partitioned table
    catalog.create_namespace("default")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="id"))
    table = catalog.create_table(
        identifier="default.test_replace_partitioned",
        schema=schema,
        partition_spec=spec,
    )

    # File in partition id=1
    file_part1 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part1.parquet",
        partition=Record(1),
        record_count=100,
        spec_id=table.spec().spec_id,
    )

    # File in partition id=2
    file_part2 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part2.parquet",
        partition=Record(2),
        record_count=100,
        spec_id=table.spec().spec_id,
    )

    # Add initial files
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_part1)
            append_snapshot.append_data_file(file_part2)

    # Replace file in partition 1
    file_part1_new = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part1_new.parquet",
        partition=Record(1),
        record_count=50,
        file_size_in_bytes=512,
        spec_id=table.spec().spec_id,
    )

    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_part1)
            rewrite.append_data_file(file_part1_new)

    snapshot = cast(Snapshot, table.current_snapshot())
    summary = cast(Summary, snapshot.summary)

    assert summary["added-data-files"] == "1"
    assert summary["deleted-data-files"] == "1"
    assert summary["total-records"] == "150"


def test_replace_no_op_on_non_empty_table(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_noop_nonempty",
        schema=Schema(),
    )

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=10,
        file_size_in_bytes=100,
    )

    # Commit 1: Append file A
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    initial_snapshot = table.current_snapshot()
    assert initial_snapshot is not None

    # Perform a no-op replace
    with table.transaction() as tx:
        with tx.update_snapshot().replace():
            pass

    # Successive calls to current_snapshot() should yield the same snapshot
    assert table.current_snapshot() == initial_snapshot
    assert len(table.history()) == 1


def test_replace_on_custom_branch(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_branch",
        schema=Schema(),
    )

    file_to_delete = _create_dummy_data_file(
        file_path="s3://bucket/test/data/deleted.parquet",
        record_count=100,
    )

    file_to_add = _create_dummy_data_file(
        file_path="s3://bucket/test/data/added.parquet",
        record_count=100,
    )

    # Initially append to have something to replace on main
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)

    initial_main_snapshot = cast(Snapshot, table.current_snapshot())
    initial_main_snapshot_id = initial_main_snapshot.snapshot_id

    # Create a new branch called "test-branch" pointing to the initial snapshot
    table.manage_snapshots().create_branch(branch_name="test-branch", snapshot_id=initial_main_snapshot_id).commit()

    # Perform a replace() operation explicitly targeting "test-branch"
    with table.transaction() as tx:
        with tx.update_snapshot(branch="test-branch").replace() as rewrite:
            rewrite.delete_data_file(file_to_delete)
            rewrite.append_data_file(file_to_add)

    # Reload table to get updated refs
    table = catalog.load_table("default.test_replace_branch")

    test_branch_ref = table.metadata.refs["test-branch"]
    main_branch_ref = table.metadata.refs["main"]

    # Assert that the operation was successful on test-branch
    assert test_branch_ref.snapshot_id != initial_main_snapshot_id

    # Assert that the "test-branch" reference now points to a REPLACE snapshot
    new_snapshot = table.snapshot_by_id(test_branch_ref.snapshot_id)
    assert new_snapshot is not None
    summary = cast(Summary, new_snapshot.summary)
    assert summary["operation"] == Operation.REPLACE

    # Assert that the "main" branch reference was completely untouched
    assert main_branch_ref.snapshot_id == initial_main_snapshot_id


def test_replace_retries_on_concurrent_append(catalog: Catalog) -> None:
    """Replace should succeed via retry when a concurrent append lands in a different partition."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField, StringType

    catalog.create_namespace("default")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="id"))
    table = catalog.create_table(
        identifier="default.test_replace_retry_append",
        schema=schema,
        partition_spec=spec,
    )

    # File in partition id=1
    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=100,
        partition=Record(1),
        spec_id=table.spec().spec_id,
    )

    # Append file_a to partition 1
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    # Load two references to simulate concurrent access
    tbl1 = catalog.load_table("default.test_replace_retry_append")
    tbl2 = catalog.load_table("default.test_replace_retry_append")

    # tbl1 appends a new file to a DIFFERENT partition (non-conflicting)
    unrelated_file = _create_dummy_data_file(
        file_path="s3://bucket/test/data/unrelated.parquet",
        record_count=50,
        partition=Record(2),
        spec_id=table.spec().spec_id,
    )
    with tbl1.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(unrelated_file)

    # tbl2 replaces file_a with file_b in partition 1 — should succeed via retry
    file_b = _create_dummy_data_file(
        file_path="s3://bucket/test/data/b.parquet",
        record_count=100,
        partition=Record(1),
        spec_id=table.spec().spec_id,
    )
    with tbl2.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_a)
            rewrite.append_data_file(file_b)

    refreshed = catalog.load_table("default.test_replace_retry_append")
    snapshot = cast(Snapshot, refreshed.current_snapshot())
    summary = cast(Summary, snapshot.summary)

    # The replace landed successfully after retry
    assert summary["operation"] == Operation.REPLACE


def test_replace_raises_on_concurrent_delete_of_same_file(catalog: Catalog) -> None:
    """Replace should raise ValidationException when a concurrent commit deletes the same file."""
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_conflict",
        schema=Schema(),
    )

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=100,
    )

    # Append file_a
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    # Load two references
    tbl1 = catalog.load_table("default.test_replace_conflict")
    tbl2 = catalog.load_table("default.test_replace_conflict")

    # tbl1 replaces file_a first
    file_b = _create_dummy_data_file(
        file_path="s3://bucket/test/data/b.parquet",
        record_count=100,
    )
    with tbl1.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_a)
            rewrite.append_data_file(file_b)

    # tbl2 tries to replace the same file_a — should fail
    file_c = _create_dummy_data_file(
        file_path="s3://bucket/test/data/c.parquet",
        record_count=100,
    )
    with pytest.raises(ValidationException):
        with tbl2.transaction() as tx:
            with tx.update_snapshot().replace() as rewrite:
                rewrite.delete_data_file(file_a)
                rewrite.append_data_file(file_c)


def test_replace_refresh_for_retry_clears_cached_entries(catalog: Catalog) -> None:
    """Verify that _refresh_for_retry clears the cached deleted entries."""
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_refresh",
        schema=Schema(),
    )

    from pyiceberg.table import Transaction
    from pyiceberg.table.update.snapshot import _RewriteFiles

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=100,
    )

    # Append file_a
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    tx = Transaction(table, autocommit=False)
    producer = _RewriteFiles(
        operation=Operation.REPLACE,
        transaction=tx,
        io=table.io,
    )
    producer.delete_data_file(file_a)

    # Force computation of the cached property
    entries = producer._cached_deleted_entries
    assert len(entries) == 1
    assert "_cached_deleted_entries" in producer.__dict__

    # Simulate retry
    producer._refresh_for_retry()

    # The cached property should be cleared
    assert "_cached_deleted_entries" not in producer.__dict__


def test_replace_concurrent_replace_different_files_retries_successfully(catalog: Catalog) -> None:
    """Two concurrent replaces on different files should both succeed (second via retry)."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import IntegerType, NestedField, StringType

    catalog.create_namespace("default")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="id"))
    table = catalog.create_table(
        identifier="default.test_replace_diff_files",
        schema=schema,
        partition_spec=spec,
    )

    file_part1 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part1.parquet",
        record_count=100,
        partition=Record(1),
        spec_id=table.spec().spec_id,
    )
    file_part2 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part2.parquet",
        record_count=100,
        partition=Record(2),
        spec_id=table.spec().spec_id,
    )

    # Append both files
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_part1)
            append_snapshot.append_data_file(file_part2)

    tbl1 = catalog.load_table("default.test_replace_diff_files")
    tbl2 = catalog.load_table("default.test_replace_diff_files")

    # tbl1 replaces file in partition 1
    new_part1 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part1_new.parquet",
        record_count=100,
        partition=Record(1),
        spec_id=table.spec().spec_id,
    )
    with tbl1.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_part1)
            rewrite.append_data_file(new_part1)

    # tbl2 replaces file in partition 2 — different partition, should succeed via retry
    new_part2 = _create_dummy_data_file(
        file_path="s3://bucket/test/data/part2_new.parquet",
        record_count=100,
        partition=Record(2),
        spec_id=table.spec().spec_id,
    )
    with tbl2.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_part2)
            rewrite.append_data_file(new_part2)

    refreshed = catalog.load_table("default.test_replace_diff_files")
    snapshot = cast(Snapshot, refreshed.current_snapshot())
    summary = cast(Summary, snapshot.summary)
    assert summary["operation"] == Operation.REPLACE

    # Both replaces landed — all entries should reference the new files
    entries: list[ManifestEntry] = []
    for m in snapshot.manifests(refreshed.io):
        entries.extend(m.fetch_manifest_entry(refreshed.io, discard_deleted=True))
    file_paths = {e.data_file.file_path for e in entries}
    assert "s3://bucket/test/data/part1_new.parquet" in file_paths
    assert "s3://bucket/test/data/part2_new.parquet" in file_paths
    assert "s3://bucket/test/data/part1.parquet" not in file_paths
    assert "s3://bucket/test/data/part2.parquet" not in file_paths


def test_replace_does_not_conflict_with_same_partition_append(catalog: Catalog) -> None:
    """Replace should NOT conflict with a concurrent append to the same partition.

    This validates the key behavioral difference from the base _validate_concurrency:
    Java's BaseRewriteFiles does not call validateAddedDataFiles, so concurrent appends
    (even to the same partition) are not conflicts for a replace operation.
    """
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_no_append_conflict",
        schema=Schema(),
    )

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=100,
    )

    # Append file_a
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    tbl1 = catalog.load_table("default.test_replace_no_append_conflict")
    tbl2 = catalog.load_table("default.test_replace_no_append_conflict")

    # tbl1 appends to the SAME (unpartitioned) table — this is NOT a conflict for replace
    appended_file = _create_dummy_data_file(
        file_path="s3://bucket/test/data/appended.parquet",
        record_count=50,
    )
    with tbl1.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(appended_file)

    # tbl2 replaces file_a — should succeed even though the append was to the same table
    file_b = _create_dummy_data_file(
        file_path="s3://bucket/test/data/b.parquet",
        record_count=100,
    )
    with tbl2.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_a)
            rewrite.append_data_file(file_b)

    refreshed = catalog.load_table("default.test_replace_no_append_conflict")
    snapshot = cast(Snapshot, refreshed.current_snapshot())
    summary = cast(Summary, snapshot.summary)
    assert summary["operation"] == Operation.REPLACE

    # All three files should be live: appended + replacement
    entries: list[ManifestEntry] = []
    for m in snapshot.manifests(refreshed.io):
        entries.extend(m.fetch_manifest_entry(refreshed.io, discard_deleted=True))
    file_paths = {e.data_file.file_path for e in entries}
    assert "s3://bucket/test/data/b.parquet" in file_paths
    assert "s3://bucket/test/data/appended.parquet" in file_paths
    assert "s3://bucket/test/data/a.parquet" not in file_paths


def test_replace_only_deletes_without_adds(catalog: Catalog) -> None:
    """Replace with files_to_delete but no files_to_add should succeed (pure compaction removal)."""
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_delete_only",
        schema=Schema(),
    )

    file_a = _create_dummy_data_file(
        file_path="s3://bucket/test/data/a.parquet",
        record_count=100,
    )

    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_a)

    # Delete file_a without adding anything (records go from 100 to 0 — added <= deleted)
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_a)

    snapshot = cast(Snapshot, table.current_snapshot())
    summary = cast(Summary, snapshot.summary)
    assert summary["operation"] == Operation.REPLACE
    assert summary["deleted-data-files"] == "1"
    assert summary["deleted-records"] == "100"
    assert summary["total-records"] == "0"


def test_replace_only_adds_raises_without_deletes(catalog: Catalog) -> None:
    """Replace with files_to_add but no files_to_delete should still commit.

    Unlike Java (which requires files-to-delete to be non-empty), PyIceberg's
    _RewriteFiles allows add-only commits. This is a deliberate simplification
    — the added_records <= deleted_records check will catch truly invalid cases.
    An add-only replace with 0 deleted records means added_records > 0 > 0 which
    would violate the invariant and raise.
    """
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_add_only",
        schema=Schema(),
    )

    new_file = _create_dummy_data_file(
        file_path="s3://bucket/test/data/new.parquet",
        record_count=100,
    )

    # Adding without deleting: added_records (100) > deleted_records (0) → should raise
    with pytest.raises(ValidationException, match="Invalid replace"):
        with table.transaction() as tx:
            with tx.update_snapshot().replace() as rewrite:
                rewrite.append_data_file(new_file)
