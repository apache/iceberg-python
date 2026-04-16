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


def test_replace_internally(catalog: Catalog) -> None:
    # Setup a basic table using the catalog fixture
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace",
        schema=Schema(),
    )

    # 1. File we will delete
    file_to_delete = DataFile.from_args(
        file_path="s3://bucket/test/data/deleted.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_delete.spec_id = 0

    # 2. File we will leave completely untouched
    file_to_keep = DataFile.from_args(
        file_path="s3://bucket/test/data/kept.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=50,
        file_size_in_bytes=512,
        content=DataFileContent.DATA,
    )
    file_to_keep.spec_id = 0

    # 3. File we are adding as a replacement
    file_to_add = DataFile.from_args(
        file_path="s3://bucket/test/data/added.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_add.spec_id = 0

    # Initially append BOTH the file to delete and the file to keep
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)
            append_snapshot.append_data_file(file_to_keep)

    old_snapshot = cast(Snapshot, table.current_snapshot())
    old_snapshot_id = old_snapshot.snapshot_id
    old_sequence_number = cast(int, old_snapshot.sequence_number)

    # Call the internal replace API
    with table.transaction() as tx:
        with tx.update_snapshot().replace() as rewrite:
            rewrite.delete_data_file(file_to_delete)
            rewrite.append_data_file(file_to_add)

    snapshot = cast(Snapshot, table.current_snapshot())
    summary = cast(Summary, snapshot.summary)

    # 1. Has a unique snapshot ID
    assert snapshot.snapshot_id is not None
    assert snapshot.snapshot_id != old_snapshot_id

    # 2. Parent points to the previous snapshot
    assert snapshot.parent_snapshot_id == old_snapshot_id

    # 3. Sequence number is exactly previous + 1
    assert snapshot.sequence_number == old_sequence_number + 1

    # 4. Operation type is set to "replace"
    assert summary["operation"] == Operation.REPLACE

    # 5. Manifest list path is correct (just verify it exists and is a string path)
    assert snapshot.manifest_list is not None
    assert isinstance(snapshot.manifest_list, str)

    # 6. Summary counts are accurate
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

    # We expect 3 entries: ADDED, DELETED, and EXISTING
    assert len(entries) == 3

    # Check ADDED
    added_entries = [e for e in entries if e.status == ManifestEntryStatus.ADDED]
    assert len(added_entries) == 1
    assert added_entries[0].data_file.file_path == file_to_add.file_path
    assert added_entries[0].snapshot_id == snapshot.snapshot_id

    # Check DELETED
    deleted_entries = [e for e in entries if e.status == ManifestEntryStatus.DELETED]
    assert len(deleted_entries) == 1
    assert deleted_entries[0].data_file.file_path == file_to_delete.file_path
    assert deleted_entries[0].snapshot_id == snapshot.snapshot_id

    # Check EXISTING
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

    file_a = DataFile.from_args(
        file_path="s3://bucket/test/data/a.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        content=DataFileContent.DATA,
    )
    file_a.spec_id = 0

    file_b = DataFile.from_args(
        file_path="s3://bucket/test/data/b.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        content=DataFileContent.DATA,
    )
    file_b.spec_id = 0

    file_c = DataFile.from_args(
        file_path="s3://bucket/test/data/c.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        content=DataFileContent.DATA,
    )
    file_c.spec_id = 0

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

    # ASSERTION 1: The untouched manifest is completely reused (the path matches exactly)
    assert manifest_b_path in manifest_paths_after

    # ASSERTION 2: File A's old manifest is NOT reused (since it was rewritten to change status to DELETED)
    assert manifest_a_path not in manifest_paths_after


def test_replace_empty_files(catalog: Catalog) -> None:
    # Setup a basic table using the catalog fixture
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_empty",
        schema=Schema(),
    )

    # Replacing empty lists should not throw errors, but should produce no changes.
    with table.transaction() as tx:
        with tx.update_snapshot().replace():
            pass  # Entering and exiting the context manager without adding/deleting

    # History should be completely empty since no files were rewritten
    assert len(table.history()) == 0
    assert table.current_snapshot() is None


def test_replace_missing_file_abort(catalog: Catalog) -> None:
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_missing",
        schema=Schema(),
    )

    fake_data_file = DataFile.from_args(
        file_path="s3://bucket/test/data/does_not_exist.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    fake_data_file.spec_id = 0

    new_data_file = DataFile.from_args(
        file_path="s3://bucket/test/data/new.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    new_data_file.spec_id = 0

    # Ensure it aborts when trying to replace a file that isn't in the table
    with pytest.raises(ValueError, match="Cannot delete files that are not present in the table"):
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

    file_to_delete = DataFile.from_args(
        file_path="s3://bucket/test/data/deleted.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_delete.spec_id = 0

    # Create a new file with MORE records than the one we are deleting
    too_many_records_file = DataFile.from_args(
        file_path="s3://bucket/test/data/too_many.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=101,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    too_many_records_file.spec_id = 0

    # Initially append to have something to replace
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)

    # Ensure it enforces the invariant: records added <= records removed
    with pytest.raises(ValueError, match=r"Invalid replace: records added \(101\) exceeds records removed \(100\)"):
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
    file_to_delete = DataFile.from_args(
        file_path="s3://bucket/test/data/deleted.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_delete.spec_id = 0

    # New data file only has 90 records (simulating 10 records were soft-deleted)
    shrunk_file_to_add = DataFile.from_args(
        file_path="s3://bucket/test/data/shrunk.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=90,
        file_size_in_bytes=900,
        content=DataFileContent.DATA,
    )
    shrunk_file_to_add.spec_id = 0

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
    # Setup a basic table
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_delete_manifests",
        schema=Schema(),
        properties={"format-version": "2"},
    )

    # 1. Data file we will replace
    file_a = DataFile.from_args(
        file_path="s3://bucket/test/data/a.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        content=DataFileContent.DATA,
    )
    file_a.spec_id = 0

    # 2. A Position Delete file (representing row-level deletes)
    file_a_deletes = DataFile.from_args(
        file_path="s3://bucket/test/data/a_deletes.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=2,
        file_size_in_bytes=50,
        content=DataFileContent.POSITION_DELETES,
    )
    file_a_deletes.spec_id = 0

    # 3. Data file we are adding as a replacement
    file_b = DataFile.from_args(
        file_path="s3://bucket/test/data/b.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        content=DataFileContent.DATA,
    )
    file_b.spec_id = 0

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

    file_1 = DataFile.from_args(
        file_path="s3://bucket/test/data/1.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_1.spec_id = 0

    file_2 = DataFile.from_args(
        file_path="s3://bucket/test/data/2.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_2.spec_id = 0

    file_1_new = DataFile.from_args(
        file_path="s3://bucket/test/data/1_new.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=50,
        file_size_in_bytes=512,
        content=DataFileContent.DATA,
    )
    file_1_new.spec_id = 0

    file_2_new = DataFile.from_args(
        file_path="s3://bucket/test/data/2_new.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=50,
        file_size_in_bytes=512,
        content=DataFileContent.DATA,
    )
    file_2_new.spec_id = 0

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
    file_part1 = DataFile.from_args(
        file_path="s3://bucket/test/data/part1.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(1),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_part1.spec_id = table.spec().spec_id

    # File in partition id=2
    file_part2 = DataFile.from_args(
        file_path="s3://bucket/test/data/part2.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(2),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_part2.spec_id = table.spec().spec_id

    # Add initial files
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_part1)
            append_snapshot.append_data_file(file_part2)

    # Replace file in partition 1
    file_part1_new = DataFile.from_args(
        file_path="s3://bucket/test/data/part1_new.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(1),
        record_count=50,
        file_size_in_bytes=512,
        content=DataFileContent.DATA,
    )
    file_part1_new.spec_id = table.spec().spec_id

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

    file_a = DataFile.from_args(
        file_path="s3://bucket/test/data/a.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        content=DataFileContent.DATA,
    )
    file_a.spec_id = 0

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
    # Setup a basic table using the catalog fixture
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_branch",
        schema=Schema(),
    )

    # 1. File we will delete
    file_to_delete = DataFile.from_args(
        file_path="s3://bucket/test/data/deleted.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_delete.spec_id = 0

    # 2. File we are adding as a replacement
    file_to_add = DataFile.from_args(
        file_path="s3://bucket/test/data/added.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_add.spec_id = 0

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
