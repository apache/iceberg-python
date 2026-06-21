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
from unittest.mock import patch

import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException, ValidationException
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties, Transaction
from pyiceberg.table.snapshots import IsolationLevel, Operation
from pyiceberg.types import LongType, NestedField, StringType


def test_isolation_level_enum() -> None:
    assert IsolationLevel.SERIALIZABLE.value == "serializable"
    assert IsolationLevel.SNAPSHOT.value == "snapshot"
    assert IsolationLevel("serializable") is IsolationLevel.SERIALIZABLE
    assert IsolationLevel("snapshot") is IsolationLevel.SNAPSHOT


def test_commit_retry_table_properties() -> None:
    assert TableProperties.COMMIT_NUM_RETRIES == "commit.retry.num-retries"
    assert TableProperties.COMMIT_NUM_RETRIES_DEFAULT == 4
    assert TableProperties.COMMIT_MIN_RETRY_WAIT_MS == "commit.retry.min-wait-ms"
    assert TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT == 100
    assert TableProperties.COMMIT_MAX_RETRY_WAIT_MS == "commit.retry.max-wait-ms"
    assert TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT == 60000
    assert TableProperties.COMMIT_TOTAL_RETRY_TIME_MS == "commit.retry.total-timeout-ms"
    assert TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT == 1800000


def test_isolation_level_table_properties() -> None:
    assert TableProperties.WRITE_DELETE_ISOLATION_LEVEL == "write.delete.isolation-level"
    assert TableProperties.WRITE_UPDATE_ISOLATION_LEVEL == "write.update.isolation-level"
    assert TableProperties.WRITE_ISOLATION_LEVEL_DEFAULT == "serializable"


def _test_schema() -> Schema:
    return Schema(NestedField(1, "x", LongType(), required=False))


def test_commit_retry_on_commit_failed(catalog: Catalog) -> None:
    """Verify that CommitFailedException triggers retry for append operations."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.retry_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    # Load two references to the same table to simulate concurrent access
    tbl1 = catalog.load_table("default.retry_test")
    tbl2 = catalog.load_table("default.retry_test")

    # First append succeeds
    tbl1.append(df)

    # Second append should succeed via retry (append vs append never conflicts)
    import pyiceberg.table as _table_module

    RuntimeTransaction = _table_module.Transaction
    original_rebuild = RuntimeTransaction._rebuild_snapshot_updates
    rebuild_count = 0

    def counting_rebuild(self_tx: Any) -> None:
        nonlocal rebuild_count
        rebuild_count += 1
        original_rebuild(self_tx)

    with patch.object(RuntimeTransaction, "_rebuild_snapshot_updates", counting_rebuild):
        tbl2.append(df)

    assert rebuild_count == 1, "Expected exactly one retry via _rebuild_snapshot_updates"

    # Both appends should be visible
    refreshed = catalog.load_table("default.retry_test")
    result = refreshed.scan().to_arrow()
    assert len(result) == 6


def test_no_retry_without_snapshot_producers(catalog: Catalog) -> None:
    """Verify that a transaction with no snapshot producers has an empty producer list."""
    catalog.create_namespace("default")
    schema = _test_schema()
    table = catalog.create_table("default.no_retry_test", schema=schema)

    tx = Transaction(table, autocommit=False)
    tx.set_properties({"key": "value"})

    # No snapshot producers registered
    assert len(tx._snapshot_producers) == 0


def test_rebuild_snapshot_updates_preserves_non_snapshot_updates(catalog: Catalog) -> None:
    """Verify that non-snapshot updates survive retry."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.rebuild_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1]})

    tbl1 = catalog.load_table("default.rebuild_test")
    tbl2 = catalog.load_table("default.rebuild_test")

    # tbl1 commits first
    tbl1.append(df)

    # tbl2 does both property change and append in one transaction
    with tbl2.transaction() as tx:
        tx.set_properties({"test_key": "test_value"})
        tx.append(df)

    # Both the property and the data should be committed
    refreshed = catalog.load_table("default.rebuild_test")
    assert refreshed.metadata.properties.get("test_key") == "test_value"
    assert len(refreshed.scan().to_arrow()) == 2


def test_refresh_for_retry_resets_producer_state(catalog: Catalog) -> None:
    """Verify that _refresh_for_retry resets the necessary fields."""
    catalog.create_namespace("default")
    schema = _test_schema()
    table = catalog.create_table("default.refresh_test", schema=schema)

    from pyiceberg.table.update.snapshot import _FastAppendFiles

    tx = Transaction(table, autocommit=False)
    producer = _FastAppendFiles(
        operation=Operation.APPEND,
        transaction=tx,
        io=table.io,
    )

    original_snapshot_id = producer._snapshot_id
    original_uuid = producer.commit_uuid

    producer._refresh_for_retry()

    assert producer._snapshot_id != original_snapshot_id
    assert producer.commit_uuid != original_uuid
    # parent stays None for empty table
    assert producer._parent_snapshot_id is None


def test_concurrent_delete_delete_raises_validation_exception(catalog: Catalog) -> None:
    """Concurrent deletes on the same data should fail with ValidationException."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.del_del_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.del_del_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.del_del_test")
    tbl2 = catalog.load_table("default.del_del_test")

    tbl1.delete("x == 1")

    with pytest.raises(ValidationException):
        tbl2.delete("x == 1")


def test_concurrent_append_delete_raises_validation_exception(catalog: Catalog) -> None:
    """Delete after a concurrent append fails with ValidationException under serializable isolation."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.app_del_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.app_del_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.app_del_test")
    tbl2 = catalog.load_table("default.app_del_test")

    tbl1.append(df)

    with pytest.raises(ValidationException):
        tbl2.delete("x == 1")


def test_concurrent_delete_append_retries_successfully(catalog: Catalog) -> None:
    """Append after a concurrent delete should succeed via retry."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.del_app_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.del_app_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.del_app_test")
    tbl2 = catalog.load_table("default.del_app_test")

    tbl1.delete("x == 1")

    import pyiceberg.table as _table_module

    RuntimeTransaction = _table_module.Transaction
    original_rebuild = RuntimeTransaction._rebuild_snapshot_updates
    rebuild_count = 0

    def counting_rebuild(self_tx: Any) -> None:
        nonlocal rebuild_count
        rebuild_count += 1
        original_rebuild(self_tx)

    with patch.object(RuntimeTransaction, "_rebuild_snapshot_updates", counting_rebuild):
        tbl2.append(df)

    assert rebuild_count == 1

    refreshed = catalog.load_table("default.del_app_test")
    result = refreshed.scan().to_arrow()
    # Original 3 rows, minus 1 deleted, plus 3 appended = 5
    assert len(result) == 5


def test_retry_exhaustion_raises_commit_failed(catalog: Catalog) -> None:
    """When retries are exhausted, CommitFailedException should be raised."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table(
        "default.exhaust_test",
        schema=schema,
        properties={"commit.retry.num-retries": "0"},
    )

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl1 = catalog.load_table("default.exhaust_test")
    tbl2 = catalog.load_table("default.exhaust_test")

    tbl1.append(df)

    with pytest.raises(CommitFailedException):
        tbl2.append(df)


def test_delete_files_refresh_clears_compute_deletes_cache(catalog: Catalog) -> None:
    """Verify that _refresh_for_retry clears the _compute_deletes cached property."""
    catalog.create_namespace("default")
    schema = _test_schema()
    table = catalog.create_table("default.cache_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})
    table.append(df)
    table = catalog.load_table("default.cache_test")

    from pyiceberg.expressions import EqualTo
    from pyiceberg.table.update.snapshot import _DeleteFiles

    tx = Transaction(table, autocommit=False)
    producer = _DeleteFiles(
        operation=Operation.DELETE,
        transaction=tx,
        io=table.io,
    )
    producer.delete_by_predicate(EqualTo("x", 1))

    # Access _compute_deletes to populate the cache
    _ = producer._compute_deletes

    assert "_compute_deletes" in producer.__dict__

    producer._refresh_for_retry()

    assert "_compute_deletes" not in producer.__dict__


def test_concurrent_overwrite_overwrite_raises_validation_exception(catalog: Catalog) -> None:
    """Concurrent overwrites on the same data should fail with ValidationException."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.ow_ow_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.ow_ow_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.ow_ow_test")
    tbl2 = catalog.load_table("default.ow_ow_test")

    tbl1.overwrite(pa.table({"x": [10, 20, 30]}), overwrite_filter="x > 0")
    with pytest.raises(ValidationException):
        tbl2.overwrite(pa.table({"x": [40, 50, 60]}), overwrite_filter="x > 0")


def test_concurrent_overwrite_append_retries_successfully(catalog: Catalog) -> None:
    """Append after a concurrent overwrite should succeed via retry."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.ow_app_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.ow_app_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.ow_app_test")
    tbl2 = catalog.load_table("default.ow_app_test")

    tbl1.overwrite(pa.table({"x": [10, 20, 30]}), overwrite_filter="x > 0")
    tbl2.append(pa.table({"x": [4, 5, 6]}))

    refreshed = catalog.load_table("default.ow_app_test")
    result = refreshed.scan().to_arrow()
    # overwrite replaced 3 rows with 3 new rows, then append added 3 more = 6
    assert len(result) == 6


def test_snapshot_isolation_allows_concurrent_append_delete(catalog: Catalog) -> None:
    """Under snapshot isolation, delete after a concurrent append should succeed via retry."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table(
        "default.snapshot_iso_test",
        schema=schema,
        properties={"write.delete.isolation-level": "snapshot"},
    )

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.snapshot_iso_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.snapshot_iso_test")
    tbl2 = catalog.load_table("default.snapshot_iso_test")

    tbl1.append(df)

    # Under serializable this would raise ValidationException,
    # but under snapshot isolation _validate_added_data_files is skipped
    tbl2.delete("x == 1")

    refreshed = catalog.load_table("default.snapshot_iso_test")
    result = refreshed.scan().to_arrow()
    # Original 3, delete removes x==1 from original (1 row), append adds 3 = 5
    assert len(result) == 5


def test_uncommitted_manifests_tracked_correctly(catalog: Catalog) -> None:
    """Verify that uncommitted manifests are moved to _uncommitted_manifests on retry."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.manifest_track_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.manifest_track_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.manifest_track_test")
    tbl2 = catalog.load_table("default.manifest_track_test")

    tbl1.append(df)

    import pyiceberg.table as _table_module2

    RuntimeTransaction2 = _table_module2.Transaction
    original_rebuild = RuntimeTransaction2._rebuild_snapshot_updates
    uncommitted_count_during_rebuild = 0

    def checking_rebuild(self_tx: Any) -> None:
        nonlocal uncommitted_count_during_rebuild
        original_rebuild(self_tx)
        for producer in self_tx._snapshot_producers:
            uncommitted_count_during_rebuild += len(producer._uncommitted_manifests)

    with patch.object(RuntimeTransaction2, "_rebuild_snapshot_updates", checking_rebuild):
        tbl2.append(df)

    # After rebuild, the first attempt's manifests should be in _uncommitted_manifests
    assert uncommitted_count_during_rebuild > 0


def test_concurrent_deletes_on_different_partitions_succeed(catalog: Catalog) -> None:
    """Concurrent deletes on different partitions should succeed via retry thanks to conflict detection filter."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "category", StringType(), required=False),
        NestedField(2, "value", LongType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="category"))
    catalog.create_table("default.part_del_test", schema=schema, partition_spec=spec)

    import pyarrow as pa

    df = pa.table(
        {
            "category": ["a", "a", "b", "b"],
            "value": [1, 2, 3, 4],
        }
    )

    tbl = catalog.load_table("default.part_del_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.part_del_test")
    tbl2 = catalog.load_table("default.part_del_test")

    # Delete from different partitions should not conflict
    tbl1.delete("category == 'a'")
    tbl2.delete("category == 'b'")

    refreshed = catalog.load_table("default.part_del_test")
    result = refreshed.scan().to_arrow()
    assert len(result) == 0


def test_concurrent_partial_deletes_on_different_partitions_succeed(catalog: Catalog) -> None:
    """Concurrent partial deletes (CoW rewrite) on different partitions should succeed.

    This tests the auto-computed partition predicate from _build_delete_files_partition_predicate.
    """
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "category", StringType(), required=False),
        NestedField(2, "value", LongType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="category"))
    catalog.create_table("default.part_partial_del_test", schema=schema, partition_spec=spec)

    import pyarrow as pa

    df = pa.table(
        {
            "category": ["a", "a", "b", "b"],
            "value": [1, 2, 3, 4],
        }
    )

    tbl = catalog.load_table("default.part_partial_del_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.part_partial_del_test")
    tbl2 = catalog.load_table("default.part_partial_del_test")

    # Partial delete: only value==1 in partition a, triggers CoW rewrite
    tbl1.delete("value == 1")
    # Partial delete: only value==3 in partition b, triggers CoW rewrite
    tbl2.delete("value == 3")

    refreshed = catalog.load_table("default.part_partial_del_test")
    result = refreshed.scan().to_arrow()
    # Original 4 rows, minus value==1 and value==3 = 2 rows remaining
    assert len(result) == 2


def test_overwrite_uses_update_isolation_level(catalog: Catalog) -> None:
    """Verify that overwrite() reads write.update.isolation-level, not write.delete.isolation-level."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table(
        "default.update_iso_test",
        schema=schema,
        properties={
            "write.delete.isolation-level": "serializable",
            "write.update.isolation-level": "snapshot",
        },
    )

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.update_iso_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.update_iso_test")
    tbl2 = catalog.load_table("default.update_iso_test")

    tbl1.append(df)

    # Under write.delete.isolation-level=serializable this would raise ValidationException.
    # But overwrite() uses write.update.isolation-level=snapshot, so it succeeds.
    tbl2.overwrite(pa.table({"x": [10, 20, 30]}), overwrite_filter="x > 0")

    refreshed = catalog.load_table("default.update_iso_test")
    result = refreshed.scan().to_arrow()
    # overwrite with x > 0 deletes all rows (including tbl1's append), then adds 3 new rows
    assert len(result) == 3


def test_overwrite_with_serializable_update_isolation_raises(catalog: Catalog) -> None:
    """Verify that overwrite() raises ValidationException when write.update.isolation-level=serializable."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table(
        "default.update_serial_test",
        schema=schema,
        properties={
            "write.update.isolation-level": "serializable",
        },
    )

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.update_serial_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.update_serial_test")
    tbl2 = catalog.load_table("default.update_serial_test")

    tbl1.append(df)

    with pytest.raises(ValidationException):
        tbl2.overwrite(pa.table({"x": [10, 20, 30]}), overwrite_filter="x > 0")


def test_clean_all_uncommitted_on_validation_exception(catalog: Catalog) -> None:
    """Verify that all manifests are cleaned up when commit aborts with ValidationException."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.clean_abort_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    tbl = catalog.load_table("default.clean_abort_test")
    tbl.append(df)

    tbl1 = catalog.load_table("default.clean_abort_test")
    tbl2 = catalog.load_table("default.clean_abort_test")

    tbl1.delete("x == 1")

    from pyiceberg.table.update.snapshot import _SnapshotProducer

    captured_producers: list[Any] = []
    original_clean_all = _SnapshotProducer._clean_all_uncommitted

    def capturing_clean_all(self_producer: Any) -> None:
        captured_producers.append(self_producer)
        original_clean_all(self_producer)

    with patch.object(_SnapshotProducer, "_clean_all_uncommitted", capturing_clean_all):
        with pytest.raises(ValidationException):
            tbl2.delete("x == 1")

    # _clean_all_uncommitted was called on abort
    assert len(captured_producers) > 0
    # All manifest lists should be cleared
    for producer in captured_producers:
        assert producer._written_manifests == []
        assert producer._uncommitted_manifests == []


def test_mixed_delete_overwrite_starts_from_catalog_snapshot(catalog: Catalog) -> None:
    """Mixed full-file and partial deletes should validate from the original table snapshot."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.table.update.snapshot import _DeleteFiles, _OverwriteFiles
    from pyiceberg.transforms import IdentityTransform

    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "category", StringType(), required=False),
        NestedField(2, "value", LongType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="category"))
    table = catalog.create_table("default.mixed_delete_start_snapshot", schema=schema, partition_spec=spec)

    import pyarrow as pa

    # Partition "a" has one row (will be fully deleted) and partition "b" has two rows (partial delete)
    table.append(pa.table({"category": ["a", "b", "b"], "value": [1, 2, 3]}))

    base_snapshot_id = table.metadata.current_snapshot_id

    tx = Transaction(table, autocommit=False)
    # "value == 1" deletes the entire file in partition "a" (full-file delete)
    # "value == 2" partially deletes from partition "b" (CoW rewrite)
    tx.delete("value <= 2")

    assert len(tx._snapshot_producers) == 2

    delete_producer = next(p for p in tx._snapshot_producers if isinstance(p, _DeleteFiles))
    overwrite_producer = next(p for p in tx._snapshot_producers if isinstance(p, _OverwriteFiles))

    assert delete_producer._starting_snapshot_id == base_snapshot_id
    assert overwrite_producer._starting_snapshot_id == base_snapshot_id


def test_validate_concurrency_skips_when_commit_window_is_empty(catalog: Catalog) -> None:
    """Validation should be skipped when CommitWindow.is_empty() is True (no concurrent commits)."""
    catalog.create_namespace("default")
    schema = _test_schema()
    table = catalog.create_table("default.missing_parent_test", schema=schema)

    import pyarrow as pa

    table.append(pa.table({"x": [1, 2, 3]}))

    from pyiceberg.table.update.snapshot import CommitWindow, _DeleteFiles

    tx = Transaction(table, autocommit=False)
    producer = _DeleteFiles(
        operation=Operation.DELETE,
        transaction=tx,
        io=table.io,
    )

    # CommitWindow where base == head means no concurrent commits occurred
    current = table.metadata.snapshot_by_id(table.metadata.current_snapshot_id)
    producer._commit_window = CommitWindow(base=current, head=current)

    # Should not raise (validation is skipped)
    producer._validate_concurrency()


def test_validate_concurrency_raises_on_missing_starting_snapshot(catalog: Catalog) -> None:
    """CommitWindow.resolve should raise when starting_snapshot_id cannot be resolved."""
    catalog.create_namespace("default")
    schema = _test_schema()
    table = catalog.create_table("default.missing_starting_test", schema=schema)

    import pyarrow as pa

    table.append(pa.table({"x": [1, 2, 3]}))

    from pyiceberg.table.update.snapshot import CommitWindow

    with pytest.raises(ValidationException, match="Cannot find starting snapshot"):
        CommitWindow.resolve(table.metadata, base_id=99999999, branch="main")


def test_mixed_delete_overwrite_retries_successfully(catalog: Catalog) -> None:
    """A mixed full-file + partial delete should succeed via retry, not raise ValidationException."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "category", StringType(), required=False),
        NestedField(2, "value", LongType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="category"))
    catalog.create_table("default.mixed_retry_test", schema=schema, partition_spec=spec)

    import pyarrow as pa

    tbl = catalog.load_table("default.mixed_retry_test")

    # 3 partitions, one data file each: a->[1,2], b->[3,4], c->[5,6]
    tbl.append(pa.table({"category": ["a", "a", "b", "b", "c", "c"], "value": [1, 2, 3, 4, 5, 6]}))

    tbl1 = catalog.load_table("default.mixed_retry_test")
    tbl2 = catalog.load_table("default.mixed_retry_test")

    # Concurrent append to partition 'c' (commits first, advances the HEAD)
    tbl1.append(pa.table({"category": ["c"], "value": [7]}))

    # Mixed delete on tbl2 (stale snapshot):
    # partition 'a' is a partial rewrite (value==1 deleted, value==2 remains) -> _OverwriteFiles
    # partition 'b' is a full delete (category == 'b') -> _DeleteFiles
    # This should NOT conflict with the append to 'c', so retry should succeed.
    tbl2.delete("value == 1 or category == 'b'")

    result = catalog.load_table("default.mixed_retry_test").scan().to_arrow()
    assert sorted(result.column("value").to_pylist()) == [2, 5, 6, 7]


def test_manifest_list_cleanup_on_retry(catalog: Catalog) -> None:
    """Verify that manifest list files from failed retry attempts are cleaned up."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.manifest_list_cleanup_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    # Seed the table so there's a branch HEAD to conflict with
    tbl = catalog.load_table("default.manifest_list_cleanup_test")
    tbl.append(df)

    # Two writers see the same snapshot
    tbl1 = catalog.load_table("default.manifest_list_cleanup_test")
    tbl2 = catalog.load_table("default.manifest_list_cleanup_test")

    # tbl1 commits first, advancing catalog HEAD
    tbl1.append(df)

    # Track deletes on tbl2
    deleted_paths: list[str] = []
    original_delete = tbl2.io.delete

    def tracking_delete(path: str) -> None:
        deleted_paths.append(path)
        original_delete(path)

    with patch.object(tbl2.io, "delete", side_effect=tracking_delete):
        tbl2.append(df)

    # At least one manifest list (snap-*.avro) from the failed attempt should be deleted
    manifest_list_deletes = [p for p in deleted_paths if "snap-" in p and p.endswith(".avro")]
    assert len(manifest_list_deletes) >= 1, (
        f"Expected at least one orphaned manifest list to be cleaned up. Deleted paths: {deleted_paths}"
    )

    # Sanity check: all data committed
    refreshed = catalog.load_table("default.manifest_list_cleanup_test")
    assert len(refreshed.scan().to_arrow()) == 9


def test_manifest_list_cleanup_on_abort(catalog: Catalog) -> None:
    """Verify that ALL manifest lists are cleaned up when a commit permanently fails."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.manifest_list_abort_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    # Seed the table
    tbl = catalog.load_table("default.manifest_list_abort_test")
    tbl.append(df)

    # Two writers see the same snapshot
    tbl1 = catalog.load_table("default.manifest_list_abort_test")
    tbl2 = catalog.load_table("default.manifest_list_abort_test")

    # tbl1 deletes x==1, so tbl2's same delete will conflict
    tbl1.delete("x == 1")

    # Track deletes on tbl2
    deleted_paths: list[str] = []
    original_delete = tbl2.io.delete

    def tracking_delete(path: str) -> None:
        deleted_paths.append(path)
        original_delete(path)

    with patch.object(tbl2.io, "delete", side_effect=tracking_delete):
        with pytest.raises(ValidationException):
            tbl2.delete("x == 1")

    # Manifest list files should be cleaned up on abort
    manifest_list_deletes = [p for p in deleted_paths if "snap-" in p and p.endswith(".avro")]
    assert len(manifest_list_deletes) >= 1, (
        f"Expected manifest list cleanup on abort. Deleted paths: {deleted_paths}"
    )


def test_commit_retry_on_non_main_branch(catalog: Catalog) -> None:
    """Verify that commit retry works correctly on a non-main branch."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table("default.branch_retry_test", schema=schema)

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    # Seed the table and create a branch
    tbl = catalog.load_table("default.branch_retry_test")
    tbl.append(df)
    tbl.manage_snapshots().create_branch(
        snapshot_id=tbl.metadata.current_snapshot_id, branch_name="test-branch"
    ).commit()

    # Two writers targeting the same branch
    tbl1 = catalog.load_table("default.branch_retry_test")
    tbl2 = catalog.load_table("default.branch_retry_test")

    # tbl1 appends to the branch first
    tbl1.append(df, branch="test-branch")

    # tbl2 appends to the same branch (stale ref), should retry and succeed
    import pyiceberg.table as _table_module

    RuntimeTransaction = _table_module.Transaction
    original_rebuild = RuntimeTransaction._rebuild_snapshot_updates
    rebuild_count = 0

    def counting_rebuild(self_tx: Any) -> None:
        nonlocal rebuild_count
        rebuild_count += 1
        original_rebuild(self_tx)

    with patch.object(RuntimeTransaction, "_rebuild_snapshot_updates", counting_rebuild):
        tbl2.append(df, branch="test-branch")

    assert rebuild_count == 1, "Expected exactly one retry on non-main branch"

    # Both branch appends should be visible when scanning the branch
    refreshed = catalog.load_table("default.branch_retry_test")
    branch_snapshot_id = refreshed.metadata.refs["test-branch"].snapshot_id
    result = refreshed.scan(snapshot_id=branch_snapshot_id).to_arrow()
    assert len(result) == 9


def test_commit_retry_delete_on_non_main_branch(catalog: Catalog) -> None:
    """Verify that delete with retry works correctly on a non-main branch."""
    catalog.create_namespace("default")
    schema = _test_schema()
    catalog.create_table(
        "default.branch_delete_retry_test",
        schema=schema,
        properties={"write.delete.isolation-level": "snapshot"},
    )

    import pyarrow as pa

    df = pa.table({"x": [1, 2, 3]})

    # Seed and create branch
    tbl = catalog.load_table("default.branch_delete_retry_test")
    tbl.append(df)
    tbl.manage_snapshots().create_branch(
        snapshot_id=tbl.metadata.current_snapshot_id, branch_name="test-branch"
    ).commit()

    # Append more data to the branch
    tbl = catalog.load_table("default.branch_delete_retry_test")
    tbl.append(pa.table({"x": [4, 5, 6]}), branch="test-branch")

    # Two writers: one appends, one deletes on the same branch
    tbl1 = catalog.load_table("default.branch_delete_retry_test")
    tbl2 = catalog.load_table("default.branch_delete_retry_test")

    # tbl1 appends to branch (non-conflicting with delete on different data)
    tbl1.append(pa.table({"x": [7, 8, 9]}), branch="test-branch")

    # tbl2 deletes x==1 on branch. Should retry (stale ref) and succeed
    # because the concurrent append (x=7,8,9) does not conflict under snapshot isolation.
    tbl2.delete("x == 1", branch="test-branch")

    refreshed = catalog.load_table("default.branch_delete_retry_test")
    branch_snapshot_id = refreshed.metadata.refs["test-branch"].snapshot_id
    result = refreshed.scan(snapshot_id=branch_snapshot_id).to_arrow()
    # Original: 1,2,3,4,5,6 + append 7,8,9 - delete x==1 = 2,3,4,5,6,7,8,9
    assert sorted(result.column("x").to_pylist()) == [2, 3, 4, 5, 6, 7, 8, 9]
