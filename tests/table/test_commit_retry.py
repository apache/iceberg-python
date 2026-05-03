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
    tbl2.append(df)

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

    print(f"DEBUG tbl1.metadata.current_snapshot_id={tbl1.metadata.current_snapshot_id}")
    print(f"DEBUG tbl2.metadata.current_snapshot_id={tbl2.metadata.current_snapshot_id}")
    print(f"DEBUG tbl1.metadata is tbl2.metadata: {tbl1.metadata is tbl2.metadata}")
    print(f"DEBUG tbl1 is tbl2: {tbl1 is tbl2}")
    print(f"DEBUG id(tbl1)={id(tbl1)} id(tbl2)={id(tbl2)}")
    print(f"DEBUG id(tbl1.metadata)={id(tbl1.metadata)} id(tbl2.metadata)={id(tbl2.metadata)}")

    original_rebuild = Transaction._rebuild_snapshot_updates
    rebuild_count = 0

    def counting_rebuild(self_tx: Transaction) -> None:
        nonlocal rebuild_count
        rebuild_count += 1
        print(f"DEBUG _rebuild_snapshot_updates called, count={rebuild_count}")
        original_rebuild(self_tx)

    original_do_commit = type(tbl2)._do_commit
    commit_count = 0

    def counting_do_commit(self, updates, requirements):
        nonlocal commit_count
        commit_count += 1
        print(f"DEBUG _do_commit called, count={commit_count}")
        return original_do_commit(self, updates, requirements)

    with patch.object(Transaction, "_rebuild_snapshot_updates", counting_rebuild), \
         patch.object(type(tbl2), "_do_commit", counting_do_commit):
        tbl2.append(df)

    print(f"DEBUG rebuild_count={rebuild_count} commit_count={commit_count}")
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

    original_rebuild = Transaction._rebuild_snapshot_updates
    uncommitted_count_during_rebuild = 0

    def checking_rebuild(self_tx: Transaction) -> None:
        nonlocal uncommitted_count_during_rebuild
        original_rebuild(self_tx)
        for producer in self_tx._snapshot_producers:
            uncommitted_count_during_rebuild += len(producer._uncommitted_manifests)

    with patch.object(Transaction, "_rebuild_snapshot_updates", checking_rebuild):
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
