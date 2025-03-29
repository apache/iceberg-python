# pylint:disable=redefined-outer-name,eval-used
from unittest.mock import Mock
import pytest

from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent, ManifestFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Operation, Snapshot, SnapshotLogEntry, SnapshotSummaryCollector, Summary, update_snapshot_summaries
from pyiceberg.table.update.snapshot import ExpireSnapshots
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
)

@pytest.fixture
def mock_table():
    """Fixture to create a mock table with metadata and snapshots."""
    snapshots = [
        Snapshot(snapshot_id=1, timestamp_ms=1000, manifest_list="manifest1.avro"),
        Snapshot(snapshot_id=2, timestamp_ms=2000, manifest_list="manifest2.avro"),
        Snapshot(snapshot_id=3, timestamp_ms=3000, manifest_list="manifest3.avro"),
    ]
    snapshot_log = [
        SnapshotLogEntry(snapshot_id=1, timestamp_ms=1000),
        SnapshotLogEntry(snapshot_id=2, timestamp_ms=2000),
        SnapshotLogEntry(snapshot_id=3, timestamp_ms=3000),
    ]
    metadata = TableMetadata(
        snapshots=snapshots,
        snapshot_log=snapshot_log,
        current_snapshot_id=3,
    )
    table = Mock(spec=Table)
    table.metadata = metadata
    return table


def test_expire_older_than(mock_table):
    """Test expiring snapshots older than a given timestamp."""
    expire_snapshots = ExpireSnapshots(mock_table)
    expire_snapshots.expire_older_than(2500)._commit(mock_table.metadata)

    remaining_snapshot_ids = {s.snapshot_id for s in mock_table.metadata.snapshots}
    assert remaining_snapshot_ids == {3}, "Only the latest snapshot should remain."


def test_retain_last(mock_table):
    """Test retaining the last N snapshots."""
    expire_snapshots = ExpireSnapshots(mock_table)
    expire_snapshots.retain_last(2)._commit(mock_table.metadata)

    remaining_snapshot_ids = {s.snapshot_id for s in mock_table.metadata.snapshots}
    assert remaining_snapshot_ids == {2, 3}, "The last two snapshots should remain."


def test_expire_specific_snapshot(mock_table):
    """Test explicitly expiring a specific snapshot."""
    expire_snapshots = ExpireSnapshots(mock_table)
    expire_snapshots.expire_snapshot_id(2)._commit(mock_table.metadata)

    remaining_snapshot_ids = {s.snapshot_id for s in mock_table.metadata.snapshots}
    assert remaining_snapshot_ids == {1, 3}, "Snapshot 2 should be expired."


def test_custom_delete_function(mock_table):
    """Test using a custom delete function for cleanup."""
    delete_func = Mock()
    expire_snapshots = ExpireSnapshots(mock_table)
    expire_snapshots.expire_snapshot_id(1).delete_with(delete_func)._commit(mock_table.metadata)

    delete_func.assert_called_once_with("Snapshot 1"), "Custom delete function should be called for expired snapshot."


def test_no_snapshots_to_expire(mock_table):
    """Test when no snapshots are identified for expiration."""
    expire_snapshots = ExpireSnapshots(mock_table)
    expire_snapshots.expire_older_than(500)._commit(mock_table.metadata)

    remaining_snapshot_ids = {s.snapshot_id for s in mock_table.metadata.snapshots}
    assert remaining_snapshot_ids == {1, 2, 3}, "No snapshots should be expired."
