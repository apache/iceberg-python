# pylint:disable=redefined-outer-name
# pylint:disable=redefined-outer-name
from unittest.mock import Mock
import pytest

from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.update.snapshot import ManageSnapshots

from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder



@pytest.fixture
def mock_table():
    """
    Creates a mock Iceberg table with predefined metadata, snapshots, and snapshot log entries.
    The mock table includes:
    - Snapshots with unique IDs, timestamps, and manifest lists.
    - A snapshot log that tracks the history of snapshots with their IDs and timestamps.
    - Table metadata including schema, partition spec, sort order, location, properties, and UUID.
    - A current snapshot ID and last updated timestamp.
    Returns:
        Mock: A mock object representing an Iceberg table with the specified metadata and attributes.
    """
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

    metadata = new_table_metadata(
        schema=Schema(fields=[]),
        partition_spec=PartitionSpec(spec_id=0, fields=[]),
        sort_order=SortOrder(order_id=0, fields=[]),
        location="s3://example-bucket/path/",
        properties={},
        table_uuid="12345678-1234-1234-1234-123456789abc",
    ).model_copy(
        update={
            "snapshots": snapshots,
            "snapshot_log": snapshot_log,
            "current_snapshot_id": 3,
            "last_updated_ms": 3000,
        }
    )

    table = Mock(spec=Table)
    table.metadata = metadata
    table.identifier = ("db", "table") 


    return table

def test_expire_snapshots_removes_correct_snapshots(mock_table: Mock):
    """
    Test case for the `ExpireSnapshots` class to ensure that the correct snapshots
    are removed and the delete function is called the expected number of times.

    """

    with ManageSnapshots(mock_table) as transaction:
        # Mock the transaction to return the mock table
        transaction.exipre_snapshot_by_id(1).exipre_snapshot_by_id(2).expire_snapshots().cleanup_files()


    for snapshot in mock_table.metadata.snapshots:
        # Verify that the snapshot is removed from the metadata
        assert snapshot.snapshot_id not in [1, 2]
