# pylint:disable=redefined-outer-name
# pylint:disable=redefined-outer-name
from unittest.mock import Mock
import pytest

from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.update.snapshot import ExpireSnapshots

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
    Args:
        mock_table (Mock): A mock object representing the table.
    Test Steps:
    1. Create a mock delete function and a mock transaction.
    2. Instantiate the `ExpireSnapshots` class with the mock transaction.
    3. Configure the `ExpireSnapshots` instance to expire snapshots with IDs 1 and 2,
       and set the delete function to the mock delete function.
    4. Commit the changes using the `_commit` method with the mock table's metadata.
    5. Validate that the mock delete function is called for the correct snapshots.
    6. Verify that the delete function is called exactly twice.
    7. Ensure that the updated metadata returned by `_commit` is not `None`.
    """
    mock_delete_func = Mock()
    mock_transaction = Mock()

    expire_snapshots = ExpireSnapshots(mock_transaction)
    expire_snapshots \
        .expire_snapshot_id(1) \
        .expire_snapshot_id(2) \
        .delete_with(mock_delete_func)

    updated_metadata = expire_snapshots._commit(mock_table.metadata)

    # Validate delete calls
    mock_delete_func.assert_any_call(mock_table.return_value.snapshots[0])
    mock_delete_func.assert_any_call(mock_table.metadata.snapshots[1])
    assert mock_delete_func.call_count == 2

    # Verify updated metadata returned
    assert updated_metadata is not None
