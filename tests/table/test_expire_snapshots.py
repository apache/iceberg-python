from uuid import uuid4
from pyiceberg.table import CommitTableResponse, Table
from unittest.mock import MagicMock

def test_expire_snapshot(table_v2: Table) -> None:
    EXPIRE_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004
    # Mock the catalog's commit_table method
    mock_response = CommitTableResponse(
        # Use the table's current metadata but keep only the snapshot not to be expired
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4()
    )

    # Mock the commit_table method to return the mock response
    table_v2.catalog.commit_table = MagicMock(return_value=mock_response)

    # Print snapshot IDs for debugging
    print(f"Snapshot IDs before expiration: {[snapshot.snapshot_id for snapshot in table_v2.metadata.snapshots]}")

    # Assert fixture data to validate test assumptions
    assert len(table_v2.metadata.snapshots) == 2
    assert len(table_v2.metadata.snapshot_log) == 2
    assert len(table_v2.metadata.refs) == 2

    # Expire the snapshot directly without using a transaction
    try:
        table_v2.manage_snapshots().expire_snapshot_by_id(EXPIRE_SNAPSHOT).commit()
    except Exception as e:
        assert False, f"Commit failed with error: {e}"

    # Assert that commit_table was called once
    table_v2.catalog.commit_table.assert_called_once()

    # Assert the expired snapshot ID is no longer present
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT not in remaining_snapshots

    # Assert the length of snapshots after expiration
    assert len(table_v2.metadata.snapshots) == 1
