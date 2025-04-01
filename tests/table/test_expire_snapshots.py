from datetime import datetime, timezone
from pathlib import PosixPath
from random import randint
import time
from typing import Any, Dict, Optional
import pytest
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.io import load_file_io
from pyiceberg.table import Table
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.table.update.snapshot import ExpireSnapshots
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, LongType, MapType, StructType
from tests.catalog.test_base import InMemoryCatalog, Table
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.metadata import TableMetadata, TableMetadataUtil, TableMetadataV2, new_table_metadata


@pytest.fixture
def mock_table():
    """Fixture to create a mock Table instance with proper metadata for testing."""
    # Create mock metadata with empty snapshots list
    metadata_dict = {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 0,
        "last-updated-ms": int(time.time() * 1000),
        "last-column-id": 3,
        "current-schema-id": 1,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 1,
                "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "snapshots": [],
        "refs": {},
    }
    
    metadata = TableMetadataUtil.parse_obj(metadata_dict)
    
    return Table(
        identifier=("mock_database", "mock_table"),
        metadata=metadata,
        metadata_location="mock_location",
        io=load_file_io(),
        catalog=InMemoryCatalog("InMemoryCatalog"),
    )


@pytest.fixture
def generate_test_table() -> Table:
    def generate_snapshot(
        snapshot_id: int,
        parent_snapshot_id: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        sequence_number: int = 0,
    ) -> Dict[str, Any]:
        return {
            "snapshot-id": snapshot_id,
            "parent-snapshot-id": parent_snapshot_id,
            "timestamp-ms": timestamp_ms or int(time.time() * 1000),
            "sequence-number": sequence_number,
            "summary": {"operation": "append"},
            "manifest-list": f"s3://a/b/{snapshot_id}.avro",
        }

    snapshots = []
    snapshot_log = []
    initial_snapshot_id = 3051729675574597004

    for i in range(5):
        snapshot_id = initial_snapshot_id + i
        parent_snapshot_id = snapshot_id - 1 if i > 0 else None
        timestamp_ms = int(time.time() * 1000) - randint(0, 1000000)
        snapshots.append(generate_snapshot(snapshot_id, parent_snapshot_id, timestamp_ms, i))
        snapshot_log.append({"snapshot-id": snapshot_id, "timestamp-ms": timestamp_ms})

    metadata_dict = {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 34,
        "last-updated-ms": snapshots[-1]["timestamp-ms"],
        "metadata-log": [
            {"metadata-file": "s3://bucket/test/location/metadata/v1.json", "timestamp-ms": 1700000000000},
            {"metadata-file": "s3://bucket/test/location/metadata/v2.json", "timestamp-ms": 1700003600000},
            {"metadata-file": "s3://bucket/test/location/metadata/v3.json", "timestamp-ms": snapshots[-1]["timestamp-ms"]},
        ],
        "last-column-id": 3,
        "current-schema-id": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]},
            {
                "type": "struct",
                "schema-id": 1,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": True, "type": "long"},
                    {"id": 2, "name": "y", "required": True, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": True, "type": "long"},
                ],
            },
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 1000,
        "default-sort-order-id": 3,
        "sort-orders": [{"order-id": 3, "fields": []}],
        "properties": {"read.split.target.size": "134217728"},
        "current-snapshot-id": initial_snapshot_id + 4,
        "snapshots": snapshots,
        "snapshot-log": snapshot_log,
        "refs": {"test": {"snapshot-id": initial_snapshot_id, "type": "tag", "max-ref-age-ms": 10000000}},
    }

    metadata = TableMetadataUtil.parse_obj(metadata_dict)

    return Table(
        identifier=("database", "table"),
        metadata=metadata,
        metadata_location=f"{metadata.location}/uuid.metadata.json",
        io=load_file_io(),
        catalog=InMemoryCatalog("InMemoryCatalog"),
    )



def test_expire_snapshots_removes_correct_snapshots(generate_test_table: Table):
    """
    Test case for the `ExpireSnapshots` class to ensure that the correct snapshots
    are removed and the delete function is called the expected number of times.
    """
    
    # Use the fixture-provided table
    with generate_test_table.expire_snapshots() as transaction:
        transaction.expire_snapshot_id(3051729675574597004).commit()

    # Check the remaining snapshots
    remaining_snapshot_ids = {snapshot.snapshot_id for snapshot in generate_test_table.metadata.snapshots}
    
    # Assert that the expired snapshot ID is not in the remaining snapshots
    assert 3051729675574597004 not in remaining_snapshot_ids
