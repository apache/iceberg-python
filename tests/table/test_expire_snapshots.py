from typing import Any, Dict, Tuple
import pytest
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.io import load_file_io
from pyiceberg.table import Table

import time
from random import randint
from typing import Any, Dict, Optional
import pytest
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.io import load_file_io
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table import Table
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.table.update import TableRequirement, TableUpdate
# Mock definition for CommitTableResponse
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder

class CommitTableResponse:
    def __init__(self, metadata=None, metadata_location='s3://bucket/test/location'):
        if metadata is None:
            # Provide a default TableMetadata object to avoid NoneType errors
            metadata = TableMetadataV2(
                location=metadata_location,
                table_uuid='9c12d441-03fe-4693-9a96-a0705ddf69c1',
                last_updated_ms=1602638573590,
                last_column_id=3,
                schemas=[
                    Schema(
                        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
                        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
                        identifier_field_ids=[1, 2],
                        schema_id=1
                    )
                ],
                current_schema_id=1,
                partition_specs=[
                    PartitionSpec(
                        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0
                    )
                ],
                default_spec_id=0,
                sort_orders=[
                    SortOrder(
                        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
                        order_id=3
                    )
                ],
                default_sort_order_id=3,
                properties={},
                current_snapshot_id=None,
                snapshots=[],
                snapshot_log=[],
                metadata_log=[],
                refs={},
                statistics=[],
                format_version=2,
                last_sequence_number=34
            )
        self.metadata = metadata
        self.metadata_location = metadata_location
        
class MockCatalog(NoopCatalog):
    def commit_table(
        self, table: Table, requirements: Tuple[TableRequirement, ...], updates: Tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        # Mock implementation of commit_table
        return CommitTableResponse()

@pytest.fixture
def example_table_metadata_v2_with_extensive_snapshots() -> Dict[str, Any]:
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

    for i in range(2000):
        snapshot_id = initial_snapshot_id + i
        parent_snapshot_id = snapshot_id - 1 if i > 0 else None
        timestamp_ms = int(time.time() * 1000) - randint(0, 1000000)
        snapshots.append(generate_snapshot(snapshot_id, parent_snapshot_id, timestamp_ms, i))
        snapshot_log.append({"snapshot-id": snapshot_id, "timestamp-ms": timestamp_ms})

    return {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 34,
        "last-updated-ms": 1602638573590,
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
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "default-sort-order-id": 3,
        "sort-orders": [
            {
                "order-id": 3,
                "fields": [
                    {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                    {"transform": "identity", "source-id": 3, "direction": "desc", "null-order": "nulls-last"},  # Adjusted field
                ],
            }
        ],
        "properties": {"read.split.target.size": "134217728"},
        "current-snapshot-id": initial_snapshot_id + 1999,
        "snapshots": snapshots,
        "snapshot-log": snapshot_log,
        "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}],
        "refs": {"test": {"snapshot-id": initial_snapshot_id, "type": "tag", "max-ref-age-ms": 10000000}},
    }

@pytest.fixture
def table_v2_with_extensive_snapshots(example_table_metadata_v2_with_extensive_snapshots: Dict[str, Any]) -> Table:
    table_metadata = TableMetadataV2(**example_table_metadata_v2_with_extensive_snapshots)
    return Table(
        identifier=("database", "table"),
        metadata=table_metadata,
        metadata_location=f"{table_metadata.location}/uuid.metadata.json",
        io=load_file_io(location=f"{table_metadata.location}/uuid.metadata.json"),
        catalog=NoopCatalog("NoopCatalog"),
    )

def test_remove_snapshot(table_v2_with_extensive_snapshots: Table):
    table = table_v2_with_extensive_snapshots
    table.catalog = MockCatalog("MockCatalog")

    # Verify the table has metadata and a current snapshot before proceeding
    assert table.metadata is not None, "Table metadata is None"
    assert table.metadata.current_snapshot_id is not None, "Current snapshot ID is None"

    snapshot_to_expire = 3051729675574599003

    # Ensure the table has snapshots
    assert table.metadata.snapshots is not None, "Snapshots list is None"
    assert len(table.metadata.snapshots) == 2000, f"Expected 2000 snapshots, got {len(table.metadata.snapshots)}"

    assert snapshot_to_expire is not None, "No valid snapshot found to expire"

    # Remove a snapshot using the expire_snapshots API
    table.expire_snapshots().expire_snapshot_id(snapshot_to_expire).commit()

    # Verify the snapshot was removed
    assert snapshot_to_expire not in [snapshot.snapshot_id for snapshot in table.metadata.snapshots], \
        f"Snapshot ID {snapshot_to_expire} was not removed"

    # Use the built-in pytest capsys fixture to capture printed output
    print(f"Snapshot ID {snapshot_to_expire} expired successfully")
    print(f"Number of snapshots after expiry: {table.metadata}")