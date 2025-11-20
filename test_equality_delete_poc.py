#!/usr/bin/env python3
"""
Proof-of-concept test for writing pre-calculated equality delete files to an Iceberg table.

This demonstrates how to:
1. Create a table with data
2. Write an equality delete file (Parquet)
3. Add the delete file to the table using a transaction
4. Verify the delete file is tracked in table metadata
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, Record
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType


def test_add_equality_delete_file_via_transaction():
    """Test adding a pre-calculated equality delete file to a table."""

    # Create a temporary directory for our test
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse_path = Path(tmpdir) / "warehouse"
        warehouse_path.mkdir()

        # Create a SQL catalog (in-memory SQLite)
        catalog = SqlCatalog(
            "test_catalog",
            **{
                "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
            }
        )

        # Create namespace and table
        catalog.create_namespace("test_db")

        # Define table schema
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "age", IntegerType(), required=False),
        )

        # Create table
        table = catalog.create_table("test_db.test_table", schema=schema)

        # Add some data to the table
        # Create PyArrow table with matching schema (required fields must be non-nullable)
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("age", pa.int32(), nullable=True),
        ])
        data = pa.table({
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"], type=pa.string()),
            "age": pa.array([25, 30, 35, 40, 45], type=pa.int32()),
        }, schema=arrow_schema)
        table.append(data)

        # Verify we have data
        assert len(table.scan().to_arrow()) == 5
        print(f"✓ Table created with 5 rows")

        # Create an equality delete file (Parquet)
        # This file contains rows to delete based on equality of certain columns
        # We'll delete rows where id=2 or id=4 (Bob and David)
        delete_data = pa.table({
            "id": pa.array([2, 4], type=pa.int64()),  # Delete records with these IDs
        })

        # Write the delete file to the warehouse
        delete_file_path = warehouse_path / "deletes" / "equality-delete-001.parquet"
        delete_file_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(delete_data, delete_file_path)

        # Get file size for metadata
        file_size = delete_file_path.stat().st_size

        # Read the Parquet metadata to extract statistics
        parquet_metadata = pq.read_metadata(delete_file_path)
        num_rows = parquet_metadata.num_rows

        print(f"✓ Equality delete file created: {delete_file_path}")
        print(f"  - Rows: {num_rows}")
        print(f"  - Size: {file_size} bytes")

        # Create a DataFile object for the equality delete file
        # The key difference from a regular data file is:
        # 1. content=DataFileContent.EQUALITY_DELETES
        # 2. equality_ids=[1] - specifies field ID 1 (id column) is used for equality matching
        delete_data_file = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,  # Mark as equality delete
            file_path=f"file://{delete_file_path}",
            file_format=FileFormat.PARQUET,
            partition=Record(),  # Unpartitioned table
            record_count=num_rows,
            file_size_in_bytes=file_size,

            # Equality IDs: specifies which field(s) to use for matching
            # Field ID 1 corresponds to the "id" column in our schema
            equality_ids=[1],  # This is the key field for equality deletes!

            # Column statistics (minimal for this POC)
            column_sizes={1: file_size},  # Rough estimate
            value_counts={1: num_rows},
            null_value_counts={1: 0},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},

            # Table format version
            _table_format_version=table.format_version,
        )

        # Set the partition spec ID
        delete_data_file.spec_id = table.metadata.default_spec_id

        print(f"✓ DataFile object created with equality_ids={delete_data_file.equality_ids}")

        # Add the delete file to the table using a transaction
        # This is the key part - using UpdateSnapshot API to add delete files
        with table.transaction() as txn:
            update_snapshot = txn.update_snapshot()

            # Use fast_append to add the delete file
            with update_snapshot.fast_append() as append_files:
                # append_data_file works for both data files AND delete files!
                append_files.append_data_file(delete_data_file)

        print(f"✓ Transaction committed successfully")

        # Reload table to see the changes
        table = catalog.load_table("test_db.test_table")

        # Verify the delete file is tracked in metadata
        latest_snapshot = table.current_snapshot()
        assert latest_snapshot is not None

        print(f"\n✓ Latest snapshot ID: {latest_snapshot.snapshot_id}")
        print(f"  Summary: {latest_snapshot.summary}")

        # Check if equality deletes are tracked
        if "total-equality-deletes" in latest_snapshot.summary.additional_properties:
            eq_deletes = latest_snapshot.summary.additional_properties["total-equality-deletes"]
            print(f"  Total equality deletes: {eq_deletes}")

        # Check delete files in manifests
        delete_file_count = 0
        for manifest_file in latest_snapshot.manifests(io=table.io):
            manifest = manifest_file.fetch_manifest_entry(io=table.io)
            for entry in manifest:
                if entry.data_file.content == DataFileContent.EQUALITY_DELETES:
                    delete_file_count += 1
                    print(f"\n✓ Found equality delete file in manifest:")
                    print(f"  - Path: {entry.data_file.file_path}")
                    print(f"  - Equality IDs: {entry.data_file.equality_ids}")
                    print(f"  - Record count: {entry.data_file.record_count}")

        assert delete_file_count > 0, "No equality delete files found in manifests!"

        print(f"\n✅ SUCCESS: Equality delete file successfully added to table!")
        print(f"   Note: Reading will fail because PyIceberg doesn't support reading equality deletes yet.")
        print(f"   But the write path works and the metadata is correctly stored.")

        # Try to scan and see what happens
        print(f"\n📊 Attempting to scan the table (expecting error about equality deletes)...")
        try:
            result = table.scan().to_arrow()
            print(f"   Unexpected: Scan succeeded with {len(result)} rows")
        except Exception as e:
            print(f"   ✓ Expected error occurred: {type(e).__name__}")
            print(f"     Message: {str(e)[:100]}")


if __name__ == "__main__":
    test_add_equality_delete_file_via_transaction()
