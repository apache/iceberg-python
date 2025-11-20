#!/usr/bin/env python3
"""
Complete example: Adding pre-calculated equality delete files to PyIceberg tables.

This example demonstrates the full workflow for adding equality delete files
that have been pre-calculated and written to Parquet format.

Usage:
    python example_add_equality_delete.py
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, Record


def add_equality_delete_file(
    table,
    delete_file_path: str,
    equality_field_ids: list[int],
) -> None:
    """
    Add a pre-calculated equality delete file to an Iceberg table.

    Args:
        table: PyIceberg Table object
        delete_file_path: Full path to the Parquet delete file
        equality_field_ids: List of field IDs to use for equality matching
                           (e.g., [1] for field 1, [1,2] for composite key)

    Example:
        >>> table = catalog.load_table("my_db.my_table")
        >>> add_equality_delete_file(
        ...     table,
        ...     "s3://bucket/deletes/delete-001.parquet",
        ...     equality_field_ids=[1]  # Delete by field 1
        ... )
    """
    # Read the Parquet file metadata to get statistics
    input_file = table.io.new_input(delete_file_path)
    parquet_metadata = pq.read_metadata(input_file.open())

    # Get file size
    file_size = len(input_file)
    num_rows = parquet_metadata.num_rows

    print(f"Adding equality delete file:")
    print(f"  Path: {delete_file_path}")
    print(f"  Records: {num_rows}")
    print(f"  Size: {file_size} bytes")
    print(f"  Equality IDs: {equality_field_ids}")

    # Create DataFile object for the delete file
    delete_data_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,  # Mark as equality delete
        file_path=delete_file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),  # Adjust if table is partitioned
        record_count=num_rows,
        file_size_in_bytes=file_size,
        equality_ids=equality_field_ids,  # Field IDs for equality matching

        # Statistics - can be extracted from Parquet metadata for better performance
        column_sizes={},  # Map of field_id -> size in bytes
        value_counts={},  # Map of field_id -> value count
        null_value_counts={},  # Map of field_id -> null count
        nan_value_counts={},
        lower_bounds={},  # Map of field_id -> lower bound (bytes)
        upper_bounds={},  # Map of field_id -> upper bound (bytes)

        _table_format_version=table.format_version,
    )

    # Set the partition spec ID
    delete_data_file.spec_id = table.metadata.default_spec_id

    # Add the delete file using a transaction
    with table.transaction() as txn:
        update_snapshot = txn.update_snapshot()
        with update_snapshot.fast_append() as append_files:
            append_files.append_data_file(delete_data_file)

    print(f"✓ Delete file added successfully")


def example_basic_usage():
    """Example: Basic usage with a local catalog."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, StringType, NestedField
    import tempfile

    print("=" * 70)
    print("EXAMPLE 1: Basic Usage")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse = Path(tmpdir) / "warehouse"
        warehouse.mkdir()

        # Create catalog and table
        catalog = SqlCatalog(
            "demo",
            **{
                "uri": f"sqlite:///{tmpdir}/catalog.db",
                "warehouse": f"file://{warehouse}",
            }
        )
        catalog.create_namespace("db")

        schema = Schema(
            NestedField(1, "user_id", LongType(), required=True),
            NestedField(2, "username", StringType(), required=True),
        )
        table = catalog.create_table("db.users", schema=schema)

        # Add some data
        arrow_schema = pa.schema([
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("username", pa.string(), nullable=False),
        ])
        data = pa.table({
            "user_id": pa.array([1, 2, 3], type=pa.int64()),
            "username": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        }, schema=arrow_schema)
        table.append(data)
        print(f"✓ Created table with {len(table.scan().to_arrow())} rows")

        # Create equality delete file (delete user_id=2)
        delete_data = pa.table({
            "user_id": pa.array([2], type=pa.int64()),
        })
        delete_path = warehouse / "deletes" / "delete-001.parquet"
        delete_path.parent.mkdir(parents=True)
        pq.write_table(delete_data, delete_path)
        print(f"✓ Created delete file at {delete_path}")

        # Add the delete file
        add_equality_delete_file(
            table=table,
            delete_file_path=f"file://{delete_path}",
            equality_field_ids=[1],  # Delete by user_id (field 1)
        )

        # Verify it's tracked
        table = catalog.load_table("db.users")
        snapshot = table.current_snapshot()
        eq_deletes = snapshot.summary.additional_properties.get("total-equality-deletes", "0")
        print(f"✓ Snapshot shows {eq_deletes} equality delete records")


def example_composite_key():
    """Example: Equality delete with composite key (multiple columns)."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, StringType, NestedField
    import tempfile

    print("\n" + "=" * 70)
    print("EXAMPLE 2: Composite Key (Multiple Columns)")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse = Path(tmpdir) / "warehouse"
        warehouse.mkdir()

        catalog = SqlCatalog(
            "demo",
            **{
                "uri": f"sqlite:///{tmpdir}/catalog.db",
                "warehouse": f"file://{warehouse}",
            }
        )
        catalog.create_namespace("db")

        schema = Schema(
            NestedField(1, "tenant_id", LongType(), required=True),
            NestedField(2, "user_id", LongType(), required=True),
            NestedField(3, "name", StringType(), required=True),
        )
        table = catalog.create_table("db.multi_tenant_users", schema=schema)

        # Add data
        arrow_schema = pa.schema([
            pa.field("tenant_id", pa.int64(), nullable=False),
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ])
        data = pa.table({
            "tenant_id": pa.array([1, 1, 2], type=pa.int64()),
            "user_id": pa.array([101, 102, 101], type=pa.int64()),
            "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        }, schema=arrow_schema)
        table.append(data)
        print(f"✓ Created table with {len(table.scan().to_arrow())} rows")

        # Create equality delete file with composite key
        # Delete where tenant_id=1 AND user_id=102 (bob)
        delete_data = pa.table({
            "tenant_id": pa.array([1], type=pa.int64()),
            "user_id": pa.array([102], type=pa.int64()),
        })
        delete_path = warehouse / "deletes" / "delete-composite.parquet"
        delete_path.parent.mkdir(parents=True)
        pq.write_table(delete_data, delete_path)
        print(f"✓ Created delete file with composite key")

        # Add with composite equality_ids
        add_equality_delete_file(
            table=table,
            delete_file_path=f"file://{delete_path}",
            equality_field_ids=[1, 2],  # Match on BOTH tenant_id AND user_id
        )

        # Verify
        table = catalog.load_table("db.multi_tenant_users")
        snapshot = table.current_snapshot()

        # Check the equality_ids in the manifest
        for manifest_file in snapshot.manifests(io=table.io):
            manifest = manifest_file.fetch_manifest_entry(io=table.io)
            for entry in manifest:
                if entry.data_file.content == DataFileContent.EQUALITY_DELETES:
                    print(f"✓ Delete file uses composite key: {entry.data_file.equality_ids}")


def example_multiple_delete_files():
    """Example: Adding multiple delete files in one transaction."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, StringType, NestedField
    import tempfile

    print("\n" + "=" * 70)
    print("EXAMPLE 3: Multiple Delete Files in One Transaction")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse = Path(tmpdir) / "warehouse"
        warehouse.mkdir()

        catalog = SqlCatalog(
            "demo",
            **{
                "uri": f"sqlite:///{tmpdir}/catalog.db",
                "warehouse": f"file://{warehouse}",
            }
        )
        catalog.create_namespace("db")

        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "email", StringType(), required=True),
        )
        table = catalog.create_table("db.users", schema=schema)

        # Add data
        arrow_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=False),
        ])
        data = pa.table({
            "id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "email": pa.array(["a@ex.com", "b@ex.com", "c@ex.com", "d@ex.com"], type=pa.string()),
        }, schema=arrow_schema)
        table.append(data)
        print(f"✓ Created table with {len(table.scan().to_arrow())} rows")

        # Create multiple delete files
        delete_files = []

        # Delete by id
        df1_path = warehouse / "deletes" / "delete-by-id.parquet"
        df1_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"id": pa.array([1], type=pa.int64())}), df1_path)
        delete_files.append((f"file://{df1_path}", [1]))

        # Delete by email
        df2_path = warehouse / "deletes" / "delete-by-email.parquet"
        pq.write_table(pa.table({"email": pa.array(["c@ex.com"], type=pa.string())}), df2_path)
        delete_files.append((f"file://{df2_path}", [2]))

        print(f"✓ Created {len(delete_files)} delete files")

        # Add all delete files in a single transaction
        data_files = []
        for path, eq_ids in delete_files:
            input_file = table.io.new_input(path)
            metadata = pq.read_metadata(input_file.open())

            df = DataFile.from_args(
                content=DataFileContent.EQUALITY_DELETES,
                file_path=path,
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=metadata.num_rows,
                file_size_in_bytes=len(input_file),
                equality_ids=eq_ids,
                column_sizes={},
                value_counts={},
                null_value_counts={},
                nan_value_counts={},
                lower_bounds={},
                upper_bounds={},
                _table_format_version=table.format_version,
            )
            df.spec_id = table.metadata.default_spec_id
            data_files.append(df)

        with table.transaction() as txn:
            update_snapshot = txn.update_snapshot()
            with update_snapshot.fast_append() as append:
                for df in data_files:
                    append.append_data_file(df)

        print(f"✓ Added all delete files in single transaction")

        # Verify
        table = catalog.load_table("db.users")
        snapshot = table.current_snapshot()
        eq_deletes = snapshot.summary.additional_properties.get("total-equality-deletes", "0")
        print(f"✓ Total equality deletes: {eq_deletes}")


if __name__ == "__main__":
    # Run all examples
    example_basic_usage()
    example_composite_key()
    example_multiple_delete_files()

    print("\n" + "=" * 70)
    print("✅ All examples completed successfully!")
    print("=" * 70)
    print("\nNote: Reading tables with equality deletes will fail with:")
    print("  ValueError: PyIceberg does not yet support equality deletes")
    print("\nBut the write path works perfectly!")
