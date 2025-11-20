"""
Test for adding pre-calculated equality delete files to Iceberg tables.

This test demonstrates the write path for equality deletes without requiring
the read path to be implemented.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, Record
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType


def test_add_equality_delete_file_via_transaction():
    """
    Test adding a pre-calculated equality delete file to an Iceberg table.

    This test demonstrates:
    1. Creating a table with data
    2. Writing an equality delete file (Parquet) with specific columns
    3. Creating a DataFile object with equality_ids set
    4. Adding the delete file via transaction using UpdateSnapshot API
    5. Verifying the delete file is correctly tracked in table metadata

    The read path is NOT tested since PyIceberg doesn't support reading
    equality deletes yet.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse_path = Path(tmpdir) / "warehouse"
        warehouse_path.mkdir()

        # Create catalog
        catalog = SqlCatalog(
            "test_catalog",
            **{
                "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
            }
        )

        # Create namespace and table
        catalog.create_namespace("test_db")

        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "age", IntegerType(), required=False),
        )

        table = catalog.create_table("test_db.test_table", schema=schema)

        # Add data
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

        assert len(table.scan().to_arrow()) == 5

        # Create equality delete file
        # This delete file will delete rows where id=2 or id=4
        delete_data = pa.table({
            "id": pa.array([2, 4], type=pa.int64()),
        })

        delete_file_path = warehouse_path / "deletes" / "eq-delete-001.parquet"
        delete_file_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(delete_data, delete_file_path)

        # Get file metadata
        file_size = delete_file_path.stat().st_size
        parquet_metadata = pq.read_metadata(delete_file_path)
        num_rows = parquet_metadata.num_rows

        # Create DataFile for the equality delete
        delete_data_file = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,  # Mark as equality delete
            file_path=f"file://{delete_file_path}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=num_rows,
            file_size_in_bytes=file_size,
            equality_ids=[1],  # Field ID 1 = "id" column
            column_sizes={1: file_size},
            value_counts={1: num_rows},
            null_value_counts={1: 0},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            _table_format_version=table.format_version,
        )
        delete_data_file.spec_id = table.metadata.default_spec_id

        # Verify equality_ids is set correctly
        assert delete_data_file.equality_ids == [1]
        assert delete_data_file.content == DataFileContent.EQUALITY_DELETES

        # Add delete file using transaction
        with table.transaction() as txn:
            update_snapshot = txn.update_snapshot()
            with update_snapshot.fast_append() as append_files:
                append_files.append_data_file(delete_data_file)

        # Verify delete file is tracked in metadata
        table = catalog.load_table("test_db.test_table")
        latest_snapshot = table.current_snapshot()

        assert latest_snapshot is not None
        assert "total-equality-deletes" in latest_snapshot.summary.additional_properties
        assert latest_snapshot.summary.additional_properties["total-equality-deletes"] == "2"

        # Verify delete file appears in manifests
        delete_file_found = False
        for manifest_file in latest_snapshot.manifests(io=table.io):
            manifest = manifest_file.fetch_manifest_entry(io=table.io)
            for entry in manifest:
                if entry.data_file.content == DataFileContent.EQUALITY_DELETES:
                    delete_file_found = True
                    assert entry.data_file.equality_ids == [1]
                    assert entry.data_file.record_count == 2
                    break

        assert delete_file_found, "Equality delete file not found in manifests"

        # Verify that scanning raises an error (read path not supported yet)
        with pytest.raises(ValueError, match="PyIceberg does not yet support equality deletes"):
            table.scan().to_arrow()


def test_add_multiple_equality_delete_files_with_different_equality_ids():
    """
    Test adding multiple equality delete files with different equality_ids.

    This demonstrates that you can have multiple equality delete files,
    each using different columns for equality matching.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse_path = Path(tmpdir) / "warehouse"
        warehouse_path.mkdir()

        catalog = SqlCatalog(
            "test_catalog",
            **{
                "uri": f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
            }
        )

        catalog.create_namespace("test_db")

        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "age", IntegerType(), required=False),
        )

        table = catalog.create_table("test_db.test_table", schema=schema)

        # Add data
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

        # Create first equality delete file (delete by id)
        delete_by_id = pa.table({
            "id": pa.array([2], type=pa.int64()),
        })
        delete_file_1 = warehouse_path / "deletes" / "delete-by-id.parquet"
        delete_file_1.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(delete_by_id, delete_file_1)

        # Create second equality delete file (delete by name)
        delete_by_name = pa.table({
            "name": pa.array(["David"], type=pa.string()),
        })
        delete_file_2 = warehouse_path / "deletes" / "delete-by-name.parquet"
        pq.write_table(delete_by_name, delete_file_2)

        # Create third equality delete file (delete by id AND name - composite key)
        delete_by_id_and_name = pa.table({
            "id": pa.array([5], type=pa.int64()),
            "name": pa.array(["Eve"], type=pa.string()),
        })
        delete_file_3 = warehouse_path / "deletes" / "delete-by-id-and-name.parquet"
        pq.write_table(delete_by_id_and_name, delete_file_3)

        # Create DataFile objects with different equality_ids
        delete_files = []

        # Delete file 1: equality on field 1 (id)
        df1 = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=f"file://{delete_file_1}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=delete_file_1.stat().st_size,
            equality_ids=[1],  # Only field 1 (id)
            column_sizes={},
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            _table_format_version=table.format_version,
        )
        df1.spec_id = table.metadata.default_spec_id
        delete_files.append(df1)

        # Delete file 2: equality on field 2 (name)
        df2 = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=f"file://{delete_file_2}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=delete_file_2.stat().st_size,
            equality_ids=[2],  # Only field 2 (name)
            column_sizes={},
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            _table_format_version=table.format_version,
        )
        df2.spec_id = table.metadata.default_spec_id
        delete_files.append(df2)

        # Delete file 3: equality on fields 1 AND 2 (id, name) - composite
        df3 = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=f"file://{delete_file_3}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=delete_file_3.stat().st_size,
            equality_ids=[1, 2],  # Both fields (composite key)
            column_sizes={},
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            _table_format_version=table.format_version,
        )
        df3.spec_id = table.metadata.default_spec_id
        delete_files.append(df3)

        # Add all delete files in a single transaction
        with table.transaction() as txn:
            update_snapshot = txn.update_snapshot()
            with update_snapshot.fast_append() as append_files:
                for delete_file in delete_files:
                    append_files.append_data_file(delete_file)

        # Verify all delete files are tracked
        table = catalog.load_table("test_db.test_table")
        latest_snapshot = table.current_snapshot()

        assert latest_snapshot is not None
        # Total 3 delete records
        assert latest_snapshot.summary.additional_properties["total-equality-deletes"] == "3"

        # Verify all three delete files with different equality_ids
        found_equality_ids = []
        for manifest_file in latest_snapshot.manifests(io=table.io):
            manifest = manifest_file.fetch_manifest_entry(io=table.io)
            for entry in manifest:
                if entry.data_file.content == DataFileContent.EQUALITY_DELETES:
                    found_equality_ids.append(entry.data_file.equality_ids)

        assert len(found_equality_ids) == 3
        assert [1] in found_equality_ids
        assert [2] in found_equality_ids
        assert [1, 2] in found_equality_ids


if __name__ == "__main__":
    test_add_equality_delete_file_via_transaction()
    print("✅ Test 1 passed!")
    test_add_multiple_equality_delete_files_with_different_equality_ids()
    print("✅ Test 2 passed!")
