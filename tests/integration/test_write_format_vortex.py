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

"""Integration tests for write format configuration with Vortex."""

from pathlib import Path
from typing import Generator

import pyarrow as pa
import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

# Check if vortex is available
try:
    import importlib.util
    VORTEX_AVAILABLE = importlib.util.find_spec("vortex") is not None
except ImportError:
    VORTEX_AVAILABLE = False

pytestmark = pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex-data package not installed")


@pytest.fixture(scope="function")
def vortex_catalog(tmp_path: Path) -> Generator[SqlCatalog, None, None]:
    """Create a SQL catalog for testing write format configuration."""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    catalog = SqlCatalog(
        name="test_write_format_catalog",
        uri=f"sqlite:///{tmp_path}/write_format_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )

    # Create the default namespace
    catalog.create_namespace("default")

    yield catalog

    # Cleanup
    try:
        catalog.close()
    except Exception:
        pass


@pytest.fixture
def test_schema() -> Schema:
    """Simple schema for testing write formats."""
    return Schema(
        NestedField(1, "id", LongType(), required=False),  # Make optional to match data
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "age", LongType(), required=False),  # Use LongType to match PyArrow
        NestedField(4, "active", BooleanType(), required=False),
        NestedField(5, "score", DoubleType(), required=False),
    )


@pytest.fixture
def test_data() -> pa.Table:
    """Test data as PyArrow table."""
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": pa.array([25, 30, 35, 40, 45], type=pa.int32()),
            "active": [True, False, True, True, False],
            "score": [95.5, 87.2, 91.8, 88.0, 92.3],
        }
    )


@pytest.mark.integration
class TestWriteFormatConfiguration:
    """Test write format configuration using table properties."""

    def test_default_parquet_format(
        self, vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table
    ) -> None:
        """Test that default format is Parquet when no write format is specified."""
        table_name = "default.test_default_parquet"
        
        # Create table without specifying write format
        table = vortex_catalog.create_table(
            identifier=table_name,
            schema=test_schema,
        )
        
        # Write data
        table.append(test_data)
        
        # Verify that data files are in Parquet format
        data_files = []
        for snapshot in table.snapshots():
            for manifest_list in snapshot.manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file:
                        data_files.append(manifest_entry.data_file)
        
        assert len(data_files) > 0
        for data_file in data_files:
            assert data_file.file_format == FileFormat.PARQUET
            assert data_file.file_path.endswith(".parquet")

    def test_vortex_write_format_via_transaction(
        self, vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table
    ) -> None:
        """Test setting Vortex as write format using transaction properties."""
        table_name = "default.test_vortex_write_format"
        
        # Create table
        table = vortex_catalog.create_table(
            identifier=table_name,
            schema=test_schema,
        )
        
        # Set write format to Vortex using transaction
        transaction = table.transaction()
        transaction.set_properties({
            TableProperties.WRITE_FORMAT_DEFAULT: "vortex"
        })
        transaction.commit_transaction()
        
        # Refresh table to get updated properties
        table = vortex_catalog.load_table(table_name)
        
        # Verify the property was set
        assert table.properties.get(TableProperties.WRITE_FORMAT_DEFAULT) == "vortex"
        
        # Write data
        table.append(test_data)
        
        # Verify that data files are in Vortex format
        data_files = []
        for snapshot in table.snapshots():
            for manifest_list in snapshot.manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file:
                        data_files.append(manifest_entry.data_file)
        
        assert len(data_files) > 0
        for data_file in data_files:
            assert data_file.file_format == FileFormat.VORTEX
            assert data_file.file_path.endswith(".vortex")

    def test_parquet_write_format_explicit(
        self, vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table
    ) -> None:
        """Test explicitly setting Parquet as write format."""
        table_name = "default.test_parquet_explicit"
        
        # Create table
        table = vortex_catalog.create_table(
            identifier=table_name,
            schema=test_schema,
        )
        
        # Set write format to Parquet explicitly using transaction
        transaction = table.transaction()
        transaction.set_properties({
            TableProperties.WRITE_FORMAT_DEFAULT: "parquet"
        })
        transaction.commit_transaction()
        
        # Refresh table to get updated properties
        table = vortex_catalog.load_table(table_name)
        
        # Verify the property was set
        assert table.properties.get(TableProperties.WRITE_FORMAT_DEFAULT) == "parquet"
        
        # Write data
        table.append(test_data)
        
        # Verify that data files are in Parquet format
        data_files = []
        for snapshot in table.snapshots():
            for manifest_list in snapshot.manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file:
                        data_files.append(manifest_entry.data_file)
        
        assert len(data_files) > 0
        for data_file in data_files:
            assert data_file.file_format == FileFormat.PARQUET
            assert data_file.file_path.endswith(".parquet")

    def test_invalid_write_format_raises_error(
        self, vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table
    ) -> None:
        """Test that invalid write format raises appropriate error."""
        table_name = "default.test_invalid_format"
        
        # Create table
        table = vortex_catalog.create_table(
            identifier=table_name,
            schema=test_schema,
        )
        
        # Set write format to invalid value using transaction
        transaction = table.transaction()
        transaction.set_properties({
            TableProperties.WRITE_FORMAT_DEFAULT: "invalid_format"
        })
        transaction.commit_transaction()
        
        # Refresh table to get updated properties
        table = vortex_catalog.load_table(table_name)
        
        # Attempt to write data should raise ValueError
        with pytest.raises(ValueError, match="Unsupported write format: invalid_format"):
            table.append(test_data)

    def test_vortex_read_back_data(
        self, vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table
    ) -> None:
        """Test that data written in Vortex format can be read back correctly."""
        table_name = "default.test_vortex_read_back"
        
        # Create table and set write format to Vortex
        table = vortex_catalog.create_table(
            identifier=table_name,
            schema=test_schema,
        )
        
        transaction = table.transaction()
        transaction.set_properties({
            TableProperties.WRITE_FORMAT_DEFAULT: "vortex"
        })
        transaction.commit_transaction()
        
        # Refresh table
        table = vortex_catalog.load_table(table_name)
        
        # Write data
        table.append(test_data)
        
        # Read data back and verify
        result = table.scan().to_arrow()

        assert len(result) == len(test_data)

        # Debug: Print result columns to understand the issue
        print(f"Result columns: {result.column_names}")
        print(f"Expected columns: {test_data.column_names}")
        
        # Verify content matches
        result_dict = result.to_pydict()
        expected_dict = test_data.to_pydict()

        # Check if all columns exist before comparing
        for col in expected_dict.keys():
            assert col in result_dict, f"Column {col} missing from result"
            assert result_dict[col] == expected_dict[col], f"Data mismatch in column {col}"
        assert result_dict["name"] == expected_dict["name"]
        assert result_dict["age"] == expected_dict["age"]
        assert result_dict["active"] == expected_dict["active"]
        
        # Handle floating point precision
        for i, (result_score, expected_score) in enumerate(zip(result_dict["score"], expected_dict["score"])):
            assert abs(result_score - expected_score) < 1e-10, f"Score mismatch at index {i}"

    def test_change_write_format_affects_new_writes_only(
        self, vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table
    ) -> None:
        """Test that changing write format only affects new writes, not existing files."""
        table_name = "default.test_format_change"
        
        # Create table (default Parquet format)
        table = vortex_catalog.create_table(
            identifier=table_name,
            schema=test_schema,
        )
        
        # Write first batch (should be Parquet)
        table.append(test_data)
        
        # Change write format to Vortex
        transaction = table.transaction()
        transaction.set_properties({
            TableProperties.WRITE_FORMAT_DEFAULT: "vortex"
        })
        transaction.commit_transaction()
        
        # Refresh table
        table = vortex_catalog.load_table(table_name)
        
        # Write second batch (should be Vortex)
        table.append(test_data)
        
        # Collect all unique data files (avoid duplicates from multiple snapshots)
        data_files = set()
        for snapshot in table.snapshots():
            for manifest_list in snapshot.manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file:
                        data_files.add(manifest_entry.data_file.file_path)
        
        # Convert back to list of data file objects for format checking
        all_data_files = []
        for snapshot in table.snapshots():
            for manifest_list in snapshot.manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file and manifest_entry.data_file.file_path in data_files:
                        all_data_files.append(manifest_entry.data_file)
                        data_files.remove(manifest_entry.data_file.file_path)  # Remove to avoid duplicates
        
        # Should have files in both formats
        assert len(all_data_files) == 2
        
        parquet_files = [df for df in all_data_files if df.file_format == FileFormat.PARQUET]
        vortex_files = [df for df in all_data_files if df.file_format == FileFormat.VORTEX]
        
        assert len(parquet_files) == 1
        assert len(vortex_files) == 1
        
        # Verify data can still be read (both formats together)
        result = table.scan().to_arrow()
        assert len(result) == len(test_data) * 2  # Two batches of data
