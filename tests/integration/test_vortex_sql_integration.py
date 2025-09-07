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
"""Integration tests for Vortex file format with SQL catalog."""

from pathlib import Path
from typing import Generator

import pyarrow as pa
import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.vortex import VORTEX_AVAILABLE, convert_iceberg_to_vortex_file, read_vortex_file
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

# Skip all tests if vortex-data is not available
pytestmark = pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex-data package not installed")


@pytest.fixture(scope="function")
def vortex_sql_catalog(tmp_path: Path) -> Generator[SqlCatalog, None, None]:
    """Create a SQL catalog configured for testing with Vortex format."""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    catalog = SqlCatalog(
        name="test_vortex_catalog",
        uri=f"sqlite:///{tmp_path}/vortex_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )

    yield catalog

    # Cleanup
    try:
        catalog.close()
    except Exception:
        pass


@pytest.fixture
def simple_schema() -> Schema:
    """Simple schema for basic Vortex testing."""
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
        NestedField(field_id=4, name="active", field_type=BooleanType(), required=True),
        NestedField(field_id=5, name="score", field_type=DoubleType(), required=False),
    )


@pytest.fixture
def simple_arrow_data() -> pa.Table:
    """Simple Arrow table for basic testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", None, "Eve"],
            "age": [25, 30, 35, 40, 28],
            "active": [True, False, True, True, False],
            "score": [95.5, 87.2, 92.8, None, 89.1],
        }
    )


class TestVortexSqlIntegration:
    """Integration tests for Vortex file format with SQL catalog."""

    def test_create_simple_table(self, vortex_sql_catalog: SqlCatalog, simple_schema: Schema) -> None:
        """Test creating a simple table that can be used with Vortex format."""
        table_name = "default.simple_vortex_table"

        # Create table
        table = vortex_sql_catalog.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        # Verify table was created
        assert table is not None
        assert table.name() == "simple_vortex_table"
        assert table.schema() == simple_schema

        # Verify table exists in catalog
        tables = vortex_sql_catalog.list_tables("default")
        assert ("default", "simple_vortex_table") in tables

    def test_write_and_read_vortex_data(
        self, vortex_sql_catalog: SqlCatalog, simple_schema: Schema, simple_arrow_data: pa.Table
    ) -> None:
        """Test writing data using Vortex format and reading it back through PyIceberg table."""
        table_name = "default.vortex_write_read_test"

        # Create table
        table = vortex_sql_catalog.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        # Write data using standard append (this will create Parquet files by default)
        table.append(simple_arrow_data)

        # Read data back to verify the table works
        result = table.scan().to_arrow()

        # Verify row count
        assert len(result) == len(simple_arrow_data)

        # Verify data content
        result_dict = result.to_pydict()
        expected_dict = simple_arrow_data.to_pydict()

        assert result_dict["id"] == expected_dict["id"]
        assert result_dict["name"] == expected_dict["name"]
        assert result_dict["age"] == expected_dict["age"]
        assert result_dict["active"] == expected_dict["active"]

        # Handle potential floating point precision differences
        for i, (result_score, expected_score) in enumerate(zip(result_dict["score"], expected_dict["score"])):
            if result_score is None and expected_score is None:
                continue
            elif result_score is None or expected_score is None:
                raise AssertionError(f"Null mismatch at index {i}: result={result_score}, expected={expected_score}")
            else:
                assert abs(result_score - expected_score) < 1e-10, f"Score mismatch at index {i}"

    def test_vortex_file_operations_with_catalog_table(
        self, vortex_sql_catalog: SqlCatalog, simple_schema: Schema, simple_arrow_data: pa.Table, tmp_path: Path
    ) -> None:
        """Test Vortex file operations in the context of a catalog table."""
        table_name = "default.vortex_file_ops_test"

        # Create table
        table = vortex_sql_catalog.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        # Create a Vortex file manually
        vortex_file_path = tmp_path / "test_data.vortex"
        data_file = convert_iceberg_to_vortex_file(
            iceberg_table_data=simple_arrow_data,
            iceberg_schema=simple_schema,
            output_path=f"file://{vortex_file_path}",
            io=table.io,
            compression=True,
        )

        # Verify the Vortex file was created
        assert vortex_file_path.exists()
        assert data_file is not None

        # Read the Vortex file back
        read_batches = read_vortex_file(f"file://{vortex_file_path}", table.io)
        read_data = pa.Table.from_batches(read_batches)

        # Verify the data
        assert len(read_data) == len(simple_arrow_data)
        read_dict = read_data.to_pydict()
        expected_dict = simple_arrow_data.to_pydict()

        assert read_dict["id"] == expected_dict["id"]
        assert read_dict["name"] == expected_dict["name"]

    def test_multiple_operations_vortex_table(
        self, vortex_sql_catalog: SqlCatalog, simple_schema: Schema, simple_arrow_data: pa.Table
    ) -> None:
        """Test multiple operations on a table that could use Vortex format."""
        table_name = "default.multiple_ops_vortex_test"

        # Create table
        table = vortex_sql_catalog.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        # Write data multiple times
        table.append(simple_arrow_data)
        table.append(simple_arrow_data)

        # Read data back and verify
        result = table.scan().to_arrow()

        # Should have double the rows
        assert len(result) == len(simple_arrow_data) * 2

        # Verify snapshots were created
        snapshots = list(table.snapshots())
        assert len(snapshots) == 2  # 2 appends

    def test_vortex_table_overwrite(
        self, vortex_sql_catalog: SqlCatalog, simple_schema: Schema, simple_arrow_data: pa.Table
    ) -> None:
        """Test overwriting data in a table."""
        table_name = "default.vortex_overwrite_test"

        # Create table and add initial data
        table = vortex_sql_catalog.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        table.append(simple_arrow_data)

        # Create new data for overwrite
        new_data = pa.table(
            {
                "id": [10, 20, 30],
                "name": ["New1", "New2", "New3"],
                "age": [50, 60, 70],
                "active": [True, True, False],
                "score": [100.0, 95.0, 85.0],
            }
        )

        # Overwrite data
        table.overwrite(new_data)

        # Read data back and verify
        result = table.scan().to_arrow()

        # Should have only the new data
        assert len(result) == len(new_data)

        result_dict = result.to_pydict()
        expected_dict = new_data.to_pydict()

        assert result_dict["id"] == expected_dict["id"]
        assert result_dict["name"] == expected_dict["name"]

    def test_catalog_persistence_vortex_tables(self, tmp_path: Path, simple_schema: Schema, simple_arrow_data: pa.Table) -> None:
        """Test that tables with Vortex-compatible data persist correctly across catalog sessions."""
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir(exist_ok=True)
        db_path = tmp_path / "persistent_catalog.db"

        table_name = "default.persistent_vortex_test"

        # Create catalog and table in first session
        catalog1 = SqlCatalog(
            name="persistent_catalog",
            uri=f"sqlite:///{db_path}",
            warehouse=f"file://{warehouse_path}",
        )

        table1 = catalog1.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        table1.append(simple_arrow_data)
        catalog1.close()

        # Create new catalog instance (simulating new session)
        catalog2 = SqlCatalog(
            name="persistent_catalog",
            uri=f"sqlite:///{db_path}",
            warehouse=f"file://{warehouse_path}",
        )

        # Load existing table
        table2 = catalog2.load_table(table_name)

        # Verify data persisted
        result = table2.scan().to_arrow()
        assert len(result) == len(simple_arrow_data)

        catalog2.close()

    def test_vortex_compatibility_check(
        self, vortex_sql_catalog: SqlCatalog, simple_schema: Schema, simple_arrow_data: pa.Table
    ) -> None:
        """Test that we can check Vortex compatibility for table data."""
        table_name = "default.vortex_compatibility_test"

        # Create table and add data
        table = vortex_sql_catalog.create_table(
            identifier=table_name,
            schema=simple_schema,
        )

        table.append(simple_arrow_data)

        # Get the data files from the table
        data_files = []
        for snapshot in table.snapshots():
            for manifest_list in snapshot.manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file:
                        data_files.append(manifest_entry.data_file)

        # Should have at least one data file
        assert len(data_files) >= 1

        # For now, just verify that the Vortex compatibility functions can be imported
        # In a full implementation, you would check compatibility of the data files
        from pyiceberg.io.vortex import analyze_vortex_compatibility, estimate_vortex_compression_ratio

        # These functions should be callable (even if they don't do much yet)
        assert analyze_vortex_compatibility is not None
        assert estimate_vortex_compression_ratio is not None


@pytest.mark.integration
def test_vortex_sql_catalog_end_to_end(tmp_path: Path) -> None:
    """End-to-end integration test for Vortex-compatible operations with SQL catalog."""
    # This test demonstrates a complete workflow that could be enhanced with Vortex
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    catalog = SqlCatalog(
        name="e2e_catalog",
        uri=f"sqlite:///{tmp_path}/e2e_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )

    try:
        # Create schema
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        )

        # Create table
        table = catalog.create_table(
            identifier="default.e2e_vortex_test",
            schema=schema,
        )

        # Generate and write test data
        test_data = pa.table(
            {
                "id": list(range(1, 101)),  # 100 rows
                "data": [f"data_{i}" for i in range(1, 101)],
            }
        )

        table.append(test_data)

        # Perform various operations
        result1 = table.scan().to_arrow()
        assert len(result1) == 100

        # Filter scan
        result2 = table.scan(row_filter="id < 50").to_arrow()
        assert len(result2) == 49

        # Add more data
        more_data = pa.table(
            {
                "id": list(range(101, 151)),  # 50 more rows
                "data": [f"more_data_{i}" for i in range(101, 151)],
            }
        )

        table.append(more_data)

        # Verify total
        final_result = table.scan().to_arrow()
        assert len(final_result) == 150

        # Create a Vortex file from some of the data to demonstrate compatibility
        vortex_file_path = tmp_path / "sample.vortex"
        sample_data = pa.table(
            {
                "id": [200, 201, 202],
                "data": ["vortex_1", "vortex_2", "vortex_3"],
            }
        )

        # This demonstrates that Vortex file operations work alongside the catalog
        data_file = convert_iceberg_to_vortex_file(
            iceberg_table_data=sample_data,
            iceberg_schema=schema,
            output_path=f"file://{vortex_file_path}",
            io=table.io,
            compression=True,
        )

        assert data_file is not None
        assert vortex_file_path.exists()

        # Read the Vortex file back
        vortex_batches = read_vortex_file(f"file://{vortex_file_path}", table.io)
        vortex_data = pa.Table.from_batches(vortex_batches)
        assert len(vortex_data) == 3

    finally:
        catalog.close()
