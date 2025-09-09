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

"""Integration tests for add_files functionality with Vortex file format."""

from pathlib import Path
from typing import Generator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

# Check if vortex is available
try:
    from pyiceberg.io.vortex import VORTEX_AVAILABLE
except ImportError:
    VORTEX_AVAILABLE = False

pytestmark = pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex-data package not installed")


@pytest.fixture(scope="function")
def vortex_catalog(tmp_path: Path) -> Generator[SqlCatalog, None, None]:
    """Create a SQL catalog for testing Vortex add_files integration."""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    catalog = SqlCatalog(
        name="test_vortex_add_files_catalog",
        uri=f"sqlite:///{tmp_path}/vortex_add_files_catalog.db",
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
    """Test schema for add_files integration tests."""
    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "age", IntegerType(), required=False),
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


def create_vortex_file(data: pa.Table, file_path: str) -> None:
    """Create a Vortex file from PyArrow table data."""
    from pyiceberg.io.fsspec import FsspecFileIO
    from pyiceberg.io.vortex import write_vortex_file

    io = FsspecFileIO()
    write_vortex_file(data=data, file_path=file_path, io=io)


@pytest.mark.integration
def test_add_vortex_files_to_table(
    vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table, tmp_path: Path
) -> None:
    """Test adding Vortex files to an Iceberg table."""
    # Create table
    table_name = "test_vortex_add_files"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Create test Vortex files
    vortex_file_1 = str(tmp_path / "data_1.vortex")
    vortex_file_2 = str(tmp_path / "data_2.vortex")
    
    # Split test data into two files
    data_1 = test_data.slice(0, 2)
    data_2 = test_data.slice(3, 2)
    
    create_vortex_file(data_1, vortex_file_1)
    create_vortex_file(data_2, vortex_file_2)

    # Add Vortex files to table
    tbl.add_files(file_paths=[vortex_file_1, vortex_file_2])

    # Verify the files were added
    data_files = tbl.inspect.data_files()
    assert len(data_files.to_pylist()) == 2

    # Verify the file format is Vortex
    for data_file in data_files.to_pylist():
        assert data_file["file_format"] == FileFormat.VORTEX.value
        assert data_file["file_path"].endswith(".vortex")

    # Verify we can read the data back
    result = tbl.scan().to_arrow()
    assert len(result) == 4  # 2 rows from each file


@pytest.mark.integration
def test_add_mixed_format_files_to_table(
    vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table, tmp_path: Path
) -> None:
    """Test adding both Vortex and Parquet files to the same table."""
    # Create table
    table_name = "test_mixed_format_add_files"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Create test files in different formats
    vortex_file = str(tmp_path / "data.vortex")
    parquet_file = str(tmp_path / "data.parquet")
    
    # Use first 2 rows for Vortex file
    vortex_data = test_data.slice(0, 2)
    create_vortex_file(vortex_data, vortex_file)
    
    # Use next 2 rows for Parquet file
    parquet_data = test_data.slice(2, 2)
    pq.write_table(parquet_data, parquet_file)

    # Add both files to table
    tbl.add_files(file_paths=[vortex_file, parquet_file])

    # Verify the files were added with correct formats
    data_files = tbl.inspect.data_files()
    data_file_list = data_files.to_pylist()
    assert len(data_file_list) == 2

    # Check that we have one of each format
    formats = {df["file_format"] for df in data_file_list}
    assert FileFormat.VORTEX.value in formats
    assert FileFormat.PARQUET.value in formats

    # Verify paths and formats match
    for data_file in data_file_list:
        if data_file["file_path"].endswith(".vortex"):
            assert data_file["file_format"] == FileFormat.VORTEX.value
        elif data_file["file_path"].endswith(".parquet"):
            assert data_file["file_format"] == FileFormat.PARQUET.value
        else:
            pytest.fail(f"Unexpected file format for file: {data_file['file_path']}")

    # Verify we can read all the data back
    result = tbl.scan().to_arrow()
    assert len(result) == 4  # 2 rows from each file


@pytest.mark.integration
def test_add_vortex_file_with_incompatible_schema(
    vortex_catalog: SqlCatalog, test_schema: Schema, tmp_path: Path
) -> None:
    """Test that adding a Vortex file with incompatible schema raises an error."""
    # Create table with original schema
    table_name = "test_vortex_incompatible_schema"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Create data with incompatible schema (missing required field)
    incompatible_data = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": ["Alice", "Bob", "Charlie"],
        # Missing age, active, score fields
    })
    
    vortex_file = str(tmp_path / "incompatible.vortex")
    create_vortex_file(incompatible_data, vortex_file)

    # Adding the file should raise an error
    with pytest.raises((ValueError, ImportError)):
        tbl.add_files(file_paths=[vortex_file])


@pytest.mark.integration
def test_add_vortex_files_maintains_transaction_consistency(
    vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table, tmp_path: Path
) -> None:
    """Test that add_files maintains transaction consistency with Vortex files."""
    # Create table
    table_name = "test_vortex_transaction_consistency"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Get initial snapshot
    initial_snapshot = tbl.current_snapshot()

    # Create Vortex files
    vortex_files = []
    for i in range(3):
        vortex_file = str(tmp_path / f"data_{i}.vortex")
        data_subset = test_data.slice(i, 1)  # One row per file
        create_vortex_file(data_subset, vortex_file)
        vortex_files.append(vortex_file)

    # Add all files in one transaction
    tbl.add_files(file_paths=vortex_files)

    # Verify new snapshot was created
    new_snapshot = tbl.current_snapshot()
    assert new_snapshot.snapshot_id != (initial_snapshot.snapshot_id if initial_snapshot else 0)

    # Verify all files are present
    data_files = tbl.inspect.data_files()
    assert len(data_files.to_pylist()) == 3

    # Verify data integrity
    result = tbl.scan().to_arrow()
    assert len(result) == 3


@pytest.mark.integration
def test_add_vortex_files_with_snapshot_properties(
    vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table, tmp_path: Path
) -> None:
    """Test adding Vortex files with custom snapshot properties."""
    # Create table
    table_name = "test_vortex_snapshot_props"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Create Vortex file
    vortex_file = str(tmp_path / "data_with_props.vortex")
    create_vortex_file(test_data, vortex_file)

    # Add file with custom properties
    custom_properties = {
        "operation": "vortex-add-files-test",
        "added_by": "test-suite",
        "file_count": "1"
    }
    
    tbl.add_files(file_paths=[vortex_file], snapshot_properties=custom_properties)

    # Verify snapshot properties
    current_snapshot = tbl.current_snapshot()
    assert current_snapshot is not None
    
    snapshot_summary = current_snapshot.summary
    for key, value in custom_properties.items():
        assert snapshot_summary.get(key) == value


@pytest.mark.integration
def test_vortex_file_format_detection_by_extension(
    vortex_catalog: SqlCatalog, test_schema: Schema, test_data: pa.Table, tmp_path: Path
) -> None:
    """Test that file format is correctly detected by extension for Vortex files."""
    # Create table
    table_name = "test_vortex_format_detection"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Create files with different extensions
    files_and_expected_formats = [
        (str(tmp_path / "data.vortex"), FileFormat.VORTEX),
        (str(tmp_path / "data.parquet"), FileFormat.PARQUET),
        (str(tmp_path / "data.VORTEX"), FileFormat.VORTEX),  # Test case insensitive
        (str(tmp_path / "data.PARQUET"), FileFormat.PARQUET),  # Test case insensitive
    ]

    for file_path, expected_format in files_and_expected_formats:
        if expected_format == FileFormat.VORTEX:
            create_vortex_file(test_data.slice(0, 1), file_path)
        else:
            pq.write_table(test_data.slice(0, 1), file_path)

    # Add all files
    file_paths = [fp for fp, _ in files_and_expected_formats]
    tbl.add_files(file_paths=file_paths)

    # Verify formats were detected correctly
    data_files = tbl.inspect.data_files()
    data_file_list = data_files.to_pylist()
    
    for data_file in data_file_list:
        file_path = data_file["file_path"]
        if file_path.lower().endswith(".vortex"):
            assert data_file["file_format"] == FileFormat.VORTEX.value
        elif file_path.lower().endswith(".parquet"):
            assert data_file["file_format"] == FileFormat.PARQUET.value
        else:
            pytest.fail(f"Unexpected file extension: {file_path}")


@pytest.mark.integration
def test_add_vortex_file_error_handling(
    vortex_catalog: SqlCatalog, test_schema: Schema, tmp_path: Path
) -> None:
    """Test error handling when adding invalid Vortex files."""
    # Create table
    table_name = "test_vortex_error_handling"
    tbl = vortex_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=test_schema,
    )

    # Test 1: Non-existent file
    non_existent_file = str(tmp_path / "does_not_exist.vortex")
    with pytest.raises(FileNotFoundError):
        tbl.add_files(file_paths=[non_existent_file])

    # Test 2: Empty file
    empty_file = str(tmp_path / "empty.vortex")
    Path(empty_file).touch()  # Create empty file
    with pytest.raises(ValueError):
        tbl.add_files(file_paths=[empty_file])

    # Test 3: Invalid Vortex file (actually a text file)
    invalid_file = str(tmp_path / "invalid.vortex")
    with open(invalid_file, "w") as f:
        f.write("This is not a Vortex file")

    with pytest.raises(ValueError):
        tbl.add_files(file_paths=[invalid_file])
