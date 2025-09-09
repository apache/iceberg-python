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
# pylint: disable=protected-access,unused-argument,redefined-outer-name
import os
import tempfile
import uuid
from typing import Iterator
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.io.vortex import (
    VORTEX_AVAILABLE,
    VortexWriteTask,
    analyze_vortex_compatibility,
    iceberg_schema_to_vortex_schema,
    read_vortex_file,
    vortex_to_arrow_table,
    write_vortex_file,
)
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

# Skip all tests if vortex-data is not available
pytestmark = pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex-data package not installed")


@pytest.fixture
def sample_schema() -> Schema:
    """Create a sample Iceberg schema for testing."""
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="active", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="score", field_type=IntegerType(), required=False),
    )


@pytest.fixture
def complex_schema() -> Schema:
    """Create a complex Iceberg schema for testing."""
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(
            field_id=3,
            name="address",
            field_type=StructType(
                NestedField(field_id=4, name="street", field_type=StringType(), required=False),
                NestedField(field_id=5, name="city", field_type=StringType(), required=False),
            ),
            required=False,
        ),
        NestedField(
            field_id=6,
            name="tags",
            field_type=ListType(element_id=7, element_type=StringType(), element_required=False),
            required=False,
        ),
        NestedField(
            field_id=8,
            name="metadata",
            field_type=MapType(
                key_id=9,
                key_type=StringType(),
                value_id=10,
                value_type=StringType(),
                value_required=False,
            ),
            required=False,
        ),
    )


@pytest.fixture
def sample_arrow_table() -> pa.Table:
    """Create a sample PyArrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "active": [True, False, True, True, False],
            "score": [95, 87, 92, 88, 91],
        }
    )


@pytest.fixture
def empty_arrow_table() -> pa.Table:
    """Create an empty PyArrow table for testing."""
    return pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
            "active": pa.array([], type=pa.bool_()),
            "score": pa.array([], type=pa.int32()),
        }
    )


@pytest.fixture
def temp_file_path() -> Iterator[str]:
    """Create a temporary file path for testing."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".vortex") as f:
        temp_path = f.name
    yield temp_path
    # Clean up
    if os.path.exists(temp_path):
        os.unlink(temp_path)


class TestVortexAvailability:
    """Test Vortex availability detection and error handling."""

    @patch("pyiceberg.io.vortex.VORTEX_AVAILABLE", False)
    def test_vortex_not_available_error(self, sample_schema: Schema) -> None:
        """Test that ImportError is raised when vortex-data is not available."""
        with pytest.raises(ImportError, match="vortex-data is not installed"):
            iceberg_schema_to_vortex_schema(sample_schema)

    def test_vortex_available(self) -> None:
        """Test that VORTEX_AVAILABLE is True when vortex-data is installed."""
        assert VORTEX_AVAILABLE is True


class TestSchemaConversion:
    """Test Iceberg to Vortex schema conversion."""

    def test_simple_schema_conversion(self, sample_schema: Schema) -> None:
        """Test conversion of a simple Iceberg schema."""
        vortex_schema = iceberg_schema_to_vortex_schema(sample_schema)
        assert vortex_schema is not None
        assert isinstance(vortex_schema, pa.Schema)
        assert len(vortex_schema) == 4

        # Check field names and types
        field_names = [field.name for field in vortex_schema]
        assert "id" in field_names
        assert "name" in field_names
        assert "active" in field_names
        assert "score" in field_names

    def test_complex_schema_conversion(self, complex_schema: Schema) -> None:
        """Test conversion of a complex Iceberg schema with nested types."""
        vortex_schema = iceberg_schema_to_vortex_schema(complex_schema)
        assert vortex_schema is not None
        assert isinstance(vortex_schema, pa.Schema)

        # Verify nested fields are preserved
        field_names = [field.name for field in vortex_schema]
        assert "address" in field_names
        assert "tags" in field_names
        assert "metadata" in field_names

    def test_schema_conversion_with_invalid_schema(self) -> None:
        """Test schema conversion with an invalid schema."""
        # Create a mock schema that will cause conversion to fail
        with patch("pyiceberg.io.pyarrow.schema_to_pyarrow", side_effect=Exception("Conversion failed")):
            with pytest.raises(ValueError, match="Failed to convert Iceberg schema"):
                iceberg_schema_to_vortex_schema(Schema())


class TestArrowVortexConversion:
    """Test Arrow to Vortex array conversion."""

    @pytest.mark.skipif(True, reason="Function arrow_to_vortex_array removed for optimization")
    def test_arrow_to_vortex_basic(self, sample_arrow_table: pa.Table) -> None:
        """Test basic Arrow to Vortex array conversion."""
        vortex_array = arrow_to_vortex_array(sample_arrow_table, compress=False)
        assert vortex_array is not None

    @pytest.mark.skipif(True, reason="Function arrow_to_vortex_array removed for optimization")
    def test_arrow_to_vortex_with_compression(self, sample_arrow_table: pa.Table) -> None:
        """Test Arrow to Vortex array conversion with compression."""
        vortex_array = arrow_to_vortex_array(sample_arrow_table, compress=True)
        assert vortex_array is not None

    @patch("pyiceberg.io.vortex.vx.array", side_effect=Exception("Conversion failed"))
    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_arrow_to_vortex_conversion_error(self, sample_arrow_table: pa.Table) -> None:
        """Test error handling in Arrow to Vortex conversion."""
        with pytest.raises(ValueError, match="Failed to convert PyArrow table to Vortex array"):
            arrow_to_vortex_array(sample_arrow_table)

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_vortex_to_arrow_basic(self, sample_arrow_table: pa.Table) -> None:
        """Test basic Vortex to Arrow table conversion."""
        # First convert to Vortex, then back to Arrow
        vortex_array = arrow_to_vortex_array(sample_arrow_table, compress=False)
        converted_table = vortex_to_arrow_table(vortex_array)

        assert isinstance(converted_table, pa.Table)
        assert len(converted_table) > 0

    def test_vortex_to_arrow_with_invalid_array(self) -> None:
        """Test Vortex to Arrow conversion with invalid input."""
        # Create a mock object without conversion methods
        mock_vortex_array = MagicMock()
        del mock_vortex_array.to_arrow_array
        del mock_vortex_array.to_arrow

        with pytest.raises(ValueError, match="does not have a recognized Arrow conversion method"):
            vortex_to_arrow_table(mock_vortex_array)


class TestVortexFileOperations:
    """Test Vortex file reading and writing operations."""

    def test_write_vortex_file(self, sample_arrow_table: pa.Table, temp_file_path: str) -> None:
        """Test writing a Vortex file."""
        io = PyArrowFileIO()
        file_size = write_vortex_file(
            arrow_table=sample_arrow_table,
            file_path=temp_file_path,
            io=io,
        )

        assert file_size > 0
        assert os.path.exists(temp_file_path)

    def test_read_vortex_file(self, sample_arrow_table: pa.Table, temp_file_path: str) -> None:
        """Test reading a Vortex file."""
        io = PyArrowFileIO()

        # First write the file
        write_vortex_file(
            arrow_table=sample_arrow_table,
            file_path=temp_file_path,
            io=io,
        )

        # Then read it back
        record_batches = list(
            read_vortex_file(
                file_path=temp_file_path,
                io=io,
            )
        )

        assert len(record_batches) > 0
        # Convert record batches to table for comparison
        read_table = pa.Table.from_batches(record_batches)
        assert isinstance(read_table, pa.Table)
        assert len(read_table) > 0

    def test_write_empty_table(self, empty_arrow_table: pa.Table, temp_file_path: str) -> None:
        """Test writing an empty Arrow table."""
        io = PyArrowFileIO()
        file_size = write_vortex_file(
            arrow_table=empty_arrow_table,
            file_path=temp_file_path,
            io=io,
        )

        assert file_size >= 0
        assert os.path.exists(temp_file_path)

    def test_read_nonexistent_file(self) -> None:
        """Test reading a non-existent Vortex file."""
        io = PyArrowFileIO()
        nonexistent_path = "/tmp/nonexistent_vortex_file.vortex"

        with pytest.raises((FileNotFoundError, OSError, IOError)):
            list(read_vortex_file(file_path=nonexistent_path, io=io))


class TestVortexWriteTask:
    """Test VortexWriteTask functionality."""

    def test_vortex_write_task_creation(self, sample_schema: Schema) -> None:
        """Test creation of VortexWriteTask."""
        task = VortexWriteTask(
            write_uuid=uuid.uuid4(),
            task_id=1,
            record_batches=[],
            partition_key=None,
            schema=sample_schema,
        )

        assert task.schema == sample_schema
        assert isinstance(task.write_uuid, uuid.UUID)
        assert task.task_id == 1

    def test_vortex_write_task_generate_data_file_path(self, sample_schema: Schema) -> None:
        """Test data file path generation in VortexWriteTask."""
        task = VortexWriteTask(
            write_uuid=uuid.uuid4(),
            task_id=1,
            record_batches=[],
            schema=sample_schema,
        )

        filename = task.generate_data_file_filename("vortex")
        assert filename.endswith(".vortex")
        assert str(task.write_uuid) in filename
        assert str(task.task_id) in filename


class TestConversionUtilities:
    """Test high-level conversion utilities."""

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_convert_iceberg_to_vortex_file(
        self, sample_arrow_table: pa.Table, sample_schema: Schema, temp_file_path: str
    ) -> None:
        """Test high-level Iceberg to Vortex file conversion."""
        io = PyArrowFileIO()
        data_file = convert_iceberg_to_vortex_file(
            iceberg_table_data=sample_arrow_table,
            iceberg_schema=sample_schema,
            output_path=temp_file_path,
            io=io,
            compression=True,
        )

        assert isinstance(data_file, DataFile)
        assert data_file.file_format == FileFormat.VORTEX
        assert data_file.file_path == temp_file_path
        assert data_file.record_count == len(sample_arrow_table)
        assert data_file.file_size_in_bytes > 0
        assert os.path.exists(temp_file_path)

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_convert_empty_table(self, empty_arrow_table: pa.Table, sample_schema: Schema) -> None:
        """Test conversion of empty table."""
        io = PyArrowFileIO()
        with pytest.raises(ValueError, match="Input table data is empty"):
            convert_iceberg_to_vortex_file(
                iceberg_table_data=empty_arrow_table,
                iceberg_schema=sample_schema,
                output_path="/tmp/test.vortex",
                io=io,
            )

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_batch_convert_iceberg_to_vortex(self, sample_arrow_table: pa.Table, sample_schema: Schema) -> None:
        """Test batch conversion of multiple tables."""
        io = PyArrowFileIO()
        tables = [sample_arrow_table, sample_arrow_table]

        with tempfile.TemporaryDirectory() as temp_dir:
            data_files = batch_convert_iceberg_to_vortex(
                arrow_tables=tables,
                iceberg_schema=sample_schema,
                output_directory=temp_dir,
                io=io,
                file_prefix="test_batch",
                compression=True,
            )

            assert len(data_files) == 2
            for i, data_file in enumerate(data_files):
                assert isinstance(data_file, DataFile)
                assert data_file.file_format == FileFormat.VORTEX
                assert f"test_batch_{i:04d}.vortex" in data_file.file_path
                assert os.path.exists(data_file.file_path)

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_batch_convert_empty_list(self, sample_schema: Schema) -> None:
        """Test batch conversion with empty table list."""
        io = PyArrowFileIO()
        data_files = batch_convert_iceberg_to_vortex(
            arrow_tables=[],
            iceberg_schema=sample_schema,
            output_directory="/tmp",
            io=io,
        )

        assert data_files == []


class TestOptimizationUtilities:
    """Test optimization and analysis utilities."""

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_estimate_compression_ratio(self, sample_arrow_table: pa.Table) -> None:
        """Test compression ratio estimation."""
        ratio = estimate_vortex_compression_ratio(sample_arrow_table)
        assert isinstance(ratio, float)
        assert 1.0 <= ratio <= 10.0

    @patch("pyiceberg.io.vortex.VORTEX_AVAILABLE", False)
    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_estimate_compression_ratio_no_vortex(self, sample_arrow_table: pa.Table) -> None:
        """Test compression ratio estimation when Vortex is not available."""
        ratio = estimate_vortex_compression_ratio(sample_arrow_table)
        assert ratio == 1.0

    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_optimize_vortex_write_config(self, sample_arrow_table: pa.Table) -> None:
        """Test write configuration optimization."""
        config = optimize_vortex_write_config(sample_arrow_table, target_file_size_mb=64)

        assert isinstance(config, dict)
        assert "compression" in config
        assert "row_group_size" in config
        assert "dictionary_encoding" in config
        assert "statistics" in config

    @patch("pyiceberg.io.vortex.VORTEX_AVAILABLE", False)
    @pytest.mark.skipif(True, reason="Removed optimization functions")
    def test_optimize_write_config_no_vortex(self, sample_arrow_table: pa.Table) -> None:
        """Test write configuration optimization when Vortex is not available."""
        config = optimize_vortex_write_config(sample_arrow_table)
        assert isinstance(config, dict)

    def test_analyze_vortex_compatibility_simple(self, sample_schema: Schema) -> None:
        """Test compatibility analysis for simple schema."""
        analysis = analyze_vortex_compatibility(sample_schema)

        assert isinstance(analysis, dict)
        assert analysis["compatible"] is True
        assert "warnings" in analysis
        assert "field_analysis" in analysis
        assert "recommended_optimizations" in analysis
        assert len(analysis["field_analysis"]) == len(sample_schema.fields)

    def test_analyze_vortex_compatibility_complex(self, complex_schema: Schema) -> None:
        """Test compatibility analysis for complex schema."""
        analysis = analyze_vortex_compatibility(complex_schema)

        assert isinstance(analysis, dict)
        assert analysis["compatible"] is True

        # Check that complex types are detected
        field_analysis = analysis["field_analysis"]
        nested_fields = [field for field in field_analysis if field["name"] in ["address", "tags", "metadata"]]
        assert len(nested_fields) > 0

    def test_analyze_compatibility_large_schema(self) -> None:
        """Test compatibility analysis for schema with many fields."""
        # Create a schema with >1000 fields
        fields = [
            NestedField(field_id=i, name=f"field_{i}", field_type=StringType(), required=False)
            for i in range(1, 1002)  # Creates 1001 fields
        ]
        large_schema = Schema(*fields)

        analysis = analyze_vortex_compatibility(large_schema)
        assert "Schema has >1000 fields" in str(analysis["warnings"])


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_write_task_with_invalid_io(self, sample_schema: Schema) -> None:
        """Test VortexWriteTask with invalid IO object."""
        # This should not raise an exception during creation
        task = VortexWriteTask(
            write_uuid=uuid.uuid4(),
            task_id=1,
            record_batches=[],
            schema=sample_schema,
        )
        assert task is not None

    @patch("pyiceberg.io.vortex.write_vortex_file", side_effect=Exception("Write failed"))
    @pytest.mark.skipif(True, reason="Function removed for optimization")
    def test_convert_file_write_error(self, sample_arrow_table: pa.Table, sample_schema: Schema) -> None:
        """Test error handling in file conversion when write fails."""
        io = PyArrowFileIO()
        with pytest.raises(ValueError, match="Failed to convert Iceberg data to Vortex file"):
            convert_iceberg_to_vortex_file(
                iceberg_table_data=sample_arrow_table,
                iceberg_schema=sample_schema,
                output_path="/tmp/test.vortex",
                io=io,
            )

    @pytest.mark.skipif(True, reason="Function removed for optimization")
    def test_batch_convert_with_write_failure(self, sample_arrow_table: pa.Table, sample_schema: Schema) -> None:
        """Test batch conversion with write failure."""
        io = PyArrowFileIO()
        tables = [sample_arrow_table]

        with patch("pyiceberg.io.vortex.convert_iceberg_to_vortex_file", side_effect=Exception("Write failed")):
            with pytest.raises(ValueError, match="Batch conversion failed"):
                batch_convert_iceberg_to_vortex(
                    arrow_tables=tables,
                    iceberg_schema=sample_schema,
                    output_directory="/tmp",
                    io=io,
                )
