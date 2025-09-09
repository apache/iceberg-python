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

"""Unit tests for Vortex file support in add_files functionality."""

from unittest.mock import Mock, patch

import pytest

from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import vortex_file_to_data_file
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import _files_to_data_files
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.types import LongType, NestedField, StringType


@pytest.fixture
def mock_table_metadata() -> Mock:
    """Create a mock TableMetadata for testing."""
    mock_metadata = Mock()
    mock_metadata.format_version = 2
    
    # Mock spec with empty fields list (unpartitioned table)
    mock_spec = Mock()
    mock_spec.fields = []
    mock_metadata.spec.return_value = mock_spec
    
    mock_metadata.schema.return_value = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )
    mock_metadata.default_spec_id = 0
    return mock_metadata


@pytest.fixture
def mock_file_io() -> Mock:
    """Create a mock FileIO for testing."""
    mock_io = Mock()
    mock_input = Mock()
    mock_input.__len__ = Mock(return_value=1024)  # Mock file size
    mock_io.new_input.return_value = mock_input
    return mock_io


def test_vortex_file_to_data_file_import_error(mock_file_io: Mock, mock_table_metadata: Mock) -> None:
    """Test that vortex_file_to_data_file raises ImportError when vortex is not available."""
    with patch("pyiceberg.io.vortex.VORTEX_AVAILABLE", False):
        with pytest.raises(ImportError, match="vortex-data is not installed"):
            vortex_file_to_data_file(mock_file_io, mock_table_metadata, "test.vortex")


def test_vortex_file_to_data_file_returns_data_file(mock_file_io: Mock, mock_table_metadata: Mock) -> None:
    """Test that vortex_file_to_data_file creates a DataFile with correct properties."""
    # Mock the vortex reader and arrow batch
    mock_batch = Mock()
    mock_batch.num_rows = 10
    mock_batch.schema = Mock()

    with patch("pyiceberg.io.vortex.VORTEX_AVAILABLE", True), \
         patch("pyiceberg.io.vortex.read_vortex_file") as mock_read_vortex, \
         patch("pyiceberg.io.pyarrow._check_pyarrow_schema_compatible") as mock_check_schema:

        mock_read_vortex.return_value = iter([mock_batch])  # Return iterator with one batch

        result = vortex_file_to_data_file(mock_file_io, mock_table_metadata, "test.vortex")

        # Verify the result is a DataFile with Vortex format
        assert result.file_format == FileFormat.VORTEX
        assert result.file_path == "test.vortex"
        assert result.record_count == 10
        
        # Verify the vortex reader was called correctly
        mock_read_vortex.assert_called_once_with(file_path="test.vortex", io=mock_file_io, batch_size=10000)
        
        # Verify schema compatibility was checked
        mock_check_schema.assert_called_once()


def test_files_to_data_files_detects_formats_correctly() -> None:
    """Test that _files_to_data_files correctly detects file formats by extension."""
    mock_metadata = Mock(spec=TableMetadata)
    mock_io = Mock(spec=FileIO)

    file_paths = [
        "data/file1.parquet",
        "data/file2.vortex",
        "data/file3.PARQUET",  # Test case insensitive
        "data/file4.VORTEX",   # Test case insensitive
        "data/file5.txt",      # Should default to parquet
    ]

    with patch("pyiceberg.io.pyarrow.parquet_file_to_data_file"), \
         patch("pyiceberg.io.pyarrow.vortex_file_to_data_file"), \
         patch("pyiceberg.table.ExecutorFactory.get_or_create") as mock_executor:

        # Mock executor to return results synchronously
        mock_executor_instance = Mock()
        mock_future = Mock()
        mock_future.result.return_value = Mock()  # Mock DataFile
        mock_executor_instance.submit.return_value = mock_future
        mock_executor.return_value = mock_executor_instance

        _files_to_data_files(mock_metadata, file_paths, mock_io)

        # Verify correct functions were called for each file type
        expected_parquet_calls = 3  # file1.parquet, file3.PARQUET, file5.txt (default)
        expected_vortex_calls = 2   # file2.vortex, file4.VORTEX

        # Check executor.submit was called with the correct functions
        submit_calls = mock_executor_instance.submit.call_args_list
        assert len(submit_calls) == 5

        # Count function calls by checking which function was passed to submit
        parquet_submits = sum(1 for call in submit_calls if "parquet_file_to_data_file" in str(call[0][0]))
        vortex_submits = sum(1 for call in submit_calls if "vortex_file_to_data_file" in str(call[0][0]))

        assert parquet_submits == expected_parquet_calls
        assert vortex_submits == expected_vortex_calls


def test_files_to_data_files_extension_detection() -> None:
    """Test specific file extension detection logic."""
    from pyiceberg.table import _files_to_data_files

    mock_metadata = Mock(spec=TableMetadata)
    mock_io = Mock(spec=FileIO)

    # Test various extension cases
    test_cases = [
        ("file.parquet", "parquet"),
        ("file.PARQUET", "parquet"),
        ("file.vortex", "vortex"),
        ("file.VORTEX", "vortex"),
        ("file.Parquet", "parquet"),
        ("file.Vortex", "vortex"),
        ("file.txt", "parquet"),  # Default case
        ("file", "parquet"),      # No extension
    ]

    with patch("pyiceberg.io.pyarrow.parquet_file_to_data_file"), \
         patch("pyiceberg.io.pyarrow.vortex_file_to_data_file"), \
         patch("pyiceberg.table.ExecutorFactory.get_or_create") as mock_executor:

        mock_executor_instance = Mock()
        mock_future = Mock()
        mock_future.result.return_value = Mock()
        mock_executor_instance.submit.return_value = mock_future
        mock_executor.return_value = mock_executor_instance

        for file_path, expected_format in test_cases:
            # Reset mocks
            mock_executor_instance.reset_mock()

            _files_to_data_files(mock_metadata, [file_path], mock_io)

            # Check that the correct function was submitted
            submit_call = mock_executor_instance.submit.call_args_list[0]
            submitted_function = submit_call[0][0]

            if expected_format == "vortex":
                assert "vortex_file_to_data_file" in str(submitted_function)
            else:  # parquet or default
                assert "parquet_file_to_data_file" in str(submitted_function)
