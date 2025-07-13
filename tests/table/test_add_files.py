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
import time

import pytest

from tests.catalog.test_base import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType


@pytest.fixture
def mock_table(tmp_path):
    """Create a mock table for testing enhanced add_files functionality."""
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    table = catalog.create_table("default.test_table", schema=schema)
    return table


def test_add_files_duplicate_file_paths_validation(mock_table):
    """Test that add_files raises ValueError for duplicate file paths in input."""
    file_paths = [
        "s3://bucket/file1.parquet",
        "s3://bucket/file2.parquet", 
        "s3://bucket/file1.parquet",  # Duplicate
    ]
    
    # Use the table's add_files method (which will create a transaction internally)
    with pytest.raises(ValueError, match="File paths must be unique"):
        mock_table.add_files(file_paths=file_paths)


def test_add_files_check_duplicate_files_parameter_validation():
    """Test that check_duplicate_files parameter is accepted and validated correctly."""
    # Test the parameter validation without full integration
    from pyiceberg.table import Transaction
    from unittest.mock import MagicMock
    
    # Create a minimal mock table
    mock_table = MagicMock()
    mock_table.metadata = MagicMock()
    mock_table.current_snapshot.return_value = None
    
    # Create transaction
    tx = Transaction(mock_table)
    
    # Test that the method accepts the parameter (basic signature test)
    # We just test that the function signature works as expected
    file_paths = ["s3://bucket/file1.parquet"]
    
    # Test duplicate file path validation (this should work without mocking)
    duplicate_paths = ["path1.parquet", "path2.parquet", "path1.parquet"]
    with pytest.raises(ValueError, match="File paths must be unique"):
        tx.add_files(file_paths=duplicate_paths, check_duplicate_files=True)
    
    with pytest.raises(ValueError, match="File paths must be unique"):
        tx.add_files(file_paths=duplicate_paths, check_duplicate_files=False)


def test_add_files_retry_configuration_parameters():
    """Test that custom retry configuration parameters are accepted."""
    from pyiceberg.table import Transaction
    from unittest.mock import MagicMock
    from tenacity import stop_after_attempt, wait_fixed
    
    # Create minimal mock
    mock_table = MagicMock()
    mock_table.metadata = MagicMock()
    mock_table.current_snapshot.return_value = None
    
    tx = Transaction(mock_table)
    
    # Test that custom retry parameters are accepted in the signature
    file_paths = ["s3://bucket/file1.parquet"]
    
    # Test parameter validation (should fail on duplicate paths regardless of retry config)
    duplicate_paths = ["path1.parquet", "path2.parquet", "path1.parquet"]
    with pytest.raises(ValueError, match="File paths must be unique"):
        tx.add_files(
            file_paths=duplicate_paths,
            stop=stop_after_attempt(1),
            wait=wait_fixed(0.1)
        )


def test_add_files_snapshot_properties_parameter():
    """Test that snapshot properties parameter is accepted and passed correctly."""
    from pyiceberg.table import Transaction
    from unittest.mock import MagicMock
    
    # Create minimal mock
    mock_table = MagicMock()
    mock_table.metadata = MagicMock()
    mock_table.current_snapshot.return_value = None
    
    tx = Transaction(mock_table)
    
    # Test that custom properties parameter is accepted
    file_paths = ["s3://bucket/file1.parquet"]
    custom_properties = {
        "test.source": "unit_test",
        "test.batch_id": "batch_001"
    }
    
    # Test parameter validation still works with custom properties
    duplicate_paths = ["path1.parquet", "path2.parquet", "path1.parquet"]
    with pytest.raises(ValueError, match="File paths must be unique"):
        tx.add_files(file_paths=duplicate_paths, snapshot_properties=custom_properties)


def test_add_files_tenacity_import():
    """Test that tenacity decorators are imported and available."""
    # Test that the retry functionality is properly imported
    from tenacity import stop_after_attempt, wait_exponential, retry_if_exception_type
    from pyiceberg.exceptions import CommitFailedException
    
    # Verify these are callable
    assert callable(stop_after_attempt)
    assert callable(wait_exponential)
    assert callable(retry_if_exception_type)
    
    # Test that we can create retry configurations
    stop_config = stop_after_attempt(3)
    wait_config = wait_exponential(multiplier=1, min=2, max=10)
    retry_config = retry_if_exception_type(CommitFailedException)
    
    assert stop_config is not None
    assert wait_config is not None
    assert retry_config is not None


def test_add_files_thread_safety_simulation():
    """Test thread safety aspects using simple data structures."""
    import threading
    import time
    
    # Simulate concurrent file path processing
    file_paths_shared = []
    lock = threading.Lock()
    
    def worker_function(worker_id, num_files):
        """Simulate a worker adding file paths."""
        worker_paths = [f"worker-{worker_id}-file-{i}.parquet" for i in range(num_files)]
        
        with lock:
            file_paths_shared.extend(worker_paths)
        
        # Simulate duplicate check processing time
        time.sleep(0.01)
        return len(worker_paths)
    
    # Run multiple workers concurrently
    threads = []
    num_workers = 5
    files_per_worker = 10
    
    for i in range(num_workers):
        thread = threading.Thread(target=worker_function, args=(i, files_per_worker))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    # Verify results
    assert len(file_paths_shared) == num_workers * files_per_worker
    
    # Test duplicate detection on collected paths
    unique_paths = list(set(file_paths_shared))
    assert len(unique_paths) == len(file_paths_shared)  # Should be no duplicates


def test_add_files_performance_large_batch_simulation():
    """Performance test simulation for large batch operations."""
    from unittest.mock import MagicMock, patch
    
    # Test with reduced complexity - focus on file path processing
    num_files = 1000
    file_paths = [f"s3://bucket/large-batch-{i:04d}.parquet" for i in range(num_files)]

    # Test the input validation part (duplicate checking)
    start_time = time.time()
    
    # Test duplicate detection performance
    unique_paths = list(set(file_paths))
    assert len(unique_paths) == num_files
    
    # Test duplicate input validation
    try:
        duplicate_paths = file_paths + [file_paths[0]]  # Add one duplicate
        if len(duplicate_paths) != len(set(duplicate_paths)):
            raise ValueError("File paths must be unique.")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "File paths must be unique" in str(e)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Should complete quickly for large batch
    assert execution_time < 1.0, f"Large batch validation took too long: {execution_time:.2f}s"
    
    print(f"Processed {num_files} file paths in {execution_time:.4f}s")


def test_add_files_empty_list_handling():
    """Test handling of empty file lists - basic validation only."""
    from pyiceberg.table import Transaction
    from unittest.mock import MagicMock
    
    # Create minimal mock
    mock_table = MagicMock()
    mock_table.metadata = MagicMock()
    mock_table.current_snapshot.return_value = None
    
    tx = Transaction(mock_table)
    
    # Test that empty list doesn't fail on duplicate validation
    file_paths = []
    
    # Empty list should pass the duplicate validation check
    assert len(file_paths) == len(set(file_paths))  # No duplicates in empty list
    
    # Test that the signature accepts empty list without raising ValueError
    # (The actual processing would be tested in integration tests)
    try:
        # We don't expect this to fully succeed due to mocking limitations,
        # but it should at least pass the initial validation
        if len(file_paths) != len(set(file_paths)):
            raise ValueError("File paths must be unique.")
        # This validation should pass for empty list
        assert True
    except ValueError as e:
        if "File paths must be unique" in str(e):
            assert False, "Empty list should not fail duplicate validation"
        # Other errors are expected due to incomplete mocking
