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

import uuid
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table
from pyiceberg.table.maintenance import MaintenanceTable
from tenacity import stop_after_attempt


@pytest.fixture
def iceberg_catalog(tmp_path: Path) -> Generator[InMemoryCatalog, None, None]:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    yield catalog
    # Clean up SQLAlchemy engine connections
    if hasattr(catalog, "engine"):
        try:
            catalog.engine.dispose()
        except Exception:
            pass


def test_rebuild_current_snapshot_retry_mechanism(iceberg_catalog: InMemoryCatalog, tmp_path: Path) -> None:
    """Test that rebuild_current_snapshot properly handles retry scenarios."""
    identifier = "default.rebuild_retry_test"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    arrow_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("value", pa.string(), nullable=True),
    ])

    # Create test files
    unique_id = uuid.uuid4()
    base_dir = tmp_path / f"{unique_id}_retry"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    file1 = base_dir / "file1.parquet"
    
    # Create test data
    data1 = pa.Table.from_pylist([{"id": 1, "value": "RETRY_TEST"}], schema=arrow_schema)
    
    # Write files
    pq.write_table(data1, str(file1))

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    try:
        # Add a file to the table
        tx1 = table.transaction()
        tx1.add_files([str(file1)], check_duplicate_files=False)
        tx1.commit_transaction()

        mt = MaintenanceTable(tbl=table)
        
        # Test 1: Verify retry kwargs are properly used
        snapshot_properties = {"retry.test": "custom_config"}
        
        # Mock the transaction commit to fail twice, then succeed
        call_count = 0
        
        def mock_commit_with_failures():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise CommitFailedException("Simulated commit failure")
            # On third call, succeed by doing nothing (we'll mock the whole transaction)
            return None
        
        # Test custom retry configuration
        with patch.object(table, 'transaction') as mock_transaction:
            mock_tx = Mock()
            mock_update = Mock()
            mock_overwrite = Mock()
            mock_overwrite.__enter__ = Mock(return_value=mock_overwrite)
            mock_overwrite.__exit__ = Mock(return_value=None)
            mock_overwrite.delete_data_file = Mock()
            mock_overwrite.append_data_file = Mock()
            
            mock_update.overwrite.return_value = mock_overwrite
            mock_tx.update_snapshot.return_value = mock_update
            mock_tx.commit_transaction = mock_commit_with_failures
            mock_transaction.return_value = mock_tx
            
            # This should succeed after 3 attempts
            mt.rebuild_current_snapshot(
                snapshot_properties=snapshot_properties,
                stop=stop_after_attempt(5)  # Custom retry config
            )
            
            # Verify that commit was attempted 3 times (2 failures + 1 success)
            assert call_count == 3, f"Expected 3 commit attempts, got {call_count}"
        
        # Test 2: Verify that retry exhaustion raises the exception
        call_count = 0
        
        def mock_commit_always_fails():
            nonlocal call_count
            call_count += 1
            raise CommitFailedException(f"Persistent failure attempt {call_count}")
        
        with patch.object(table, 'transaction') as mock_transaction:
            mock_tx = Mock()
            mock_update = Mock()
            mock_overwrite = Mock()
            mock_overwrite.__enter__ = Mock(return_value=mock_overwrite)
            mock_overwrite.__exit__ = Mock(return_value=None)
            mock_overwrite.delete_data_file = Mock()
            mock_overwrite.append_data_file = Mock()
            
            mock_update.overwrite.return_value = mock_overwrite
            mock_tx.update_snapshot.return_value = mock_update
            mock_tx.commit_transaction = mock_commit_always_fails
            mock_transaction.return_value = mock_tx
            
            # This should fail after 2 attempts (custom stop config)
            with pytest.raises(CommitFailedException, match="Persistent failure"):
                mt.rebuild_current_snapshot(
                    snapshot_properties=snapshot_properties,
                    stop=stop_after_attempt(2)  # Only 2 attempts
                )
            
            # Verify that commit was attempted exactly 2 times
            assert call_count == 2, f"Expected 2 commit attempts, got {call_count}"

    finally:
        # Cleanup table's catalog connections
        if hasattr(table, "_catalog") and hasattr(table._catalog, "engine"):
            try:
                table._catalog.engine.dispose()
            except Exception:
                pass


def test_rebuild_current_snapshot_optimization(iceberg_catalog: InMemoryCatalog, tmp_path: Path) -> None:
    """Test that rebuild_current_snapshot only re-fetches data files when snapshot ID changes."""
    identifier = "default.rebuild_optimization_test"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    arrow_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("value", pa.string(), nullable=True),
    ])

    # Create test files
    unique_id = uuid.uuid4()
    base_dir = tmp_path / f"{unique_id}_optimization"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    file1 = base_dir / "file1.parquet"
    
    # Create test data
    data1 = pa.Table.from_pylist([{"id": 1, "value": "OPTIMIZATION_TEST"}], schema=arrow_schema)
    
    # Write files
    pq.write_table(data1, str(file1))

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    try:
        # Add a file to the table
        tx1 = table.transaction()
        tx1.add_files([str(file1)], check_duplicate_files=False)
        tx1.commit_transaction()

        mt = MaintenanceTable(tbl=table)
        
        # Track calls to _get_all_datafiles to verify optimization
        get_datafiles_call_count = 0
        original_get_datafiles = mt._get_all_datafiles
        
        def mock_get_datafiles():
            nonlocal get_datafiles_call_count
            get_datafiles_call_count += 1
            return original_get_datafiles()
        
        # Mock to simulate multiple retries but with same snapshot ID
        call_count = 0
        
        def mock_commit_with_failures():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise CommitFailedException("Simulated commit failure for optimization test")
            # Third attempt succeeds
            return None
        
        with patch.object(mt, '_get_all_datafiles', side_effect=mock_get_datafiles), \
             patch.object(table, 'transaction') as mock_transaction:
            
            mock_tx = Mock()
            mock_update = Mock()
            mock_overwrite = Mock()
            mock_overwrite.__enter__ = Mock(return_value=mock_overwrite)
            mock_overwrite.__exit__ = Mock(return_value=None)
            mock_overwrite.delete_data_file = Mock()
            mock_overwrite.append_data_file = Mock()
            
            mock_update.overwrite.return_value = mock_overwrite
            mock_tx.update_snapshot.return_value = mock_update
            mock_tx.commit_transaction = mock_commit_with_failures
            mock_transaction.return_value = mock_tx
            
            # This should succeed after 3 attempts with the same snapshot
            mt.rebuild_current_snapshot(
                snapshot_properties={"optimization": "test"},
                stop=stop_after_attempt(5)
            )
            
            # Verify that _get_all_datafiles was called only once initially
            # Since snapshot ID doesn't change between retries, it shouldn't be called again
            assert get_datafiles_call_count == 1, f"Expected 1 call to _get_all_datafiles, got {get_datafiles_call_count}"
            
            # Verify that commit was attempted 3 times
            assert call_count == 3, f"Expected 3 commit attempts, got {call_count}"

    finally:
        # Cleanup table's catalog connections
        if hasattr(table, "_catalog") and hasattr(table._catalog, "engine"):
            try:
                table._catalog.engine.dispose()
            except Exception:
                pass


def test_rebuild_current_snapshot_refresh_on_retry(iceberg_catalog: InMemoryCatalog, tmp_path: Path) -> None:
    """Test that rebuild_current_snapshot refreshes table state on retry attempts."""
    identifier = "default.rebuild_refresh_test"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    arrow_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("value", pa.string(), nullable=True),
    ])

    # Create test files
    unique_id = uuid.uuid4()
    base_dir = tmp_path / f"{unique_id}_refresh"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    file1 = base_dir / "file1.parquet"
    
    # Create test data
    data1 = pa.Table.from_pylist([{"id": 1, "value": "REFRESH_TEST"}], schema=arrow_schema)
    
    # Write files
    pq.write_table(data1, str(file1))

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    try:
        # Add a file to the table
        tx1 = table.transaction()
        tx1.add_files([str(file1)], check_duplicate_files=False)
        tx1.commit_transaction()

        mt = MaintenanceTable(tbl=table)
        
        # Mock refresh to verify it's called on retry
        refresh_call_count = 0
        original_refresh = table.refresh
        
        def mock_refresh():
            nonlocal refresh_call_count
            refresh_call_count += 1
            return original_refresh()
        
        call_count = 0
        
        def mock_commit_with_one_failure():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise CommitFailedException("First attempt fails")
            # Second attempt succeeds
            return None
        
        with patch.object(table, 'refresh', side_effect=mock_refresh), \
             patch.object(table, 'transaction') as mock_transaction:
            
            mock_tx = Mock()
            mock_update = Mock()
            mock_overwrite = Mock()
            mock_overwrite.__enter__ = Mock(return_value=mock_overwrite)
            mock_overwrite.__exit__ = Mock(return_value=None)
            mock_overwrite.delete_data_file = Mock()
            mock_overwrite.append_data_file = Mock()
            
            mock_update.overwrite.return_value = mock_overwrite
            mock_tx.update_snapshot.return_value = mock_update
            mock_tx.commit_transaction = mock_commit_with_one_failure
            mock_transaction.return_value = mock_tx
            
            # This should succeed after 2 attempts (1 failure + 1 success)
            mt.rebuild_current_snapshot(
                snapshot_properties={"test": "refresh"},
                stop=stop_after_attempt(3)
            )
            
            # Verify that refresh was called on each retry attempt
            # Should be called at least twice: once for each attempt
            assert refresh_call_count >= 2, f"Expected at least 2 refresh calls, got {refresh_call_count}"
            
            # Verify that commit was attempted 2 times (1 failure + 1 success)
            assert call_count == 2, f"Expected 2 commit attempts, got {call_count}"

    finally:
        # Cleanup table's catalog connections
        if hasattr(table, "_catalog") and hasattr(table._catalog, "engine"):
            try:
                table._catalog.engine.dispose()
            except Exception:
                pass
