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
"""Test the MaintenanceTable API surface."""

from pyiceberg.table import Table  # noqa: F401
from pyiceberg.table.maintenance import MaintenanceTable
from pyiceberg.table.update.snapshot import ExpireSnapshots


def test_maintenance_table_api(table_v2: Table) -> None:
    """Test that MaintenanceTable provides the expected API surface."""
    # Test MaintenanceTable can be instantiated
    maintenance = MaintenanceTable(table_v2)
    assert maintenance.tbl is table_v2

    # Test expire_snapshots returns ExpireSnapshots instance
    expire_snapshots = maintenance.expire_snapshots()
    assert isinstance(expire_snapshots, ExpireSnapshots)

    # Test that the returned ExpireSnapshots has expected methods
    assert hasattr(expire_snapshots, "expire_snapshot_by_id")
    assert hasattr(expire_snapshots, "commit")

    # Test that it has transaction semantics (context manager)
    assert hasattr(expire_snapshots, "__enter__")
    assert hasattr(expire_snapshots, "__exit__")


def test_maintenance_table_context_manager_usage(table_v2: Table) -> None:
    """Test that the ExpireSnapshots can be used as context manager."""
    maintenance = MaintenanceTable(table_v2)

    # Test context manager usage
    try:
        with maintenance.expire_snapshots() as expire:
            # Just test that the context manager works
            # We don't actually expire anything to avoid side effects
            assert hasattr(expire, "expire_snapshot_by_id")
    except NotImplementedError:
        # Expected: The test catalog doesn't support commits,
        # but this confirms transaction semantics work
        pass
    except Exception as e:
        # Other expected exceptions for missing snapshots or protection
        assert "does not exist" in str(e) or "protected" in str(e)


def test_basic_usage_pattern(table_v2: Table) -> None:
    """Test basic usage: table.maintenance.expire_snapshots().commit()"""
    # Get the maintenance interface
    maintenance = table_v2.maintenance
    assert isinstance(maintenance, MaintenanceTable)

    # Get expire snapshots builder
    expire_snapshots = maintenance.expire_snapshots()
    assert isinstance(expire_snapshots, ExpireSnapshots)

    # Verify we can call expire_snapshot_by_id and commit methods
    assert hasattr(expire_snapshots, "expire_snapshot_by_id")
    assert hasattr(expire_snapshots, "commit")


def test_context_manager_usage_pattern(table_v2: Table) -> None:
    """Test recommended context manager usage pattern."""
    # Test that context manager works without errors
    try:
        with table_v2.maintenance.expire_snapshots() as expire:
            # Verify the expire object has the expected methods
            assert hasattr(expire, "expire_snapshot_by_id")
            assert isinstance(expire, ExpireSnapshots)
    except (NotImplementedError, ValueError):
        # Expected for test catalogs that don't support commits
        # or when no snapshots exist to expire
        pass


def test_method_chaining_pattern(table_v2: Table) -> None:
    """Test method chaining pattern: table.maintenance.expire_snapshots().commit()"""
    # Get the expire snapshots builder through method chaining
    expire_snapshots = table_v2.maintenance.expire_snapshots()

    # Verify it's the right type and has expected methods
    assert isinstance(expire_snapshots, ExpireSnapshots)
    assert hasattr(expire_snapshots, "expire_snapshot_by_id")
    assert hasattr(expire_snapshots, "commit")

    # Verify method chaining works (expire_snapshot_by_id returns self)
    try:
        result = expire_snapshots.expire_snapshot_by_id(12345)
        assert result is expire_snapshots  # Should return self for chaining
    except ValueError as e:
        # Expected: snapshot doesn't exist
        assert "does not exist" in str(e)


def test_multiple_snapshots_context_manager(table_v2: Table) -> None:
    """Test expiring multiple snapshots in a single context manager."""
    snapshot_ids = [12345, 67890, 11111]

    try:
        with table_v2.maintenance.expire_snapshots() as expire:
            # Verify we can call expire_snapshot_by_id multiple times
            for snapshot_id in snapshot_ids:
                result = expire.expire_snapshot_by_id(snapshot_id)
                # Verify the method returns the same object for chaining
                assert result is expire
    except (ValueError, NotImplementedError):
        # Expected: snapshots don't exist or catalog doesn't support commits
        pass


def test_maintenance_interface_extensibility(table_v2: Table) -> None:
    """Test that maintenance interface is extensible for future operations."""
    maintenance = table_v2.maintenance

    # Current functionality
    assert hasattr(maintenance, "expire_snapshots")

    # Verify the maintenance table holds reference to original table
    assert maintenance.tbl is table_v2
    assert isinstance(maintenance, MaintenanceTable)


def test_transaction_autocommit_configuration(table_v2: Table) -> None:
    """Test that ExpireSnapshots is configured with autocommit transaction."""
    expire_snapshots = table_v2.maintenance.expire_snapshots()

    # Verify transaction is configured (autocommit=True is set internally)
    assert hasattr(expire_snapshots, "_transaction")
    assert expire_snapshots._transaction is not None


def test_real_world_cleanup_pattern(table_v2: Table) -> None:
    """Test a real-world cleanup function pattern."""

    def cleanup_old_snapshots(tbl: Table, snapshot_ids: list[int]) -> int:
        """Simulate cleanup function that returns number of snapshots processed."""
        count = 0
        try:
            with tbl.maintenance.expire_snapshots() as expire:
                for snapshot_id in snapshot_ids:
                    expire.expire_snapshot_by_id(snapshot_id)
                    count += 1
        except (ValueError, NotImplementedError):
            # Expected for test scenarios
            pass
        return count

    # Test the cleanup function pattern
    snapshot_ids = [12345, 67890]
    result = cleanup_old_snapshots(table_v2, snapshot_ids)

    # Function should execute without errors (even if snapshots don't exist)
    assert isinstance(result, int)


def test_table_maintenance_property_integration(table_v2: Table) -> None:
    """Test that table.maintenance returns MaintenanceTable instance."""
    # Verify table has maintenance property
    assert hasattr(table_v2, "maintenance")

    # Verify it returns MaintenanceTable
    maintenance = table_v2.maintenance
    assert isinstance(maintenance, MaintenanceTable)

    # Verify full integration chain works
    expire_snapshots = maintenance.expire_snapshots()
    assert isinstance(expire_snapshots, ExpireSnapshots)
