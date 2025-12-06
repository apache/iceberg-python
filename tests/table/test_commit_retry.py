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

"""Tests for commit retry logic in SnapshotProducer."""

from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table, TableProperties
from pyiceberg.table.update import TableRequirement, TableUpdate
from pyiceberg.types import LongType, NestedField


@pytest.fixture
def schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    )


@pytest.fixture
def arrow_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3]})


@pytest.fixture
def catalog(tmp_path: str) -> SqlCatalog:
    """Create a SQL catalog for testing."""
    catalog = SqlCatalog(
        "test_catalog",
        uri=f"sqlite:///{tmp_path}/test.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace_if_not_exists("default")
    return catalog


class TestSnapshotProducerRetry:
    def test_retry_on_commit_failed_exception(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that CommitFailedException triggers retry."""
        table = catalog.create_table(
            "default.test_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # Track commit attempts
        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.append(arrow_table)

        assert commit_count == 2
        assert len(table.scan().to_arrow()) == 3

    def test_max_retries_exceeded(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that exception is raised after max retries."""
        table = catalog.create_table(
            "default.test_max_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            raise CommitFailedException("Always fails")

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with pytest.raises(CommitFailedException, match="Always fails"):
                table.append(arrow_table)

        assert commit_count == 3

    def test_commit_state_unknown_not_retried(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that CommitStateUnknownException is not retried."""
        table = catalog.create_table(
            "default.test_unknown",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "5",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
            },
        )

        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            raise CommitStateUnknownException("Unknown state")

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with pytest.raises(CommitStateUnknownException, match="Unknown state"):
                table.append(arrow_table)

        # Should only be called once (no retry)
        assert commit_count == 1

    def test_snapshot_id_changes_on_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that snapshot ID is regenerated on retry."""
        table = catalog.create_table(
            "default.test_snapshot_id",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        snapshot_ids: list[int] = []
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            # Extract snapshot ID from updates
            for update in updates:
                if hasattr(update, "snapshot"):
                    snapshot_ids.append(update.snapshot.snapshot_id)
            if commit_count < 2:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.append(arrow_table)

        assert commit_count == 2
        assert len(snapshot_ids) == 2
        # Snapshot IDs should be different on retry
        assert snapshot_ids[0] != snapshot_ids[1]

    def test_table_refreshed_on_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that table is refreshed on retry."""
        table = catalog.create_table(
            "default.test_refresh",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        refresh_count = 0
        original_refresh = table.refresh

        def mock_refresh() -> None:
            nonlocal refresh_count
            refresh_count += 1
            original_refresh()

        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count < 2:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(table, "refresh", side_effect=mock_refresh):
            with patch.object(catalog, "commit_table", side_effect=mock_commit):
                table.append(arrow_table)

        # Refresh should be called on retry (not on first attempt)
        assert refresh_count == 1
        assert commit_count == 2

    def test_multiple_appends_with_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that multiple append operations each retry independently."""
        table = catalog.create_table(
            "default.test_multi_append_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            # Fail every first attempt of each append
            if commit_count in (1, 3):
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            # First append: fails on 1, succeeds on 2
            table.append(arrow_table)
            # Second append: fails on 3, succeeds on 4
            table.append(arrow_table)

        assert commit_count == 4
        # Verify both appends succeeded
        assert len(table.scan().to_arrow()) == 6

    def test_delete_operation_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that delete operations also retry on CommitFailedException.

        Note: Delete operations may produce multiple commits:
        1. DELETE snapshot: marks files as deleted
        2. OVERWRITE snapshot: rewrites files if rows need to be filtered out

        This test verifies that retry works for delete operations.
        """
        table = catalog.create_table(
            "default.test_delete_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append data
        table.append(arrow_table)

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.delete("1 <= id AND id <= 3")

        # Delete may produce multiple commits (DELETE + OVERWRITE if rewrite needed)
        # At minimum, we expect 2 commits (1 failed + 1 success for first operation)
        assert commit_count == 2
        # Verify delete worked - should have 2 rows remaining
        assert len(table.scan().to_arrow()) == 0

    def test_partial_delete_operation_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that delete operations also retry on CommitFailedException.

        Note: Delete operations may produce multiple commits:
        1. DELETE snapshot: marks files as deleted
        2. OVERWRITE snapshot: rewrites files if rows need to be filtered out

        This test verifies that retry works for delete operations.
        """
        table = catalog.create_table(
            "default.test_delete_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append data
        table.append(arrow_table)

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.delete("id == 1")

        # commit_count is 2: 1 failed + 1 success for DELETE snapshot
        # Note: OVERWRITE may or may not happen depending on partition evaluation
        assert commit_count >= 2
        # Verify delete worked - should have 2 rows remaining
        assert len(table.scan().to_arrow()) == 2


class TestTransactionRetryWithMultipleUpdates:
    """Test transaction retry with multiple update types (like Java BaseTransaction)."""

    def test_transaction_with_property_and_append(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test transaction with property update and append retries correctly."""
        table = catalog.create_table(
            "default.test_prop_append",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as tx:
                tx.set_properties({"test.property": "value1"})
                tx.append(arrow_table)

        # Should have retried once
        assert commit_count == 2

        # Verify both updates were applied
        table = catalog.load_table("default.test_prop_append")
        assert table.metadata.properties.get("test.property") == "value1"
        assert len(table.scan().to_arrow()) == 3

    def test_transaction_with_single_append_snapshot_regenerated(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test that snapshot ID is regenerated on retry for single append."""
        table = catalog.create_table(
            "default.test_snapshot_regen",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0
        snapshot_ids: list[int] = []

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            # Capture snapshot IDs from updates
            for update in updates:
                if hasattr(update, "snapshot"):
                    snapshot_ids.append(update.snapshot.snapshot_id)
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as tx:
                tx.append(arrow_table)

        # Should have retried once
        assert commit_count == 2

        # Verify data was written
        table = catalog.load_table("default.test_snapshot_regen")
        assert len(table.scan().to_arrow()) == 3

        # On retry, snapshot ID should be regenerated
        assert len(snapshot_ids) == 2
        # IDs should be different
        assert snapshot_ids[0] != snapshot_ids[1]

    def test_transaction_with_schema_evolution_and_append(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test transaction with schema evolution and append retries correctly."""
        from pyiceberg.types import StringType

        table = catalog.create_table(
            "default.test_schema_append",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as tx:
                tx.update_schema().add_column("name", StringType()).commit()
                tx.append(arrow_table)

        # Should have retried once
        assert commit_count == 2

        # Verify schema was updated
        table = catalog.load_table("default.test_schema_append")
        field_names = [field.name for field in table.schema().fields]
        assert "name" in field_names

        # Verify data was written
        assert len(table.scan().to_arrow()) == 3

    def test_transaction_max_retries_exceeded_with_multiple_updates(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test that transaction fails after max retries with multiple updates."""
        table = catalog.create_table(
            "default.test_max_retry_multi",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "2",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            raise CommitFailedException("Always fails")

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with pytest.raises(CommitFailedException, match="Always fails"):
                with table.transaction() as tx:
                    tx.set_properties({"test.property": "value"})
                    tx.append(arrow_table)

        # Should have tried max_attempts times
        assert commit_count == 2

        table = catalog.load_table("default.test_max_retry_multi")
        assert "test.property" not in table.metadata.properties
        assert len(table.scan().to_arrow()) == 0

    def test_transaction_updates_regenerated_on_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that all updates are regenerated on retry."""
        table = catalog.create_table(
            "default.test_regenerate",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0
        updates_per_attempt: list[int] = []

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            updates_per_attempt.append(len(updates))
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as tx:
                tx.set_properties({"prop1": "value1", "prop2": "value2"})
                tx.append(arrow_table)

        # Both attempts should have the same number of updates
        assert len(updates_per_attempt) == 2
        assert updates_per_attempt[0] == updates_per_attempt[1]

    def test_transaction_with_remove_properties_and_append(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test transaction with remove properties and append."""
        table = catalog.create_table(
            "default.test_remove_prop",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
                "to.be.removed": "value",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as tx:
                tx.remove_properties("to.be.removed")
                tx.append(arrow_table)

        assert commit_count == 2

        # Verify property was removed and data was written
        table = catalog.load_table("default.test_remove_prop")
        assert "to.be.removed" not in table.metadata.properties
        assert len(table.scan().to_arrow()) == 3


class TestMultiSnapshotTransactionRetry:
    """Tests for transactions containing multiple snapshot operations with retry."""

    def test_overwrite_operation_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test overwrite operation (delete + append) retries correctly.

        Overwrite creates two snapshots:
        1. DELETE snapshot - removes existing data
        2. APPEND snapshot - adds new data

        The APPEND snapshot must have DELETE as its parent.
        """
        table = catalog.create_table(
            "default.test_overwrite_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append some data
        table.append(arrow_table)
        initial_snapshot_id = table.metadata.current_snapshot_id

        original_commit = catalog.commit_table
        commit_count = 0
        captured_updates: list[list[Any]] = []

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            captured_updates.append(list(updates))
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        overwrite_arrow_table = pa.table({"id": [2, 3, 4]})
        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.overwrite(overwrite_arrow_table)

        assert commit_count == 2
        assert len(table.scan().to_arrow()) == 3

        # Verify the snapshot chain is correct
        table = catalog.load_table("default.test_overwrite_retry")
        snapshots = table.metadata.snapshots

        # Should have 3 snapshots: initial append, delete, overwrite append
        assert len(snapshots) == 3

        # Get the last two snapshots (delete and append from overwrite)
        delete_snapshot = snapshots[1]
        append_snapshot = snapshots[2]

        # Delete snapshot's parent should be initial snapshot
        assert delete_snapshot.parent_snapshot_id == initial_snapshot_id

        # Append snapshot's parent should be delete snapshot
        assert append_snapshot.parent_snapshot_id == delete_snapshot.snapshot_id

    def test_multiple_appends_in_transaction_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test transaction with multiple appends retries correctly.

        Note: This test verifies that multiple snapshot operations in a single
        transaction maintain correct parent relationships after retry.
        """
        table = catalog.create_table(
            "default.test_multi_append_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as tx:
                tx.append(arrow_table)
                tx.append(arrow_table)

        assert commit_count == 2

        # Verify data was written correctly
        table = catalog.load_table("default.test_multi_append_retry")
        assert len(table.scan().to_arrow()) == 6  # 3 + 3 rows

        # Verify snapshot chain
        snapshots = table.metadata.snapshots
        assert len(snapshots) == 2

        # Second snapshot's parent should be first snapshot
        assert snapshots[1].parent_snapshot_id == snapshots[0].snapshot_id

    def test_parent_snapshot_id_correct_on_first_attempt(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test that parent snapshot IDs are correct even on first attempt (no retry).

        This verifies the fix works for the normal case, not just retries.
        """
        table = catalog.create_table(
            "default.test_parent_first_attempt",
            schema=schema,
        )

        # Initial append
        table.append(arrow_table)
        initial_snapshot_id = table.metadata.current_snapshot_id

        # Overwrite (creates delete + append snapshots)
        table.overwrite(arrow_table)

        table = catalog.load_table("default.test_parent_first_attempt")
        snapshots = table.metadata.snapshots

        assert len(snapshots) == 3

        delete_snapshot = snapshots[1]
        append_snapshot = snapshots[2]

        # Verify parent chain
        assert delete_snapshot.parent_snapshot_id == initial_snapshot_id
        assert append_snapshot.parent_snapshot_id == delete_snapshot.snapshot_id

    def test_snapshot_ids_change_on_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that snapshot IDs are regenerated on retry."""
        table = catalog.create_table(
            "default.test_ids_change",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append
        table.append(arrow_table)

        original_commit = catalog.commit_table
        commit_count = 0
        snapshot_ids_per_attempt: list[list[int]] = []

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1

            # Extract snapshot IDs from updates
            from pyiceberg.table.update import AddSnapshotUpdate

            ids = [u.snapshot.snapshot_id for u in updates if isinstance(u, AddSnapshotUpdate)]
            snapshot_ids_per_attempt.append(ids)

            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.overwrite(arrow_table)

        assert commit_count == 2
        assert len(snapshot_ids_per_attempt) == 2

        # Both attempts should have 2 snapshots (delete + append)
        assert len(snapshot_ids_per_attempt[0]) == 2
        assert len(snapshot_ids_per_attempt[1]) == 2

        # Snapshot IDs should be different between attempts
        assert snapshot_ids_per_attempt[0][0] != snapshot_ids_per_attempt[1][0]
        assert snapshot_ids_per_attempt[0][1] != snapshot_ids_per_attempt[1][1]

    def test_apply_method_returns_updated_metadata(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that apply() method returns correctly updated metadata.

        This tests the Java-style apply() pattern where each operation
        transforms metadata directly.
        """
        table = catalog.create_table(
            "default.test_apply_method",
            schema=schema,
        )

        # First append some data
        table.append(arrow_table)
        initial_snapshot_id = table.metadata.current_snapshot_id

        # Create a transaction with overwrite
        with table.transaction() as tx:
            # Access the snapshot producer through update_snapshot
            update_snap = tx.update_snapshot(snapshot_properties={}, branch="main")
            fast_append = update_snap.fast_append()

            # Add data to the append
            from pyiceberg.io.pyarrow import _dataframe_to_data_files

            data_files = _dataframe_to_data_files(
                table_metadata=tx.table_metadata, write_uuid=fast_append.commit_uuid, df=arrow_table, io=table.io
            )
            for data_file in data_files:
                fast_append.append_data_file(data_file)

            # Test apply() method
            updated_metadata = fast_append.apply()

            # Verify the metadata was updated
            # apply() returns metadata with the new snapshot added
            assert len(updated_metadata.snapshots) == len(tx.table_metadata.snapshots) + 1
            # The apply() should have added the snapshot
            new_snapshot = updated_metadata.snapshots[-1]
            assert new_snapshot.parent_snapshot_id == initial_snapshot_id
            assert new_snapshot.snapshot_id == fast_append.snapshot_id

            # Continue with the transaction (this will actually commit)
            fast_append.commit()

        # Verify data was written
        table = catalog.load_table("default.test_apply_method")
        assert len(table.scan().to_arrow()) == 6  # 3 + 3 rows


class TestAutocommitRetry:
    """Tests for autocommit transaction retry scenarios.

    These tests verify that the autocommit path (where _SnapshotProducer.commit()
    handles retry directly) works correctly, particularly that _working_metadata
    is updated on retry.
    """

    def test_autocommit_append_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that autocommit append retries correctly.

        This tests the _refresh_state() fix where _working_metadata must be
        updated to match the refreshed table state for retry to work properly.
        """
        table = catalog.create_table(
            "default.test_autocommit_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count < 2:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            # Direct append uses autocommit=True internally
            table.append(arrow_table)

        assert commit_count == 2
        # Verify data was written
        assert len(table.scan().to_arrow()) == 3

    def test_autocommit_retry_with_concurrent_commits(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test autocommit retry when another writer commits between retries.

        This tests the critical scenario where _working_metadata must be updated
        to the latest table state so that parent_snapshot_id is correct on retry.
        """
        table = catalog.create_table(
            "default.test_autocommit_concurrent",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append to have initial data
        table.append(arrow_table)

        # Simulate concurrent commit by another writer before our retry
        table2 = catalog.load_table("default.test_autocommit_concurrent")
        table2.append(arrow_table)
        concurrent_snapshot_id = table2.metadata.current_snapshot_id

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1

            if commit_count == 1:
                # First attempt fails because our table object has stale metadata
                raise CommitFailedException("Simulated conflict from concurrent writer")

            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            # This should retry and succeed with updated parent
            table.append(arrow_table)

        assert commit_count == 2

        # Reload table to verify
        table = catalog.load_table("default.test_autocommit_concurrent")
        assert len(table.scan().to_arrow()) == 9  # 3 + 3 + 3 rows

        # Verify snapshot chain is correct
        snapshots = table.metadata.snapshots
        assert len(snapshots) == 3

        # Last snapshot's parent should be the concurrent writer's snapshot
        # (not the initial snapshot)
        assert snapshots[2].parent_snapshot_id == concurrent_snapshot_id

    def test_autocommit_delete_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that autocommit delete operation retries correctly."""
        table = catalog.create_table(
            "default.test_autocommit_delete",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append data
        table.append(arrow_table)

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.delete("id == 1")

        # At minimum 2 commits (failed + success)
        assert commit_count >= 2
        # Verify delete worked
        assert len(table.scan().to_arrow()) == 2

    def test_autocommit_overwrite_retry(self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table) -> None:
        """Test that autocommit overwrite operation retries correctly."""
        table = catalog.create_table(
            "default.test_autocommit_overwrite",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First append data
        table.append(arrow_table)

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        overwrite_data = pa.table({"id": [4, 5, 6]})
        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            table.overwrite(overwrite_data)

        assert commit_count == 2
        # Verify overwrite worked
        result = table.scan().to_arrow()
        assert len(result) == 3
        assert result["id"].to_pylist() == [4, 5, 6]


class TestUpdateSpecRetry:
    def test_update_spec_retried_on_conflict(self, catalog: SqlCatalog, schema: Schema) -> None:
        """Test that UpdateSpec operations are retried on CommitFailedException."""
        from pyiceberg.transforms import BucketTransform

        table = catalog.create_table(
            "default.test_spec_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated spec conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.update_spec() as update_spec:
                update_spec.add_field(source_column_name="id", transform=BucketTransform(16), partition_field_name="id_bucket")

        assert commit_count == 2

    def test_update_spec_resolves_conflict_on_retry(self, catalog: SqlCatalog, schema: Schema) -> None:
        """Test that spec update can resolve conflicts via retry"""
        from pyiceberg.transforms import BucketTransform

        table = catalog.create_table(
            "default.test_spec_conflict_resolved",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "5",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        with table.update_spec() as update_spec:
            update_spec.add_field(source_column_name="id", transform=BucketTransform(16), partition_field_name="id_bucket")

        table2 = catalog.load_table("default.test_spec_conflict_resolved")
        with table2.update_spec() as update_spec2:
            update_spec2.add_identity("id")

        assert table.spec().spec_id == 1
        assert table2.spec().spec_id == 2

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            # Retry resolves conflicts caused by mismatch spec_id
            with table.update_spec() as update_spec:
                update_spec.add_field(source_column_name="id", transform=BucketTransform(8), partition_field_name="id_bucket_new")

        assert commit_count == 2

    def test_transaction_with_spec_change_and_append_retries(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test that a transaction with spec change and append handles retry correctly."""
        table = catalog.create_table(
            "default.test_transaction_spec_and_append",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0
        captured_updates: list[tuple[TableUpdate, ...]] = []

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            captured_updates.append(updates)
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as txn:
                with txn.update_spec() as update_spec:
                    update_spec.add_identity("id")
                txn.append(arrow_table)

        assert commit_count == 2

        first_attempt_update_types = [type(u).__name__ for u in captured_updates[0]]
        assert "AddPartitionSpecUpdate" in first_attempt_update_types
        assert "AddSnapshotUpdate" in first_attempt_update_types

        retry_attempt_update_types = [type(u).__name__ for u in captured_updates[1]]
        assert "AddPartitionSpecUpdate" in retry_attempt_update_types
        assert "AddSnapshotUpdate" in retry_attempt_update_types

        assert len(table.scan().to_arrow()) == 3

        from pyiceberg.transforms import IdentityTransform

        assert table.spec().spec_id == 1
        assert len(table.spec().fields) == 1
        partition_field = table.spec().fields[0]
        assert partition_field.name == "id"
        assert partition_field.source_id == 1  # "id" column's field_id
        assert isinstance(partition_field.transform, IdentityTransform)


class TestUpdateSchemaRetry:
    """Tests for UpdateSchema retry behavior (Java-like behavior)."""

    def test_update_schema_retried_on_conflict(self, catalog: SqlCatalog, schema: Schema) -> None:
        """Test that UpdateSchema operations are retried on CommitFailedException."""
        from pyiceberg.types import StringType

        table = catalog.create_table(
            "default.test_schema_retry",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise CommitFailedException("Simulated schema conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.update_schema() as update_schema:
                update_schema.add_column("new_col", StringType())

        assert commit_count == 2

        # Verify schema was updated
        table.refresh()
        assert len(table.schema().fields) == 2
        assert table.schema().find_field("new_col").field_type == StringType()

    def test_update_schema_resolves_conflict_on_retry(self, catalog: SqlCatalog, schema: Schema) -> None:
        """Test that schema update can resolve conflicts via retry."""
        from pyiceberg.types import StringType

        table = catalog.create_table(
            "default.test_schema_conflict_resolved",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "5",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        # First schema change
        with table.update_schema() as update_schema:
            update_schema.add_column("col1", StringType())

        # Concurrent schema change by another writer
        table2 = catalog.load_table("default.test_schema_conflict_resolved")
        with table2.update_schema() as update_schema2:
            update_schema2.add_column("col2", StringType())

        assert table.schema().schema_id == 1
        assert table2.schema().schema_id == 2

        original_commit = catalog.commit_table
        commit_count = 0

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            # This should succeed on retry after refreshing metadata
            with table.update_schema() as update_schema:
                update_schema.add_column("col3", StringType())

        # Should have retried (first attempt fails due to stale schema_id)
        assert commit_count == 2

        # Verify schema was updated with all columns
        table.refresh()
        assert table.schema().schema_id == 3
        field_names = [f.name for f in table.schema().fields]
        assert "id" in field_names
        assert "col1" in field_names
        assert "col2" in field_names
        assert "col3" in field_names

    def test_transaction_with_schema_change_and_append_retries(
        self, catalog: SqlCatalog, schema: Schema, arrow_table: pa.Table
    ) -> None:
        """Test that a transaction with schema change and append handles retry correctly."""
        from pyiceberg.types import StringType

        table = catalog.create_table(
            "default.test_transaction_schema_and_append",
            schema=schema,
            properties={
                TableProperties.COMMIT_NUM_RETRIES: "3",
                TableProperties.COMMIT_MIN_RETRY_WAIT_MS: "1",
                TableProperties.COMMIT_MAX_RETRY_WAIT_MS: "10",
            },
        )

        original_commit = catalog.commit_table
        commit_count = 0
        captured_updates: list[tuple[TableUpdate, ...]] = []

        def mock_commit(
            tbl: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
        ) -> CommitTableResponse:
            nonlocal commit_count
            commit_count += 1
            captured_updates.append(updates)
            if commit_count == 1:
                raise CommitFailedException("Simulated conflict")
            return original_commit(tbl, requirements, updates)

        with patch.object(catalog, "commit_table", side_effect=mock_commit):
            with table.transaction() as txn:
                with txn.update_schema() as update_schema:
                    update_schema.add_column("new_col", StringType())
                txn.append(arrow_table)

        assert commit_count == 2

        # On the first attempt, updates should include both schema change and snapshot
        first_attempt_update_types = [type(u).__name__ for u in captured_updates[0]]
        assert "AddSchemaUpdate" in first_attempt_update_types
        assert "AddSnapshotUpdate" in first_attempt_update_types

        # On the retry, BOTH schema and snapshot updates should be present
        retry_attempt_update_types = [type(u).__name__ for u in captured_updates[1]]
        assert "AddSchemaUpdate" in retry_attempt_update_types
        assert "AddSnapshotUpdate" in retry_attempt_update_types

        # Verify data was written
        assert len(table.scan().to_arrow()) == 3

        # Verify schema was updated
        assert table.schema().schema_id == 1
        assert len(table.schema().fields) == 2
        assert table.schema().find_field("new_col").field_type == StringType()
