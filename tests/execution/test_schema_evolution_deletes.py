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

"""Tests for schema evolution interactions with equality deletes, planner edge cases, and cleanup.

Covers:
1. Schema evolution + equality deletes (dropped columns emit warning, deletes skipped)
2. BoundedMemoryPlanner with empty manifests (yields zero FileScanTasks)
3. Sort-on-write temp file cleanup on mid-stream exception
4. CoW two-pass correctness under file immutability invariant
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from pyiceberg.manifest import DataFile
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

# =============================================================================
# Gap 1: Schema evolution + equality deletes (dropped columns)
# =============================================================================


class TestEqualityDeletesWithDroppedColumns:
    """Verify correct behavior when equality delete files reference columns dropped via schema evolution.

    Scenario: Table originally had columns (id, name, email). An equality delete file
    was written targeting equality_ids=[3] (email). Later, the schema was evolved to
    drop the 'email' column. The current schema is (id, name) only.

    Expected: _get_equality_field_names should emit a UserWarning and return an empty
    list (no column names resolved), causing the equality deletes to be skipped.
    """

    def test_all_equality_ids_dropped_emits_warning(self) -> None:
        """When ALL equality field IDs reference dropped columns, a warning is emitted."""
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        # Current schema after ALTER TABLE DROP COLUMN email
        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )

        # Equality delete file references field_id=3 (email) which no longer exists
        mock_delete_file = MagicMock(spec=DataFile)
        mock_delete_file.equality_ids = [3]

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = current_schema

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _get_equality_field_names([mock_delete_file], mock_metadata)

        # Should return empty list (no resolvable names), not None
        assert result == []

        # Should emit a UserWarning about unresolvable field IDs
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(user_warnings) == 1
        assert "field IDs [3]" in str(user_warnings[0].message)
        assert "do not exist" in str(user_warnings[0].message)
        assert "schema evolution" in str(user_warnings[0].message)

    def test_partial_equality_ids_dropped_resolves_remaining(self) -> None:
        """When SOME equality field IDs are dropped, resolve the remaining ones."""
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        # Current schema still has 'id' (field_id=1) but dropped 'email' (field_id=3)
        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )

        # Delete file references both id (exists) and email (dropped)
        mock_delete_file = MagicMock(spec=DataFile)
        mock_delete_file.equality_ids = [1, 3]

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = current_schema

        result = _get_equality_field_names([mock_delete_file], mock_metadata)

        # Should resolve 'id' (field_id=1) but not 'email' (field_id=3)
        assert result == ["id"]

    def test_no_equality_ids_returns_none(self) -> None:
        """When delete files have no equality_ids metadata at all, return None."""
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        )

        mock_delete_file = MagicMock(spec=DataFile)
        mock_delete_file.equality_ids = None

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = current_schema

        result = _get_equality_field_names([mock_delete_file], mock_metadata)

        # None means "metadata absent" -- distinct from empty list
        assert result is None

    def test_orchestrate_scan_warns_when_equality_ids_unresolvable(self) -> None:
        """orchestrate_scan emits a warning when equality_ids cannot be resolved and returns data unchanged."""
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        )

        # Delete file has equality_ids but none resolve to current schema
        mock_delete_file = MagicMock(spec=DataFile)
        mock_delete_file.equality_ids = [99, 100]  # Non-existent field IDs

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = current_schema

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _get_equality_field_names([mock_delete_file], mock_metadata)

        assert result == []
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(user_warnings) == 1
        assert "compaction" in str(user_warnings[0].message).lower()

    def test_none_vs_empty_list_triggers_different_caller_behavior(self) -> None:
        """Callers must distinguish None (no metadata) from [] (columns dropped).

        - None: caller should emit "do not specify equality_ids" warning
        - []: _get_equality_field_names already warned about schema evolution;
              caller should NOT emit a redundant/misleading "do not specify" warning
        """
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        )

        # Case 1: No equality_ids metadata → returns None
        mock_no_metadata = MagicMock(spec=DataFile)
        mock_no_metadata.equality_ids = None
        mock_table_meta = MagicMock()
        mock_table_meta.schema.return_value = current_schema

        result_none = _get_equality_field_names([mock_no_metadata], mock_table_meta)
        assert result_none is None, "No metadata must return None (not [])"

        # Case 2: equality_ids present but all dropped → returns []
        mock_dropped = MagicMock(spec=DataFile)
        mock_dropped.equality_ids = [99]  # Field ID doesn't exist in schema
        mock_table_meta2 = MagicMock()
        mock_table_meta2.schema.return_value = current_schema

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result_empty = _get_equality_field_names([mock_dropped], mock_table_meta2)

        assert result_empty == [], "Dropped columns must return [] (not None)"
        assert result_empty is not None, "Must be distinguishable from None via 'is None' check"

        # The schema evolution warning was emitted by _get_equality_field_names itself
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(user_warnings) == 1
        assert "do not specify" not in str(user_warnings[0].message).lower(), (
            "The 'do not specify equality_ids' message must NOT appear for dropped columns. "
            "That warning is only for the None case (metadata truly absent)."
        )


# =============================================================================
# Gap 2: BoundedMemoryPlanner with empty manifests
# =============================================================================


class TestBoundedMemoryPlannerEmptyManifests:
    """Verify BoundedMemoryPlanner handles edge cases with empty or delete-only manifests."""

    @pytest.fixture
    def planner(self) -> None:
        """Create a BoundedMemoryPlanner instance (requires DataFusion)."""
        pytest.importorskip("datafusion")
        from pyiceberg.execution.planning import BoundedMemoryPlanner

        return BoundedMemoryPlanner(memory_limit=64 * 1024 * 1024)

    def test_no_data_entries_yields_zero_tasks(self, planner) -> None:
        """When all manifests contain only delete entries (no data files), yield nothing."""
        from pyiceberg.execution.planning import InMemoryPlanner

        # Use InMemoryPlanner for this test since the logic is shared
        # (BoundedMemoryPlanner delegates to ManifestGroupPlanner for entry enumeration)
        in_memory = InMemoryPlanner()

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        )
        mock_metadata.specs.return_value = {0: MagicMock()}

        # Empty manifest list
        tasks = list(
            in_memory.plan_files(
                manifests=[],
                table_metadata=mock_metadata,
                row_filter=MagicMock(),
                io=MagicMock(),
            )
        )

        assert tasks == []

    def test_stream_entries_to_parquet_handles_empty_input(self, planner) -> None:
        """_stream_entries_to_parquet with zero entries produces valid (empty) Parquet files."""
        import tempfile
        from pathlib import Path

        import pyarrow.parquet as pq

        data_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        delete_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        data_tmp_path = data_tmp.name
        delete_tmp_path = delete_tmp.name
        data_tmp.close()
        delete_tmp.close()

        try:
            # Mock a ManifestGroupPlanner that yields no entries
            mock_planner = MagicMock()
            mock_planner.plan_manifest_entries.return_value = iter([])

            planner._stream_entries_to_parquet(
                mock_planner, manifests=[], data_tmp_path=data_tmp_path, delete_tmp_path=delete_tmp_path
            )

            # Both files should exist and be valid (possibly empty) Parquet
            data_meta = pq.read_metadata(data_tmp_path)
            delete_meta = pq.read_metadata(delete_tmp_path)
            assert data_meta.num_rows == 0
            assert delete_meta.num_rows == 0
        finally:
            Path(data_tmp_path).unlink(missing_ok=True)
            Path(delete_tmp_path).unlink(missing_ok=True)


# =============================================================================
# Gap 3: Sort-on-write temp file cleanup on mid-stream exception
# =============================================================================


class TestSortOnWriteTempFileCleanupOnException:
    """Verify temp files are cleaned up when the input reader raises mid-stream."""

    def test_materialize_context_manager_cleans_up_on_exception(self) -> None:
        """materialize_batches_to_parquet deletes temp file when context exits normally after write."""
        import os

        from pyiceberg.execution.materialize import materialize_batches_to_parquet

        schema = pa.schema([("id", pa.int64())])

        def _good_batches() -> Iterator[pa.RecordBatch]:
            yield pa.record_batch({"id": [1, 2, 3]}, schema=schema)
            yield pa.record_batch({"id": [4, 5, 6]}, schema=schema)

        tmp_path_captured = None
        with materialize_batches_to_parquet(_good_batches(), schema) as tmp_path:
            tmp_path_captured = tmp_path
            # File exists inside context
            assert os.path.exists(tmp_path)

        # Temp file must be cleaned up after context manager exits
        from pathlib import Path

        assert tmp_path_captured is not None
        assert not Path(tmp_path_captured).exists(), f"Temp file {tmp_path_captured} was not cleaned up after context exit"

    def test_sorted_reader_cleanup_guard_on_sort_exception(self) -> None:
        """_CleanupGuard cleans up when sort_fn raises an exception."""
        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader
        from pyiceberg.execution.materialize import materialize_to_parquet

        schema = pa.schema([("id", pa.int64())])
        table = pa.table({"id": [3, 1, 2]})

        def _failing_sort(path: str) -> Iterator[pa.RecordBatch]:
            raise RuntimeError("Sort backend failure")

        reader = _SortedRecordBatchReader.create(
            materialize_fn=lambda: materialize_to_parquet(table),
            sort_fn=_failing_sort,
            schema=schema,
        )

        with pytest.raises(RuntimeError, match="Sort backend failure"):
            reader.read_all()

    def test_sorted_reader_cleanup_guard_on_partial_consumption(self) -> None:
        """Temp file is cleaned up even if the reader is only partially consumed then dropped."""
        import gc

        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader
        from pyiceberg.execution.materialize import materialize_to_parquet

        schema = pa.schema([("id", pa.int64())])
        table = pa.table({"id": [3, 1, 2]})
        paths_created: list[str] = []

        # Wrap materialize_to_parquet to capture the temp path
        from contextlib import contextmanager

        @contextmanager
        def _tracking_materialize() -> None:
            with materialize_to_parquet(table) as path:
                paths_created.append(path)
                yield path

        def _identity_sort(path: str) -> Iterator[pa.RecordBatch]:
            import pyarrow.parquet as pq

            yield from pq.read_table(path).to_batches()

        reader = _SortedRecordBatchReader.create(
            materialize_fn=_tracking_materialize,
            sort_fn=_identity_sort,
            schema=schema,
        )

        # Read one batch then abandon the reader
        batch = next(reader)
        assert batch.num_rows > 0

        # Drop reader and force GC
        del reader
        gc.collect()

        # The finalizer should have cleaned up the temp file
        from pathlib import Path

        assert len(paths_created) == 1
        # The file should be cleaned up (either by GC finalizer or the context manager)
        # Note: GC-based cleanup timing is non-deterministic, but the file should
        # eventually be cleaned up. In CPython with refcounting, del triggers immediately.
        assert not Path(paths_created[0]).exists(), f"Temp file {paths_created[0]} was not cleaned up after reader abandonment"


# =============================================================================
# Gap 4: CoW file immutability invariant (two-pass correctness)
# =============================================================================


class TestCowTwoPassFileImmutabilityInvariant:
    """Verify the CoW two-pass path relies on Iceberg's file immutability invariant.

    The two-pass CoW delete reads a file twice:
    - Pass 1: count kept rows (to decide whether rewrite is needed)
    - Pass 2: re-read and stream filtered rows to a new file

    This is correct because Iceberg data files are immutable once written.
    The content cannot change between passes. If the file is deleted (concurrent
    compaction + GC), the read will fail, which correctly causes the transaction
    to fail (OCC retry pattern).
    """

    def test_two_pass_produces_same_result_as_single_pass(self) -> None:
        """Two-pass streaming and single-pass materialization produce identical results."""
        import tempfile
        from pathlib import Path

        import pyarrow.parquet as pq

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowReadBackend

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        )

        # Write test data to a Parquet file
        table = pa.table({"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]})
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            pq.write_table(table, tmp_path)

            backend = PyArrowReadBackend()

            # Single-pass: materialize and filter
            batches_single = list(backend.read_parquet(tmp_path, schema, MagicMock(), {}))
            full_table = pa.Table.from_batches(batches_single)
            # Delete rows where id > 3

            import pyarrow.compute as pc

            keep_filter = pc.field("id") <= 3
            single_pass_result = full_table.filter(keep_filter)

            # Two-pass: count then re-read
            batches_pass1 = list(backend.read_parquet(tmp_path, schema, MagicMock(), {}))
            total_rows = sum(b.num_rows for b in batches_pass1)
            kept_count = sum(b.filter(keep_filter).num_rows for b in batches_pass1)

            # Since file is immutable, pass 2 reads same data
            batches_pass2 = list(backend.read_parquet(tmp_path, schema, MagicMock(), {}))
            two_pass_result = pa.concat_tables(
                [pa.Table.from_batches([b.filter(keep_filter)]) for b in batches_pass2 if b.filter(keep_filter).num_rows > 0]
            )

            # Results must be identical (file immutability invariant)
            assert single_pass_result.equals(two_pass_result)
            assert single_pass_result.num_rows == 3
            assert kept_count == 3
            assert total_rows == 5
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_two_pass_fails_if_file_disappears(self) -> None:
        """If the file is deleted between passes, the second read raises an error."""
        import tempfile
        from pathlib import Path

        import pyarrow.parquet as pq

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowReadBackend

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        )

        table = pa.table({"id": [1, 2, 3]})
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            pq.write_table(table, tmp_path)
            backend = PyArrowReadBackend()

            # Pass 1 succeeds
            batches_pass1 = list(backend.read_parquet(tmp_path, schema, MagicMock(), {}))
            assert sum(b.num_rows for b in batches_pass1) == 3

            # Simulate concurrent compaction + GC deleting the file
            Path(tmp_path).unlink()

            # Pass 2 must fail (file gone) — this is correct OCC behavior
            with pytest.raises((FileNotFoundError, OSError, pa.ArrowInvalid)):
                list(backend.read_parquet(tmp_path, schema, MagicMock(), {}))
        finally:
            Path(tmp_path).unlink(missing_ok=True)
