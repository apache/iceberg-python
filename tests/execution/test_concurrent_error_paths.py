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

"""Tests for concurrency correctness, error propagation, and file-disappearance edge cases.

Covers:
1. Concurrent equality delete resolution — parallel anti-joins produce correct results
2. Schema evolution + equality deletes with RENAMED columns (same field ID, new name)
3. BoundedMemoryPlanner with corrupt/truncated temp Parquet — clear error propagation
4. CoW delete two-pass: file disappearing between passes raises (no silent skip)
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

# =============================================================================
# Gap 1: Concurrent equality delete resolution — semantic correctness
# =============================================================================


class TestConcurrentAntiJoinSemanticCorrectness:
    """Verify parallel anti-join operations produce correct, non-interfering results.

    The thread pool dispatches multiple tasks concurrently, each performing an
    anti-join on different data. Results must be independent — no cross-task
    contamination from shared backend instances.
    """

    def test_parallel_anti_joins_produce_independent_results(self):
        """Multiple threads performing anti-joins on distinct data yield correct results."""
        backend = PyArrowComputeBackend()

        # Each task has unique data/delete sets — results must not bleed across tasks.
        tasks = [
            # (left_data, right_deletes, expected_surviving_ids)
            ({"id": [1, 2, 3, 4, 5]}, {"id": [2, 4]}, {1, 3, 5}),
            ({"id": [10, 20, 30, 40]}, {"id": [10, 30]}, {20, 40}),
            ({"id": [100, 200, 300]}, {"id": [200]}, {100, 300}),
            ({"id": [7, 8, 9]}, {"id": [7, 8, 9]}, set()),
            ({"id": [42, 43, 44]}, {"id": []}, {42, 43, 44}),
        ]

        errors: list[str] = []

        def run_anti_join(left_data, right_data, expected):
            left = [pa.record_batch(left_data)]
            right = [pa.record_batch(right_data)] if right_data["id"] else []
            result_batches = list(backend.anti_join(iter(left), iter(right), ["id"]))
            if result_batches:
                result_ids = set(pa.Table.from_batches(result_batches).column("id").to_pylist())
            else:
                result_ids = set()
            if result_ids != expected:
                errors.append(f"Expected {expected}, got {result_ids}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            # Run multiple iterations to increase chance of detecting races.
            for _ in range(20):
                for left_data, right_data, expected in tasks:
                    futures.append(executor.submit(run_anti_join, left_data, right_data, expected))

            for future in as_completed(futures):
                future.result()  # Propagate exceptions

        assert not errors, f"Concurrent anti-join produced incorrect results: {errors}"

    def test_parallel_anti_joins_with_nulls_produce_correct_results(self):
        """Concurrent anti-joins with NULL values maintain IS NOT DISTINCT FROM semantics."""
        backend = PyArrowComputeBackend()

        results: list[set] = []
        lock = threading.Lock()

        def anti_join_with_nulls(thread_id: int):
            # Left has [1, 2, None, 4], right has [2, None]
            # IS NOT DISTINCT FROM: None matches None → result is {1, 4}
            left = [pa.record_batch({"id": pa.array([1, 2, None, 4], type=pa.int64())})]
            right = [pa.record_batch({"id": pa.array([2, None], type=pa.int64())})]
            result_batches = list(backend.anti_join(iter(left), iter(right), ["id"]))
            result_ids = set(pa.Table.from_batches(result_batches).column("id").to_pylist())
            with lock:
                results.append(result_ids)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(anti_join_with_nulls, i) for i in range(50)]
            for f in as_completed(futures):
                f.result()

        # ALL results must be identical: {1, 4}
        for i, result in enumerate(results):
            assert result == {1, 4}, f"Thread {i} produced {result}, expected {{1, 4}}"


# =============================================================================
# Gap 2: Schema evolution + equality deletes with RENAMED columns
# =============================================================================


class TestEqualityDeletesWithRenamedColumns:
    """Verify equality deletes work correctly when columns have been renamed.

    In Iceberg, field IDs are stable across renames. A delete file targeting
    field_id=2 should still apply after the column is renamed from "name" to
    "full_name" — because find_column_name(field_id=2) returns the NEW name.
    """

    def test_renamed_column_resolves_via_field_id(self):
        """Equality delete on field_id=2 resolves to current name after rename."""
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        # Current schema: field_id=2 is now called "full_name" (was "name")
        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="full_name", field_type=StringType(), required=False),
            NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
        )

        # Delete file was written when field_id=2 was called "name"
        delete_file = MagicMock()
        delete_file.equality_ids = [2]  # References field_id=2

        table_metadata = MagicMock()
        table_metadata.schema.return_value = current_schema

        result = _get_equality_field_names([delete_file], table_metadata)

        # Should resolve to the CURRENT name "full_name" (not the old "name")
        assert result == ["full_name"]

    def test_renamed_column_anti_join_uses_new_name(self):
        """Anti-join correctly uses the renamed column for equality delete resolution."""
        backend = PyArrowComputeBackend()

        # Data file has column "full_name" (the current name for field_id=2)
        left = [
            pa.record_batch(
                {
                    "id": [1, 2, 3],
                    "full_name": ["alice", "bob", "charlie"],
                }
            )
        ]
        # Delete file also has "full_name" (because read_parquet projects via current schema)
        right = [
            pa.record_batch(
                {
                    "full_name": ["bob"],
                }
            )
        ]

        result_batches = list(backend.anti_join(iter(left), iter(right), ["full_name"]))
        result = pa.Table.from_batches(result_batches)

        assert result.column("id").to_pylist() == [1, 3]
        assert result.column("full_name").to_pylist() == ["alice", "charlie"]

    def test_partially_renamed_multi_column_equality_delete(self):
        """Multi-column equality delete works when some columns are renamed."""
        from pyiceberg.execution._orchestrate import _get_equality_field_names

        # Original: (id=1, name=2, email=3)
        # Current: (id=1, full_name=2, contact_email=3)
        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="full_name", field_type=StringType(), required=False),
            NestedField(field_id=3, name="contact_email", field_type=StringType(), required=False),
        )

        # Delete file targets field_ids=[2, 3]
        delete_file = MagicMock()
        delete_file.equality_ids = [2, 3]

        table_metadata = MagicMock()
        table_metadata.schema.return_value = current_schema

        result = _get_equality_field_names([delete_file], table_metadata)

        # Both should resolve to current names
        assert sorted(result) == ["contact_email", "full_name"]


# =============================================================================
# Gap 3: BoundedMemoryPlanner with corrupt/truncated temp Parquet
# =============================================================================


class TestBoundedMemoryPlannerCorruptInput:
    """Verify BoundedMemoryPlanner produces clear errors for corrupt temp files.

    If the process is interrupted during Phase 1 (streaming entries to Parquet),
    temp files may be truncated. DataFusion should raise a Parquet decode error
    that propagates clearly (not a silent empty result or generic KeyError).
    """

    @pytest.fixture
    def _skip_if_no_datafusion(self):
        """Skip test if DataFusion is not installed."""
        pytest.importorskip("datafusion")

    @pytest.mark.usefixtures("_skip_if_no_datafusion")
    def test_truncated_parquet_raises_readable_error(self, tmp_path):
        """A truncated Parquet file registered with DataFusion raises on query."""
        from datafusion import SessionContext

        # Create a valid Parquet file then truncate it
        valid_file = tmp_path / "data.parquet"
        table = pa.table({"file_path": ["a", "b"], "sequence_number": [1, 2]})
        pq.write_table(table, str(valid_file))

        # Truncate to simulate interrupted write (keep only first 50 bytes of header)
        original_size = valid_file.stat().st_size
        with open(valid_file, "r+b") as f:
            f.truncate(min(50, original_size // 2))

        ctx = SessionContext()

        # DataFusion should raise an error when trying to read the corrupt file.
        # The specific error type varies (ArrowError, ParquetError, etc.) but it
        # must NOT silently return empty results.
        with pytest.raises(Exception) as exc_info:
            ctx.register_parquet("data_entries", str(valid_file))
            ctx.sql("SELECT * FROM data_entries").to_arrow_table()

        # Error message should mention parquet/arrow/file corruption
        error_msg = str(exc_info.value).lower()
        assert any(keyword in error_msg for keyword in ("parquet", "arrow", "eof", "magic", "invalid", "corrupt", "truncate")), (
            f"Error should mention file corruption, got: {exc_info.value}"
        )

    @pytest.mark.usefixtures("_skip_if_no_datafusion")
    def test_empty_parquet_produces_zero_results(self, tmp_path):
        """An empty (valid but zero-row) Parquet file yields zero tasks from the planner."""
        from datafusion import SessionContext

        # Create valid but empty Parquet files
        data_schema = pa.schema(
            [
                pa.field("file_path", pa.string()),
                pa.field("partition_key", pa.string()),
                pa.field("sequence_number", pa.int64()),
                pa.field("record_count", pa.int64()),
                pa.field("spec_id", pa.int32()),
                pa.field("data_file_json", pa.binary()),
            ]
        )
        delete_schema = pa.schema(
            [
                pa.field("file_path", pa.string()),
                pa.field("partition_key", pa.string()),
                pa.field("sequence_number", pa.int64()),
                pa.field("content", pa.int32()),
                pa.field("data_file_json", pa.binary()),
            ]
        )

        data_file = tmp_path / "data_entries.parquet"
        delete_file = tmp_path / "delete_entries.parquet"
        pq.write_table(data_schema.empty_table(), str(data_file))
        pq.write_table(delete_schema.empty_table(), str(delete_file))

        ctx = SessionContext()
        ctx.register_parquet("data_entries", str(data_file))
        ctx.register_parquet("delete_entries", str(delete_file))

        # The assignment SQL should produce zero rows (no data entries → no tasks)
        from pyiceberg.execution.planning import BoundedMemoryPlanner

        result = ctx.sql(BoundedMemoryPlanner._ASSIGNMENT_SQL).to_arrow_table()
        assert result.num_rows == 0


# =============================================================================
# Gap 4: CoW delete two-pass — file disappearing between passes raises
# =============================================================================


class TestCowDeleteTwoPassFailSafe:
    """Verify that if a file disappears between the two passes of CoW delete,
    the operation raises an error (not a silent skip).

    The two-pass streaming path reads the file once to count kept rows, then
    reads again to stream filtered batches to the writer. If concurrent
    compaction + GC removes the file between passes, the second read must FAIL
    so the transaction can be retried against the new table state (OCC pattern).
    """

    def test_file_missing_on_second_pass_raises(self, tmp_path):
        """If a data file disappears between pass 1 and pass 2, an error is raised."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowReadBackend

        read_backend = PyArrowReadBackend()
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        )

        # Create a data file
        data_file = tmp_path / "data.parquet"
        table = pa.table({"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]})
        pq.write_table(table, str(data_file))

        # Pass 1 succeeds: file exists, we read and count
        batches_pass1 = list(
            read_backend.read_parquet(str(data_file), schema, MagicMock(__class__=type("AlwaysTrue", (), {})), {})
        )
        assert sum(b.num_rows for b in batches_pass1) == 5

        # Simulate concurrent compaction + GC removing the file between passes
        data_file.unlink()

        # Pass 2 must FAIL (not silently return empty)
        with pytest.raises((FileNotFoundError, OSError, pa.ArrowInvalid, Exception)):
            list(read_backend.read_parquet(str(data_file), schema, MagicMock(__class__=type("AlwaysTrue", (), {})), {}))

    def test_cow_two_pass_does_not_swallow_read_errors(self):
        """The CoW large-file path must NOT try/except around the second read.

        This is a structural assertion: the code must let FileNotFoundError
        propagate for OCC retry. We verify by checking that _cow_filter_batches
        propagates exceptions from the upstream iterator.
        """
        import pyarrow.compute as pc

        from pyiceberg.execution._orchestrate import _cow_filter_batches

        def failing_iterator():
            yield pa.record_batch({"id": [1, 2]})
            raise FileNotFoundError("File was removed by concurrent compaction")

        pa_filter = pc.field("id") > 0

        # _cow_filter_batches must propagate the FileNotFoundError, not swallow it
        with pytest.raises(FileNotFoundError, match="concurrent compaction"):
            list(_cow_filter_batches(failing_iterator(), pa_filter))
