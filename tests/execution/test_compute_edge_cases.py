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


"""Tests for anti-join, sort, filter edge cases, NULL semantics, and data file serialization."""

from __future__ import annotations

import inspect
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
from pyiceberg.execution.protocol import ComputeBackend
from pyiceberg.expressions import AlwaysFalse
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat


def _try_import_datafusion() -> bool:
    """Check if datafusion is importable (for skipif decorators)."""
    try:
        import datafusion  # noqa: F401

        return True
    except ImportError:
        return False


class TestFilterAlwaysFalse:
    """Verify filter() with AlwaysFalse produces empty output across all backends."""

    @pytest.fixture(params=["pyarrow", "datafusion"])
    def backend(self, request) -> None:
        """Parametrized compute backend."""
        if request.param == "pyarrow":
            return PyArrowComputeBackend()
        elif request.param == "datafusion":
            pytest.importorskip("datafusion")
            from pyiceberg.execution.backends.datafusion_backend import (
                DataFusionComputeBackend,
            )

            return DataFusionComputeBackend()

    def test_filter_always_false_produces_empty(self, backend) -> None:
        """AlwaysFalse filter should yield zero rows from any input."""
        data = pa.table({"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "e"]})
        batches = data.to_batches()

        result = list(backend.filter(iter(batches), AlwaysFalse()))
        total_rows = sum(b.num_rows for b in result)
        assert total_rows == 0, f"AlwaysFalse filter should produce 0 rows, got {total_rows}"

    def test_filter_always_false_empty_input(self, backend) -> None:
        """AlwaysFalse on empty input produces empty output without error."""
        result = list(backend.filter(iter([]), AlwaysFalse()))
        assert result == []


# =============================================================================
# Concurrent _scoped_env_vars thread isolation
# =============================================================================


class TestAntiJoinFromFilesEmptyLeft:
    """T1: Verify anti_join_from_files handles empty left file on disk correctly.

    Current tests cover empty right (returns all left) and empty Iterator inputs,
    but not the case where the left Parquet file exists on disk with zero rows.
    This is a valid edge case when a data file has had all rows deleted by
    positional deletes but the file still exists in the manifest.
    """

    @pytest.fixture(params=["pyarrow", "datafusion"])
    def compute_backend(self, request) -> None:
        """Parametrized compute backend."""
        if request.param == "pyarrow":
            return PyArrowComputeBackend()
        elif request.param == "datafusion":
            pytest.importorskip("datafusion")
            from pyiceberg.execution.backends.datafusion_backend import (
                DataFusionComputeBackend,
            )

            return DataFusionComputeBackend()

    def test_anti_join_from_files_empty_left_returns_empty(self, tmp_path: Path, compute_backend: ComputeBackend) -> None:
        """anti_join_from_files with zero-row left Parquet produces zero output rows."""
        left_path = str(tmp_path / "empty_left.parquet")
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), left_path)

        right_path = str(tmp_path / "right.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3]}), right_path)

        result = list(compute_backend.anti_join_from_files([left_path], [right_path], on=["id"], io_properties={}))
        total_rows = sum(b.num_rows for b in result)
        assert total_rows == 0, f"anti_join_from_files with empty left file should return 0 rows, got {total_rows}"

    def test_anti_join_from_files_empty_right_returns_all_left(self, tmp_path: Path, compute_backend: ComputeBackend) -> None:
        """anti_join_from_files with zero-row right Parquet returns all left rows."""
        left_path = str(tmp_path / "left.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5]}), left_path)

        right_path = str(tmp_path / "empty_right.parquet")
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), right_path)

        result = list(compute_backend.anti_join_from_files([left_path], [right_path], on=["id"], io_properties={}))
        total_rows = sum(b.num_rows for b in result)
        assert total_rows == 5, f"anti_join_from_files with empty right file should return all 5 left rows, got {total_rows}"

    def test_anti_join_from_files_both_empty_returns_empty(self, tmp_path: Path, compute_backend: ComputeBackend) -> None:
        """anti_join_from_files with both files empty returns zero rows."""
        left_path = str(tmp_path / "empty_left.parquet")
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), left_path)

        right_path = str(tmp_path / "empty_right.parquet")
        pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), right_path)

        result = list(compute_backend.anti_join_from_files([left_path], [right_path], on=["id"], io_properties={}))
        total_rows = sum(b.num_rows for b in result)
        assert total_rows == 0


# =============================================================================
# _SortedRecordBatchReader abandoned without full consumption
# =============================================================================


class TestPyArrowAntiJoinFromFilesNullSemantics:
    """Verify PyArrow's anti_join_from_files correctly handles NULL matching.

    Previously only DataFusion and DuckDB were tested for NULL=NULL semantics.
    This tests the actual PyArrow anti_join_from_files call path used when
    DataFusion/DuckDB are not installed.
    """

    def test_pyarrow_anti_join_from_files_null_matches_null(self, tmp_path: Path) -> None:
        """NULL in delete file should match NULL in data file for PyArrow backend."""
        # Data: id=[1, 2, None, 3, None]
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": pa.array([1, 2, None, 3, None], type=pa.int64())}), data_path)

        # Deletes: id=[2, None]
        del_path = str(tmp_path / "deletes.parquet")
        pq.write_table(pa.table({"id": pa.array([2, None], type=pa.int64())}), del_path)

        backend = PyArrowComputeBackend()
        result = pa.Table.from_batches(list(backend.anti_join_from_files([data_path], [del_path], on=["id"], io_properties={})))

        # IS NOT DISTINCT FROM: NULL matches NULL, so id=2 and both NULLs excluded
        result_ids = sorted([v for v in result.column("id").to_pylist() if v is not None])
        assert result_ids == [1, 3]
        # No NULLs should remain
        assert None not in result.column("id").to_pylist()

    def test_pyarrow_anti_join_in_memory_null_matches_null(self, tmp_path: Path) -> None:
        """NULL matching also works for the in-memory anti_join path."""
        backend = PyArrowComputeBackend()

        left = pa.table({"id": pa.array([1, None, 3, None, 5], type=pa.int64())})
        right = pa.table({"id": pa.array([None, 5], type=pa.int64())})

        result = pa.Table.from_batches(list(backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["id"])))

        # NULL and 5 excluded → survivors: [1, 3]
        result_ids = sorted([v for v in result.column("id").to_pylist() if v is not None])
        assert result_ids == [1, 3]
        assert None not in result.column("id").to_pylist()

    def test_pyarrow_anti_join_multi_column_null_handling(self, tmp_path: Path) -> None:
        """Multi-column anti-join with NULLs in composite key.

        Tests the per-row matching algorithm that handles multi-column joins
        correctly, including IS NOT DISTINCT FROM semantics for NULL values.
        """
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(
            pa.table(
                {
                    "region": pa.array(["us", "eu", None, "us", None], type=pa.string()),
                    "id": pa.array([1, 2, 3, 4, 3], type=pa.int64()),
                }
            ),
            data_path,
        )

        del_path = str(tmp_path / "deletes.parquet")
        pq.write_table(
            pa.table(
                {
                    "region": pa.array([None], type=pa.string()),
                    "id": pa.array([3], type=pa.int64()),
                }
            ),
            del_path,
        )

        backend = PyArrowComputeBackend()
        result = pa.Table.from_batches(
            list(backend.anti_join_from_files([data_path], [del_path], on=["region", "id"], io_properties={}))
        )

        # (None, 3) should be excluded
        result_regions = result.column("region").to_pylist()
        result_ids = result.column("id").to_pylist()
        surviving_pairs = list(zip(result_regions, result_ids, strict=False))
        assert ("us", 1) in surviving_pairs
        assert ("eu", 2) in surviving_pairs
        assert ("us", 4) in surviving_pairs


# =============================================================================
# orchestrate_scan -- schema reconciliation with evolved files
# =============================================================================


class TestAntiJoinNullSemanticsStructural:
    """Structural tests: verify anti_join callers pass null_equals_null=True.

    Iceberg equality deletes require IS NOT DISTINCT FROM semantics. If someone
    removes null_equals_null=True from the callers, these tests catch it.
    """

    def test_anti_join_passes_null_equals_null_true(self) -> None:
        """PyArrowComputeBackend.anti_join must call _anti_join_tables with null_equals_null=True."""
        source = inspect.getsource(PyArrowComputeBackend.anti_join)
        assert "null_equals_null=True" in source, (
            "PyArrowComputeBackend.anti_join does not pass null_equals_null=True. "
            "Iceberg equality deletes require IS NOT DISTINCT FROM semantics."
        )

    def test_anti_join_from_files_passes_null_equals_null_true(self) -> None:
        """PyArrowComputeBackend.anti_join_from_files must call _anti_join_tables with null_equals_null=True."""
        source = inspect.getsource(PyArrowComputeBackend.anti_join_from_files)
        assert "null_equals_null=True" in source, (
            "PyArrowComputeBackend.anti_join_from_files does not pass null_equals_null=True. "
            "Iceberg equality deletes require IS NOT DISTINCT FROM semantics."
        )

    def test_apply_positional_deletes_uses_shared_impl(self) -> None:
        """All backends delegate positional deletes to _apply_positional_deletes_impl."""
        source = inspect.getsource(PyArrowComputeBackend.apply_positional_deletes)
        assert "_apply_positional_deletes_impl" in source, (
            "PyArrowComputeBackend.apply_positional_deletes does not delegate to _apply_positional_deletes_impl."
        )


class TestMultiColumnAntiJoinMixedNulls:
    """Verify multi-column anti-join correctness with >2 columns and mixed NULLs.

    The PyArrow backend uses O(left × right) matching for multi-column joins.
    Iceberg spec requires IS NOT DISTINCT FROM semantics (NULL matches NULL).
    These tests verify correctness for complex NULL patterns across 3+ columns.
    """

    def test_three_column_anti_join_basic(self, tmp_path: Path) -> None:
        """Anti-join on 3 columns correctly excludes matching rows."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        left_path = str(tmp_path / "data.parquet")
        pq.write_table(
            pa.table(
                {
                    "region": ["us", "us", "eu", "eu", "ap"],
                    "year": [2024, 2024, 2024, 2023, 2024],
                    "month": [1, 2, 1, 12, 1],
                }
            ),
            left_path,
        )

        right_path = str(tmp_path / "deletes.parquet")
        pq.write_table(
            pa.table(
                {
                    "region": ["us", "eu"],
                    "year": [2024, 2023],
                    "month": [1, 12],
                }
            ),
            right_path,
        )

        backend = PyArrowComputeBackend()
        batches = list(backend.anti_join_from_files([left_path], [right_path], on=["region", "year", "month"], io_properties={}))
        result = pa.Table.from_batches(batches)

        # Row (us, 2024, 1) and (eu, 2023, 12) should be excluded
        assert result.num_rows == 3
        surviving = result.to_pydict()
        assert ("us", 2024, 2) in list(zip(surviving["region"], surviving["year"], surviving["month"], strict=False))
        assert ("eu", 2024, 1) in list(zip(surviving["region"], surviving["year"], surviving["month"], strict=False))
        assert ("ap", 2024, 1) in list(zip(surviving["region"], surviving["year"], surviving["month"], strict=False))

    def test_three_column_anti_join_null_matches_null(self, tmp_path: Path) -> None:
        """NULL in any join column matches NULL in the other side (IS NOT DISTINCT FROM)."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        left_path = str(tmp_path / "data.parquet")
        pq.write_table(
            pa.table(
                {
                    "a": pa.array([1, 2, None, 4, None], type=pa.int64()),
                    "b": pa.array(["x", None, "y", None, None], type=pa.string()),
                    "c": pa.array([10, 20, 30, 40, 50], type=pa.int64()),
                }
            ),
            left_path,
        )

        right_path = str(tmp_path / "deletes.parquet")
        pq.write_table(
            pa.table(
                {
                    "a": pa.array([None, 2], type=pa.int64()),
                    "b": pa.array(["y", None], type=pa.string()),
                    "c": pa.array([30, 20], type=pa.int64()),
                }
            ),
            right_path,
        )

        backend = PyArrowComputeBackend()
        batches = list(backend.anti_join_from_files([left_path], [right_path], on=["a", "b", "c"], io_properties={}))
        result = pa.Table.from_batches(batches)

        # Row (None, "y", 30) matches delete (None, "y", 30) → excluded
        # Row (2, None, 20) matches delete (2, None, 20) → excluded
        # Remaining: (1,"x",10), (4,None,40), (None,None,50)
        assert result.num_rows == 3
        assert 1 in result.column("a").to_pylist()
        assert 4 in result.column("a").to_pylist()
        assert 50 in result.column("c").to_pylist()

    def test_three_column_anti_join_partial_null_no_match(self, tmp_path: Path) -> None:
        """NULL in one column but different values in others → no match."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        left_path = str(tmp_path / "data.parquet")
        pq.write_table(
            pa.table(
                {
                    "a": pa.array([None], type=pa.int64()),
                    "b": pa.array(["x"], type=pa.string()),
                    "c": pa.array([10], type=pa.int64()),
                }
            ),
            left_path,
        )

        right_path = str(tmp_path / "deletes.parquet")
        pq.write_table(
            pa.table(
                {
                    "a": pa.array([None], type=pa.int64()),
                    "b": pa.array(["y"], type=pa.string()),  # different!
                    "c": pa.array([10], type=pa.int64()),
                }
            ),
            right_path,
        )

        backend = PyArrowComputeBackend()
        batches = list(backend.anti_join_from_files([left_path], [right_path], on=["a", "b", "c"], io_properties={}))
        result = pa.Table.from_batches(batches)

        # Column "b" differs ("x" vs "y") → no match, row survives
        assert result.num_rows == 1

    @pytest.mark.skipif(
        not _try_import_datafusion(),
        reason="DataFusion not installed",
    )
    def test_three_column_anti_join_datafusion_matches_pyarrow(self, tmp_path: Path) -> None:
        """DataFusion and PyArrow produce identical results for 3-column mixed-NULL join."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        left_path = str(tmp_path / "data.parquet")
        pq.write_table(
            pa.table(
                {
                    "a": pa.array([1, None, 3, None, 5], type=pa.int64()),
                    "b": pa.array(["x", "y", None, None, "z"], type=pa.string()),
                    "c": pa.array([10, 20, 30, 40, 50], type=pa.int64()),
                }
            ),
            left_path,
        )

        right_path = str(tmp_path / "deletes.parquet")
        pq.write_table(
            pa.table(
                {
                    "a": pa.array([None, 3], type=pa.int64()),
                    "b": pa.array(["y", None], type=pa.string()),
                    "c": pa.array([20, 30], type=pa.int64()),
                }
            ),
            right_path,
        )

        pyarrow_backend = PyArrowComputeBackend()
        df_backend = DataFusionComputeBackend()

        pa_batches = list(pyarrow_backend.anti_join_from_files([left_path], [right_path], on=["a", "b", "c"], io_properties={}))
        df_batches = list(df_backend.anti_join_from_files([left_path], [right_path], on=["a", "b", "c"], io_properties={}))

        pa_result = pa.Table.from_batches(pa_batches)
        df_result = pa.Table.from_batches(df_batches)

        # Both should produce the same surviving rows (order may differ)
        pa_rows = sorted(pa_result.to_pydict()["c"])
        df_rows = sorted(df_result.to_pydict()["c"])
        assert pa_rows == df_rows, (
            f"PyArrow and DataFusion produce different results for 3-column anti-join:\n"
            f"  PyArrow: {pa_rows}\n  DataFusion: {df_rows}"
        )


# =============================================================================
# Section 14 Edge Cases (review pt5)
# =============================================================================


class TestAntiJoinAllNulls:
    """Anti-join where all join column values are NULL on both sides.

    IS NOT DISTINCT FROM semantics: NULL = NULL, so all left rows should be
    excluded when right contains NULL.
    """

    def test_all_nulls_single_column(self) -> None:
        """All-NULL left anti-joined against NULL right produces empty result."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        backend = PyArrowComputeBackend()
        left = pa.table({"key": pa.array([None, None, None], type=pa.int64()), "val": [1, 2, 3]})
        right = pa.table({"key": pa.array([None], type=pa.int64())})

        batches = list(backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))
        if batches:
            result = pa.Table.from_batches(batches)
        else:
            result = pa.table({"key": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.int64())})
        assert result.num_rows == 0

    def test_all_nulls_multi_column(self) -> None:
        """Multi-column all-NULL anti-join also produces empty result."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        backend = PyArrowComputeBackend()
        left = pa.table(
            {
                "a": pa.array([None, None], type=pa.string()),
                "b": pa.array([None, None], type=pa.int32()),
                "val": [10, 20],
            }
        )
        right = pa.table(
            {
                "a": pa.array([None], type=pa.string()),
                "b": pa.array([None], type=pa.int32()),
            }
        )

        batches = list(backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["a", "b"]))
        if batches:
            result = pa.Table.from_batches(batches)
        else:
            result = left.schema.empty_table()
        assert result.num_rows == 0

    def test_mixed_nulls_partial_match(self) -> None:
        """Only rows whose join key matches right (including NULL=NULL) are excluded."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        backend = PyArrowComputeBackend()
        left = pa.table({"key": pa.array([None, 1, 2, None], type=pa.int64()), "val": [10, 20, 30, 40]})
        right = pa.table({"key": pa.array([None, 2], type=pa.int64())})

        result = pa.Table.from_batches(list(backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"])))
        # NULL and 2 are in right, so only key=1 survives
        assert result.column("val").to_pylist() == [20]


class TestSortFromFilesEmptyInput:
    """sort_from_files with an empty file list should return empty, not raise."""

    def test_pyarrow_sort_from_files_empty_list(self) -> None:
        """PyArrow backend handles empty file list gracefully."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        backend = PyArrowComputeBackend()
        result = list(backend.sort_from_files([], [("id", "ascending")], {}))
        assert result == []

    def test_pyarrow_anti_join_from_files_empty_left(self, tmp_path: Path) -> None:
        """Anti-join with empty left produces empty result."""
        import pyarrow.parquet as pq

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend

        backend = PyArrowComputeBackend()

        # Create a minimal right-side file
        right_table = pa.table({"key": [1, 2, 3]})
        right_path = str(tmp_path / "right.parquet")
        pq.write_table(right_table, right_path)

        # Empty left file
        left_table = pa.table({"key": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())})
        left_path = str(tmp_path / "left.parquet")
        pq.write_table(left_table, left_path)

        result = list(backend.anti_join_from_files([left_path], [right_path], ["key"], {}))
        total_rows = sum(b.num_rows for b in result)
        assert total_rows == 0


class TestDataFileSerializationRoundTrip:
    """Verify _serialize_data_file → _deserialize_data_file round-trip preserves all fields.

    This is critical for BoundedMemoryPlanner correctness: DataFile objects are
    serialized to JSON blobs, stored in temp Parquet, passed through a SQL join,
    and deserialized back. All field types must survive the round-trip.
    """

    def test_basic_fields_survive_round_trip(self) -> None:
        """Core fields (path, format, content, counts) survive serialize/deserialize."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="s3://bucket/table/data/part-00000.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record("us-east-1", 2024),
            record_count=10000,
            file_size_in_bytes=1048576,
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.content == DataFileContent.DATA
        assert restored.file_path == "s3://bucket/table/data/part-00000.parquet"
        assert restored.file_format == FileFormat.PARQUET
        assert restored.record_count == 10000
        assert restored.file_size_in_bytes == 1048576

    def test_binary_bounds_survive_round_trip(self) -> None:
        """lower_bounds and upper_bounds (bytes values) survive hex encode/decode."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        lower = {1: b"\x00\x00\x00\x01", 2: b"\x41\x42\x43"}
        upper = {1: b"\x00\x00\x03\xe8", 2: b"\x58\x59\x5a"}

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="s3://bucket/data.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=100,
            file_size_in_bytes=5000,
            lower_bounds=lower,
            upper_bounds=upper,
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.lower_bounds == lower
        assert restored.upper_bounds == upper
        # Verify bytes type (not str)
        for k, v in restored.lower_bounds.items():
            assert isinstance(v, bytes), f"lower_bounds[{k}] should be bytes, got {type(v)}"

    def test_column_statistics_survive_round_trip(self) -> None:
        """column_sizes, value_counts, null_value_counts survive int-key reconstruction."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="data.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=500,
            file_size_in_bytes=10000,
            column_sizes={1: 4096, 2: 8192, 3: 2048},
            value_counts={1: 500, 2: 500, 3: 500},
            null_value_counts={1: 0, 2: 10, 3: 50},
            nan_value_counts={1: 0, 2: 0, 3: 5},
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.column_sizes == {1: 4096, 2: 8192, 3: 2048}
        assert restored.value_counts == {1: 500, 2: 500, 3: 500}
        assert restored.null_value_counts == {1: 0, 2: 10, 3: 50}
        assert restored.nan_value_counts == {1: 0, 2: 0, 3: 5}

    def test_equality_delete_fields_survive_round_trip(self) -> None:
        """Equality delete files with equality_ids and sort_order_id are preserved."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path="s3://bucket/eq-delete-00001.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record("eu-west-1"),
            record_count=50,
            file_size_in_bytes=2000,
            equality_ids=[1, 3, 5],
            sort_order_id=2,
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.content == DataFileContent.EQUALITY_DELETES
        assert restored.equality_ids == [1, 3, 5]
        assert restored.sort_order_id == 2

    def test_partition_values_survive_round_trip(self) -> None:
        """Partition Record values are preserved through JSON serialization."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        # Multi-field partition: string + int + None
        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="data.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record("us-east-1", 2024, None),
            record_count=100,
            file_size_in_bytes=5000,
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.partition[0] == "us-east-1"
        assert restored.partition[1] == 2024
        assert restored.partition[2] is None

    def test_key_metadata_bytes_survive_round_trip(self) -> None:
        """key_metadata (optional bytes field) survives hex encode/decode."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="encrypted.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=100,
            file_size_in_bytes=5000,
            key_metadata=b"\xde\xad\xbe\xef\x00\x01\x02\x03",
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.key_metadata == b"\xde\xad\xbe\xef\x00\x01\x02\x03"
        assert isinstance(restored.key_metadata, bytes)

    def test_spec_id_survives_round_trip(self) -> None:
        """spec_id attribute (set post-construction) survives serialization."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="data.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=100,
            file_size_in_bytes=5000,
        )
        original.spec_id = 3

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.spec_id == 3

    def test_position_delete_file_survives_round_trip(self) -> None:
        """Position delete DataFiles (content=POSITION_DELETES) are preserved."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/table/data/pos-delete-00001.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record("us-east-1"),
            record_count=25000,
            file_size_in_bytes=512000,
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.content == DataFileContent.POSITION_DELETES
        assert restored.file_path == "s3://bucket/table/data/pos-delete-00001.parquet"
        assert restored.record_count == 25000

    def test_split_offsets_survive_round_trip(self) -> None:
        """split_offsets (list[int]) field is preserved through serialization."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="data.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1000000,
            file_size_in_bytes=67108864,
            split_offsets=[0, 16777216, 33554432, 50331648],
        )

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.split_offsets == [0, 16777216, 33554432, 50331648]

    def test_full_datafile_all_fields_round_trip(self) -> None:
        """Comprehensive round-trip: all optional fields populated simultaneously."""
        from pyiceberg.execution.planning import (
            _deserialize_data_file,
            _serialize_data_file,
        )
        from pyiceberg.typedef import Record

        original = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="s3://bucket/ns/table/data/part-00042.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record("eu-west-1", 2024),
            record_count=500000,
            file_size_in_bytes=33554432,
            column_sizes={1: 10000, 2: 20000, 3: 5000},
            value_counts={1: 500000, 2: 500000, 3: 500000},
            null_value_counts={1: 0, 2: 100, 3: 0},
            nan_value_counts={2: 5},
            lower_bounds={1: b"\x00\x00\x00\x01", 2: b"\x41"},
            upper_bounds={1: b"\x00\x07\xa1\x20", 2: b"\x5a"},
            split_offsets=[0, 8388608, 16777216, 25165824],
            sort_order_id=1,
            key_metadata=b"\xca\xfe\xba\xbe",
        )
        original.spec_id = 2

        blob = _serialize_data_file(original)
        restored = _deserialize_data_file(blob)

        assert restored.file_path == original.file_path
        assert restored.content == original.content
        assert restored.file_format == original.file_format
        assert restored.record_count == original.record_count
        assert restored.file_size_in_bytes == original.file_size_in_bytes
        assert restored.column_sizes == original.column_sizes
        assert restored.value_counts == original.value_counts
        assert restored.null_value_counts == original.null_value_counts
        assert restored.nan_value_counts == original.nan_value_counts
        assert restored.lower_bounds == original.lower_bounds
        assert restored.upper_bounds == original.upper_bounds
        assert restored.split_offsets == original.split_offsets
        assert restored.sort_order_id == original.sort_order_id
        assert restored.key_metadata == original.key_metadata
        assert restored.spec_id == original.spec_id
        assert restored.partition == original.partition


# =============================================================================
# Test Gap 5: Multi-column anti-join with mixed NULL patterns
# =============================================================================
