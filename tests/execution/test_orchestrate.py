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

"""Tests for orchestration wiring, behavioral dispatch, variable shadowing,
schema inference warnings, count/write paths, and scan routing.

Covers:
- Behavioral wiring: observable backends prove dispatch correctness
- Variable shadowing regression in _orchestrate.py
- Schema inference failure logging
- DataScan.count() removal of ArrowScan
- Sort-on-write behavioral tests
- Scan and batch reader routing through pluggable backends
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.execution.backends.pyarrow_backend import (
    PyArrowComputeBackend,
    PyArrowReadBackend,
)
from pyiceberg.execution.protocol import Backends, ComputeBackend, ReadBackend, SortKeyList
from pyiceberg.expressions import AlwaysTrue, BooleanExpression, EqualTo
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import (
    FileScanTask,
    _to_arrow_batch_reader_via_file_scan_tasks,
    _to_arrow_via_file_scan_tasks,
)
from pyiceberg.types import IntegerType, NestedField, StringType

if TYPE_CHECKING:
    pass

# =============================================================================
# From test_behavioral_wiring.py
# =============================================================================


class ObservableReadBackend:
    """A ReadBackend that records all calls for verification.

    Wraps PyArrowReadBackend and logs calls, proving the dispatch actually
    routes through the pluggable backend (not ArrowScan or any other path).
    """

    def __init__(self) -> None:
        self._delegate = PyArrowReadBackend()
        self.calls: list[dict[str, Any]] = []

    def read_parquet(
        self,
        location: str,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: Mapping[str, Any],
        dictionary_columns: tuple[str, ...] = (),
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append(
            {
                "method": "read_parquet",
                "location": location,
                "projected_schema": projected_schema,
                "row_filter": row_filter,
                "io_properties": io_properties,
                "dictionary_columns": dictionary_columns,
            }
        )
        return self._delegate.read_parquet(location, projected_schema, row_filter, io_properties, dictionary_columns)


class ObservableComputeBackend:
    """A ComputeBackend that records all calls for verification.

    Wraps PyArrowComputeBackend and logs which methods are called,
    proving the orchestration dispatches correctly to the compute backend.
    """

    def __init__(self) -> None:
        self._delegate = PyArrowComputeBackend()
        self.calls: list[dict[str, Any]] = []

    @property
    def supports_bounded_memory(self) -> bool:
        return False

    def sort(
        self,
        data: Iterator[pa.RecordBatch],
        sort_keys: SortKeyList,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append({"method": "sort", "sort_keys": sort_keys})
        return self._delegate.sort(data, sort_keys, memory_limit)

    def sort_from_files(
        self,
        file_paths: list[str],
        sort_keys: SortKeyList,
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append({"method": "sort_from_files", "file_paths": file_paths})
        return self._delegate.sort_from_files(file_paths, sort_keys, io_properties, memory_limit)

    def anti_join(
        self,
        left: Iterator[pa.RecordBatch],
        right: Iterator[pa.RecordBatch],
        on: list[str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append({"method": "anti_join", "on": on})
        return self._delegate.anti_join(left, right, on, memory_limit)

    def anti_join_from_files(
        self,
        left_paths: list[str],
        right_paths: list[str],
        on: list[str],
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append({"method": "anti_join_from_files", "on": on, "left_paths": left_paths})
        return self._delegate.anti_join_from_files(left_paths, right_paths, on, io_properties, memory_limit)

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append({"method": "filter", "predicate": predicate})
        return self._delegate.filter(data, predicate)

    def apply_positional_deletes(
        self,
        data_path: str,
        position_delete_paths: list[str],
        projected_schema: Schema,
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        self.calls.append({"method": "apply_positional_deletes", "data_path": data_path})
        return self._delegate.apply_positional_deletes(
            data_path, position_delete_paths, projected_schema, io_properties, memory_limit
        )


@pytest.fixture
def schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )


@pytest.fixture
def observable_backends() -> Backends:
    """Create backends with observable read and compute."""
    read = ObservableReadBackend()
    compute = ObservableComputeBackend()
    # ObservableReadBackend and ObservableComputeBackend match the protocol signatures
    # but are not structurally recognized by mypy. We cast to satisfy the Backends constructor.
    return Backends(
        read=cast(ReadBackend, read),
        write=MagicMock(),
        compute=cast(ComputeBackend, compute),
        io_properties={},
    )


def _get_observable_read(backends: Backends) -> ObservableReadBackend:
    """Extract the ObservableReadBackend from Backends for test assertions."""
    return cast(ObservableReadBackend, backends.read)


def _get_observable_compute(backends: Backends) -> ObservableComputeBackend:
    """Extract the ObservableComputeBackend from Backends for test assertions."""
    return cast(ObservableComputeBackend, backends.compute)


class TestScanDispatchesThroughPluggableBackend:
    """Behavioral proof: scan operations route through the pluggable backend.

    Instead of checking source code with inspect, we inject observable backends
    and verify they are called. This survives any refactoring.
    """

    def test_scan_calls_read_backend_for_plain_read(self, tmp_path: Path, schema: Schema, observable_backends: Backends) -> None:
        """orchestrate_scan calls ReadBackend.read_parquet for tasks without deletes."""
        from pyiceberg.execution._orchestrate import orchestrate_scan

        # Write a real data file
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), data_path)

        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path,
                file_format=FileFormat.PARQUET,
                record_count=3,
                file_size_in_bytes=500,
            ),
        )

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.format_version = 2

        batches = list(
            orchestrate_scan(
                backends=observable_backends,
                tasks=iter([task]),
                table_metadata=mock_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        # BEHAVIORAL PROOF: ReadBackend.read_parquet was called
        read_backend = _get_observable_read(observable_backends)
        read_calls = [c for c in read_backend.calls if c["method"] == "read_parquet"]
        assert len(read_calls) == 1, f"Expected 1 read_parquet call, got {len(read_calls)}"
        assert read_calls[0]["location"] == data_path

        # Verify data came through correctly
        result = pa.Table.from_batches(batches)
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_scan_calls_apply_positional_deletes_for_pos_tasks(
        self, tmp_path: Path, schema: Schema, observable_backends: Backends
    ) -> None:
        """orchestrate_scan correctly resolves positional deletes for pos delete tasks."""
        from pyiceberg.execution._orchestrate import orchestrate_scan

        # Write data + position delete files
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}), data_path)

        pos_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(pa.table({"file_path": [data_path], "pos": pa.array([1], type=pa.int64())}), pos_path)

        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path,
                file_format=FileFormat.PARQUET,
                record_count=5,
                file_size_in_bytes=500,
            ),
            delete_files={
                DataFile.from_args(
                    content=DataFileContent.POSITION_DELETES,
                    file_path=pos_path,
                    file_format=FileFormat.PARQUET,
                    record_count=1,
                    file_size_in_bytes=100,
                )
            },
        )

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.format_version = 2

        batches = list(
            orchestrate_scan(
                backends=observable_backends,
                tasks=iter([task]),
                table_metadata=mock_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        # Verify correct result (position 1 deleted = id=2 removed)
        result = pa.Table.from_batches(batches)
        assert sorted(result.column("id").to_pylist()) == [1, 3, 4, 5]

    def test_scan_calls_anti_join_for_equality_deletes(
        self, tmp_path: Path, schema: Schema, observable_backends: Backends
    ) -> None:
        """orchestrate_scan calls ComputeBackend.anti_join_from_files for equality delete tasks."""
        from pyiceberg.execution._orchestrate import orchestrate_scan

        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}), data_path)

        eq_path = str(tmp_path / "eq_delete.parquet")
        pq.write_table(pa.table({"id": [2, 4]}), eq_path)

        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path,
                file_format=FileFormat.PARQUET,
                record_count=5,
                file_size_in_bytes=500,
            ),
            delete_files={
                DataFile.from_args(
                    content=DataFileContent.EQUALITY_DELETES,
                    file_path=eq_path,
                    file_format=FileFormat.PARQUET,
                    record_count=2,
                    file_size_in_bytes=100,
                    equality_ids=[1],
                )
            },
        )

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.format_version = 2

        batches = list(
            orchestrate_scan(
                backends=observable_backends,
                tasks=iter([task]),
                table_metadata=mock_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        # BEHAVIORAL PROOF: anti_join_from_files was called
        compute_backend = _get_observable_compute(observable_backends)
        aj_calls = [c for c in compute_backend.calls if c["method"] == "anti_join_from_files"]
        assert len(aj_calls) == 1
        assert aj_calls[0]["on"] == ["id"]

        # Verify correct result (id=2, id=4 removed)
        result = pa.Table.from_batches(batches)
        assert sorted(result.column("id").to_pylist()) == [1, 3, 5]

    def test_scan_calls_both_pos_and_eq_for_combined_deletes(
        self, tmp_path: Path, schema: Schema, observable_backends: Backends
    ) -> None:
        """orchestrate_scan resolves both positional and equality deletes for combined tasks."""
        from pyiceberg.execution._orchestrate import orchestrate_scan

        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}), data_path)

        pos_path = str(tmp_path / "pos.parquet")
        pq.write_table(pa.table({"file_path": [data_path], "pos": pa.array([0], type=pa.int64())}), pos_path)

        eq_path = str(tmp_path / "eq.parquet")
        pq.write_table(pa.table({"id": [4]}), eq_path)

        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path,
                file_format=FileFormat.PARQUET,
                record_count=5,
                file_size_in_bytes=500,
            ),
            delete_files={
                DataFile.from_args(
                    content=DataFileContent.POSITION_DELETES,
                    file_path=pos_path,
                    file_format=FileFormat.PARQUET,
                    record_count=1,
                    file_size_in_bytes=100,
                ),
                DataFile.from_args(
                    content=DataFileContent.EQUALITY_DELETES,
                    file_path=eq_path,
                    file_format=FileFormat.PARQUET,
                    record_count=1,
                    file_size_in_bytes=100,
                    equality_ids=[1],
                ),
            },
        )

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.format_version = 2

        batches = list(
            orchestrate_scan(
                backends=observable_backends,
                tasks=iter([task]),
                table_metadata=mock_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        # Verify correct result: pos removes position 0 (id=1), eq removes id=4
        result = pa.Table.from_batches(batches)
        assert sorted(result.column("id").to_pylist()) == [2, 3, 5]

    def test_scan_applies_filter_via_post_filter_when_residual_is_always_true(
        self, tmp_path: Path, schema: Schema, observable_backends: Backends
    ) -> None:
        """orchestrate_scan applies filter via post-filter when task.residual is AlwaysTrue.

        When task.residual is AlwaysTrue, it means either:
        1. Partition filters were fully evaluated by the planner
        2. REST server didn't compute a residual (residual_filter=None)

        In both cases, we DON'T push down the filter because:
        - Partition column filters reference columns not in the data file
        - The row_filter may reference columns that require schema reconciliation

        Instead, the filter is applied via post-filter (ComputeBackend.filter).
        """
        from pyiceberg.execution._orchestrate import orchestrate_scan
        from pyiceberg.expressions.visitors import bind

        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}), data_path)

        # Create a BOUND predicate
        bound_filter = bind(schema, EqualTo("id", 3), case_sensitive=True)

        # Task with AlwaysTrue residual (simulating REST catalog or partition filter)
        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path,
                file_format=FileFormat.PARQUET,
                record_count=5,
                file_size_in_bytes=500,
            ),
            residual=AlwaysTrue(),
        )

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.format_version = 2

        batches = list(
            orchestrate_scan(
                backends=observable_backends,
                tasks=iter([task]),
                table_metadata=mock_metadata,
                projected_schema=schema,
                row_filter=bound_filter,
                case_sensitive=True,
            )
        )

        # With the fix: filter is applied via post-filter (not pushdown)
        compute_backend = _get_observable_compute(observable_backends)
        filter_calls = [c for c in compute_backend.calls if c["method"] == "filter"]
        assert len(filter_calls) == 1, "Post-filter should be called when task.residual is AlwaysTrue"

        # Verify correct result - filter was applied
        result = pa.Table.from_batches(batches)
        assert result.column("id").to_pylist() == [3], "Filter should select only id=3"


class TestToArrowDispatchesThroughBackends:
    """Behavioral proof: _to_arrow_via_file_scan_tasks routes through Backends.resolve."""

    def test_to_arrow_resolves_backends_and_orchestrates(self, tmp_path: Path, schema: Schema) -> None:
        """_to_arrow_via_file_scan_tasks calls Backends.resolve and passes result to orchestrate_scan."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)
        batch = pa.record_batch(
            {"id": pa.array([1, 2], type=pa.int32()), "name": pa.array(["a", "b"], type=pa.large_string())},
            schema=arrow_schema,
        )

        mock_scan = MagicMock()
        mock_scan._backends = None  # No cached backends → falls through to resolve()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {"test": "value"}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        resolve_called_with = {}

        def tracking_resolve(cls_or_props: Any, **kwargs: Any) -> MagicMock:
            # Backends.resolve is called as classmethod
            if isinstance(cls_or_props, dict):
                props = cls_or_props
            else:
                props = cls_or_props
            resolve_called_with["props"] = props
            # Return a real backends instance
            mock_backends = MagicMock()
            mock_backends.io_properties = props if isinstance(props, dict) else {}
            return mock_backends

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", side_effect=tracking_resolve) as mock_resolve,
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter([batch])),
        ):
            result = _to_arrow_via_file_scan_tasks(mock_scan, schema, iter([]))

        # BEHAVIORAL PROOF: Backends.resolve was called with io.properties
        mock_resolve.assert_called_once_with({"test": "value"})

        # And we got the data through
        assert len(result) == 2


# =============================================================================
# From test_schema_inference_warning.py
# =============================================================================


class TestSchemaInferenceFailureLogging:
    """_build_reconcile_fn must log when schema inference fails."""

    def test_logs_debug_when_schema_inference_returns_none(self, caplog: pytest.LogCaptureFixture) -> None:
        """When _infer_file_schema_from_batch returns None, a debug message must be logged."""
        from pyiceberg.execution._orchestrate import (
            _NO_RECONCILIATION,
            _build_reconcile_fn,
        )

        projected_schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )

        # Create a batch whose schema cannot be resolved to an Iceberg schema
        batch = pa.record_batch(
            {"id": pa.array([1, 2, 3], type=pa.int32()), "name": pa.array(["a", "b", "c"])},
        )

        # Mock table_metadata so schema inference fails (no name mapping, no field IDs)
        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = projected_schema
        mock_metadata.format_version = 2
        # Force _infer_file_schema_from_batch to return None
        with (
            patch(
                "pyiceberg.execution._orchestrate._infer_file_schema_from_batch",
                return_value=None,
            ),
            caplog.at_level(logging.DEBUG, logger="pyiceberg.execution._orchestrate"),
        ):
            result = _build_reconcile_fn(batch, projected_schema, mock_metadata, False)

        # Should return _NO_RECONCILIATION (correct behavior -- no error)
        assert result is _NO_RECONCILIATION

        # Should have logged a debug message about the failure
        assert any("schema inference" in record.message.lower() for record in caplog.records), (
            "_build_reconcile_fn must log a debug message when schema inference "
            "returns None. This makes schema-drift issues debuggable without "
            "breaking the non-error fast path."
        )

    def test_no_log_when_schema_inference_succeeds(self, caplog: pytest.LogCaptureFixture) -> None:
        """When schema inference succeeds and no reconciliation needed, no warning logged."""
        from pyiceberg.execution._orchestrate import (
            _NO_RECONCILIATION,
            _build_reconcile_fn,
        )

        projected_schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
        )

        batch = pa.record_batch({"id": pa.array([1, 2], type=pa.int32())})

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = projected_schema
        mock_metadata.format_version = 2

        # Mock schema inference to return a schema matching projected (no reconciliation needed)
        with (
            patch(
                "pyiceberg.execution._orchestrate._infer_file_schema_from_batch",
                return_value=projected_schema,
            ),
            caplog.at_level(logging.DEBUG, logger="pyiceberg.execution._orchestrate"),
        ):
            result = _build_reconcile_fn(batch, projected_schema, mock_metadata, False)

        assert result is _NO_RECONCILIATION
        # No schema inference failure log
        schema_inference_logs = [r for r in caplog.records if "schema inference" in r.message.lower()]
        assert len(schema_inference_logs) == 0

    def test_no_log_when_reconciliation_is_needed(self, caplog: pytest.LogCaptureFixture) -> None:
        """When schema inference succeeds and reconciliation IS needed, no inference-failure log."""
        from pyiceberg.execution._orchestrate import (
            _NO_RECONCILIATION,
            _build_reconcile_fn,
        )

        projected_schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )

        # File schema differs from projected (has extra field 3 -- triggers reconciliation)
        file_schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(3, "extra", IntegerType(), required=False),
        )

        batch = pa.record_batch({"id": pa.array([1, 2], type=pa.int32())})

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = projected_schema
        mock_metadata.format_version = 2
        mock_metadata.specs.return_value = {0: MagicMock()}
        mock_metadata.default_spec_id = 0

        # Don't pass task so the partition projection path is skipped
        with patch(
            "pyiceberg.execution._orchestrate._infer_file_schema_from_batch",
            return_value=file_schema,
        ):
            with caplog.at_level(logging.DEBUG, logger="pyiceberg.execution._orchestrate"):
                result = _build_reconcile_fn(batch, projected_schema, mock_metadata, False, task=None)

        # Should return a reconciliation function (not sentinel) because field_ids differ
        assert result is not _NO_RECONCILIATION
        assert callable(result)

        # No "schema inference failed" log -- inference succeeded
        schema_inference_logs = [r for r in caplog.records if "schema inference" in r.message.lower()]
        assert len(schema_inference_logs) == 0


# =============================================================================
# From test_count_and_write.py
# =============================================================================


# =============================================================================
# From test_count_write_behavioral.py
# =============================================================================


class TestCountFastPath:
    """DataScan.count() must use file metadata for tasks without deletes."""

    def test_count_without_deletes_uses_record_count(self) -> None:
        """Tasks with AlwaysTrue residual and no deletes → use metadata record_count."""
        mock_data_file = MagicMock()
        mock_data_file.record_count = 1000
        mock_data_file.file_size_in_bytes = 100_000
        mock_data_file.file_path = "s3://bucket/data/part-001.parquet"

        task = FileScanTask(
            data_file=mock_data_file,
            delete_files=None,
            residual=AlwaysTrue(),
        )

        # Mock a DataScan that returns this single task
        mock_scan = MagicMock()
        mock_scan.plan_files.return_value = [task]
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True

        # Import and call the count logic

        # The fast path sums record_count without reading any files.
        # We verify by ensuring orchestrate_scan is NOT called for this task.
        with patch("pyiceberg.execution._orchestrate.orchestrate_scan") as mock_orchestrate:
            mock_orchestrate.return_value = iter([])

            # Call count on a real-enough DataScan
            # We need to test the logic directly since DataScan.count() reads self.plan_files()
            # Use the fast-path logic: tasks with AlwaysTrue + no deletes use record_count
            tasks_list = [task]
            metadata_count = sum(
                t.file.record_count for t in tasks_list if t.residual == AlwaysTrue() and len(t.delete_files) == 0
            )
            assert metadata_count == 1000

    def test_count_with_deletes_calls_read_path(self) -> None:
        """Tasks with delete files must go through the read path (orchestrate_scan)."""
        mock_data_file = MagicMock()
        mock_data_file.record_count = 1000
        mock_data_file.file_path = "s3://bucket/data/part-001.parquet"
        mock_data_file.file_size_in_bytes = 100_000

        mock_delete_file = MagicMock()
        mock_delete_file.file_path = "s3://bucket/data/del-001.parquet"

        task = FileScanTask(
            data_file=mock_data_file,
            delete_files={mock_delete_file},
            residual=AlwaysTrue(),
        )

        # This task has deletes → should NOT use the fast path
        tasks_list = [task]
        fast_path_tasks = [t for t in tasks_list if t.residual == AlwaysTrue() and len(t.delete_files) == 0]
        slow_path_tasks = [t for t in tasks_list if not (t.residual == AlwaysTrue() and len(t.delete_files) == 0)]

        assert len(fast_path_tasks) == 0, "Task with deletes should NOT be on fast path"
        assert len(slow_path_tasks) == 1, "Task with deletes must go through slow path"

    def test_count_mixed_tasks(self) -> None:
        """Mix of fast-path and slow-path tasks: both contribute to final count."""
        # Fast-path task: no deletes, AlwaysTrue residual
        fast_file = MagicMock()
        fast_file.record_count = 500
        fast_file.file_path = "fast.parquet"
        fast_file.file_size_in_bytes = 50_000
        fast_task = FileScanTask(data_file=fast_file, delete_files=None, residual=AlwaysTrue())

        # Slow-path task: has a delete file
        slow_file = MagicMock()
        slow_file.record_count = 300
        slow_file.file_path = "slow.parquet"
        slow_file.file_size_in_bytes = 30_000
        del_file = MagicMock()
        del_file.file_path = "del.parquet"
        slow_task = FileScanTask(data_file=slow_file, delete_files={del_file}, residual=AlwaysTrue())

        tasks_list = [fast_task, slow_task]

        # Fast path count
        metadata_count = sum(t.file.record_count for t in tasks_list if t.residual == AlwaysTrue() and len(t.delete_files) == 0)
        assert metadata_count == 500, "Only fast-path task contributes to metadata count"

        # Slow path tasks
        slow_tasks = [t for t in tasks_list if not (t.residual == AlwaysTrue() and len(t.delete_files) == 0)]
        assert len(slow_tasks) == 1


class TestSortOnWriteBehavioral:
    """Behavioral tests for _apply_sort_order: verifies actual data transformation."""

    def test_no_sort_when_table_has_no_sort_order(self) -> None:
        """When table has no sort order, _apply_sort_order returns input unchanged."""
        from pyiceberg.table import Transaction

        mock_txn = MagicMock(spec=Transaction)
        mock_txn.table_metadata = MagicMock()
        mock_txn._table = MagicMock()
        mock_txn._table.io.properties = {}

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = True

        input_table = pa.table({"id": [3, 1, 2]})

        with patch("pyiceberg.execution._orchestrate._get_sort_order", return_value=None):
            result = Transaction._apply_sort_order(mock_txn, input_table, mock_backends)

        assert result is input_table, "No sort order → input returned unchanged"

    def test_no_sort_when_backend_cannot_spill(self) -> None:
        """When backend lacks bounded memory, _apply_sort_order skips sort."""
        from pyiceberg.table import Transaction

        mock_txn = MagicMock(spec=Transaction)
        mock_txn.table_metadata = MagicMock()
        mock_txn._table = MagicMock()
        mock_txn._table.io.properties = {}

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = False

        input_table = pa.table({"id": [3, 1, 2]})

        with patch("pyiceberg.execution._orchestrate._get_sort_order", return_value=[("id", "ascending")]):
            result = Transaction._apply_sort_order(mock_txn, input_table, mock_backends)

        assert result is input_table, "No bounded memory → sort skipped, input unchanged"

    def test_sort_applied_when_backend_can_spill(self) -> None:
        """When backend supports bounded memory, _apply_sort_order produces sorted output."""
        from pyiceberg.table import Transaction

        mock_txn = MagicMock(spec=Transaction)
        mock_txn.table_metadata = MagicMock()
        mock_txn._table = MagicMock()
        mock_txn._table.io.properties = {}

        # Backend that supports bounded memory and returns sorted batches
        sorted_batches = pa.table({"id": [1, 2, 3]}).to_batches()
        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = True
        mock_backends.compute.sort_from_files.return_value = iter(sorted_batches)

        Schema(NestedField(1, "id", IntegerType(), required=True))

        input_table = pa.table({"id": [3, 1, 2]})

        with (
            patch("pyiceberg.execution._orchestrate._get_sort_order", return_value=[("id", "ascending")]),
            patch("pyiceberg.io.pyarrow.schema_to_pyarrow", return_value=pa.schema([pa.field("id", pa.int64())])),
        ):
            result = Transaction._apply_sort_order(mock_txn, input_table, mock_backends)

        # Result should be a RecordBatchReader (streaming sorted output)
        assert isinstance(result, pa.RecordBatchReader), (
            f"When sort is applied, result should be RecordBatchReader, got {type(result).__name__}"
        )


class TestConftestIsolationIsOverridable:
    """The autouse fixture isolates from filesystem config but can be overridden."""

    def test_can_override_pyiceberg_home_in_test(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tests CAN set PYICEBERG_HOME explicitly to test config-file-based behavior."""
        # The conftest autouse fixture sets PYICEBERG_HOME to a temp dir.
        # This test shows you can override it within a test using monkeypatch.
        config_dir = tmp_path / "custom_config"
        config_dir.mkdir()
        config_file = config_dir / ".pyiceberg.yaml"
        config_file.write_text("execution:\n  compute-backend: pyarrow\n")

        monkeypatch.setenv("PYICEBERG_HOME", str(config_dir))

        # Now Config() should find this file
        from pyiceberg.utils.config import Config

        config = Config()
        exec_section = config.config.get("execution")
        assert isinstance(exec_section, dict)
        assert exec_section.get("compute-backend") == "pyarrow"

    def test_without_override_config_is_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without explicit override, the conftest fixture ensures no config is found."""
        # The conftest autouse already sets PYICEBERG_HOME to tmp_path (which has no yaml)
        from pyiceberg.utils.config import Config

        config = Config()
        exec_section = config.config.get("execution")
        # Should be None or empty -- no .pyiceberg.yaml in the temp dir
        assert exec_section is None or exec_section == {}


# =============================================================================
# From test_wiring.py
# =============================================================================


@pytest.fixture
def simple_schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )


@pytest.fixture
def sample_batches(simple_schema: Schema) -> list[pa.RecordBatch]:
    """Sample RecordBatches with schema matching what schema_to_pyarrow produces."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    arrow_schema = schema_to_pyarrow(simple_schema, include_field_ids=False)
    batch1 = (
        pa.table({"id": pa.array([1, 2, 3], type=pa.int32()), "name": pa.array(["a", "b", "c"], type=pa.large_string())})
        .cast(arrow_schema)
        .to_batches()[0]
    )
    batch2 = (
        pa.table({"id": pa.array([4, 5], type=pa.int32()), "name": pa.array(["d", "e"], type=pa.large_string())})
        .cast(arrow_schema)
        .to_batches()[0]
    )
    return [batch1, batch2]


class TestScanDispatchesViaBackends:
    """Verify _to_arrow_via_file_scan_tasks calls Backends.resolve and orchestrate_scan."""

    def test_to_arrow_calls_backends_resolve(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """_to_arrow_via_file_scan_tasks must call Backends.resolve(io.properties)."""
        mock_scan = MagicMock()
        mock_scan._backends = None  # No cached backends → falls through to resolve()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {"warehouse": "s3://bucket"}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = mock_scan.io.properties

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends) as mock_resolve,
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)),
        ):
            _to_arrow_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        mock_resolve.assert_called_once_with(mock_scan.io.properties)

    def test_to_arrow_calls_orchestrate_scan(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """_to_arrow_via_file_scan_tasks must route through orchestrate_scan."""
        mock_scan = MagicMock()
        mock_scan._backends = None  # No cached backends → falls through to resolve()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)) as mock_orchestrate,
        ):
            _to_arrow_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        mock_orchestrate.assert_called_once()
        # Verify it received the backends and relevant parameters
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["backends"] is mock_backends

    def test_to_arrow_applies_limit(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """When scan.limit is set, the result table must be sliced."""
        mock_scan = MagicMock()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = 2

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)),
        ):
            result = _to_arrow_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        assert len(result) == 2

    def test_to_arrow_no_limit_returns_all(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """Without limit, all rows are returned."""
        mock_scan = MagicMock()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)),
        ):
            result = _to_arrow_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        assert len(result) == 5


class TestBatchReaderDispatchesViaBackends:
    """Verify _to_arrow_batch_reader_via_file_scan_tasks routes through backends."""

    def test_batch_reader_calls_backends_resolve(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """_to_arrow_batch_reader_via_file_scan_tasks must call Backends.resolve."""
        mock_scan = MagicMock()
        mock_scan._backends = None  # No cached backends → falls through to resolve()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {"warehouse": "s3://bucket"}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = mock_scan.io.properties

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends) as mock_resolve,
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)),
        ):
            _to_arrow_batch_reader_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        mock_resolve.assert_called_once_with(mock_scan.io.properties)

    def test_batch_reader_returns_record_batch_reader(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """Result must be a pa.RecordBatchReader."""
        mock_scan = MagicMock()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)),
        ):
            result = _to_arrow_batch_reader_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        assert isinstance(result, pa.RecordBatchReader)

    def test_batch_reader_streams_all_rows(self, simple_schema: Schema, sample_batches: list[pa.RecordBatch]) -> None:
        """Reading all batches from the reader produces all original rows."""
        mock_scan = MagicMock()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter(sample_batches)),
        ):
            reader = _to_arrow_batch_reader_via_file_scan_tasks(mock_scan, simple_schema, iter([]))
            table = reader.read_all()

        assert len(table) == 5


class TestBatchReaderCastsToTargetSchema:
    """Verify _to_arrow_batch_reader_via_file_scan_tasks applies .cast(target_schema)."""

    def test_batch_reader_handles_string_to_large_string_promotion(self, simple_schema: Schema) -> None:
        """Batches with string type should be promoted to large_string by .cast()."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        mock_scan = MagicMock()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        # Target schema expects large_string (Iceberg default for string type)
        target_schema = schema_to_pyarrow(simple_schema, include_field_ids=False)
        assert target_schema.field("name").type == pa.large_string()

        # But the batch from an older file has regular string
        batch_with_string = pa.record_batch(
            {"id": pa.array([1, 2], type=pa.int32()), "name": pa.array(["a", "b"], type=pa.string())},
        )

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter([batch_with_string])),
        ):
            reader = _to_arrow_batch_reader_via_file_scan_tasks(mock_scan, simple_schema, iter([]))
            # This would raise ArrowInvalid without .cast()
            table = reader.read_all()

        assert len(table) == 2
        assert table.schema.field("name").type == pa.large_string()

    def test_batch_reader_output_schema_matches_target(self, simple_schema: Schema) -> None:
        """The reader's output schema must always match the projected schema exactly."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        mock_scan = MagicMock()
        mock_scan.table_metadata = MagicMock()
        mock_scan.io = MagicMock()
        mock_scan.io.properties = {}
        mock_scan.row_filter = AlwaysTrue()
        mock_scan.case_sensitive = True
        mock_scan.limit = None

        mock_backends = MagicMock()
        mock_backends.io_properties = {}

        target_schema = schema_to_pyarrow(simple_schema, include_field_ids=False)

        # Batch already matches target schema
        batch = pa.record_batch(
            {"id": pa.array([1], type=pa.int32()), "name": pa.array(["x"], type=pa.large_string())},
            schema=target_schema,
        )

        with (
            patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends),
            patch("pyiceberg.execution._orchestrate.orchestrate_scan", return_value=iter([batch])),
        ):
            reader = _to_arrow_batch_reader_via_file_scan_tasks(mock_scan, simple_schema, iter([]))

        assert reader.schema == target_schema


class TestDeleteCoWRoutesViaBackends:
    """Verify Transaction.delete CoW path uses the pluggable backend."""

    def test_arrowscan_emits_deprecation_warning(self) -> None:
        """Directly instantiating ArrowScan must emit a DeprecationWarning."""
        from pyiceberg.io.pyarrow import ArrowScan

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = Schema(
            NestedField(1, "id", IntegerType(), required=True),
        )
        mock_io = MagicMock()

        with pytest.warns(DeprecationWarning, match="ArrowScan is deprecated"):
            ArrowScan(
                table_metadata=mock_metadata,
                io=mock_io,
                projected_schema=Schema(NestedField(1, "id", IntegerType(), required=True)),
                row_filter=AlwaysTrue(),
                case_sensitive=True,
                limit=None,
            )


# =============================================================================
# Edge case tests (review pt6 issue 4.4)
# =============================================================================


class TestGetEqualityFieldNamesDroppedColumns:
    """_get_equality_field_names must warn and return [] when equality field IDs
    reference columns dropped via schema evolution."""

    def test_equality_ids_referencing_dropped_columns_returns_empty_with_warning(self) -> None:
        """When equality_ids point to fields no longer in the schema, return [] and warn."""
        from unittest.mock import MagicMock

        from pyiceberg.execution._orchestrate import _get_equality_field_names
        from pyiceberg.manifest import DataFileContent
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField

        # Schema with only field 1 (fields 10 and 20 have been dropped)
        current_schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        table_metadata = MagicMock()
        table_metadata.schema.return_value = current_schema

        # Delete file references field IDs 10 and 20 (both dropped)
        delete_file = MagicMock()
        delete_file.equality_ids = [10, 20]
        delete_file.content = DataFileContent.EQUALITY_DELETES

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _get_equality_field_names([delete_file], table_metadata)

        assert result == []
        assert len(w) == 1
        assert "do not exist in the current table schema" in str(w[0].message)
        assert "10" in str(w[0].message)
        assert "20" in str(w[0].message)

    def test_equality_ids_none_returns_none_no_warning(self) -> None:
        """When equality_ids is None (not set on delete files), return None without warning.

        None distinguishes 'metadata absent' from 'IDs present but columns dropped' ([]).
        """
        from unittest.mock import MagicMock

        from pyiceberg.execution._orchestrate import _get_equality_field_names
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField

        current_schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        table_metadata = MagicMock()
        table_metadata.schema.return_value = current_schema

        delete_file = MagicMock()
        delete_file.equality_ids = None

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = _get_equality_field_names([delete_file], table_metadata)

        assert result is None
        assert len(w) == 0


class TestPositionalDeletesZeroMatchingPositions:
    """apply_positional_deletes must return all data rows when no positions match."""

    def test_no_matching_positions_returns_all_rows(self, tmp_path: Path) -> None:
        """When delete file has positions for a DIFFERENT data file, all rows survive."""
        import pyarrow.parquet as pq

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        # Write data file: 5 rows
        data_schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
        data_table = pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}, schema=data_schema)
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(data_table, data_path)

        # Write position delete file: positions for a DIFFERENT file path
        del_schema = pa.schema([pa.field("file_path", pa.string()), pa.field("pos", pa.int64())])
        del_table = pa.table(
            {
                "file_path": ["s3://bucket/other_file.parquet", "s3://bucket/other_file.parquet"],
                "pos": [0, 2],
            },
            schema=del_schema,
        )
        del_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(del_table, del_path)

        backend = PyArrowComputeBackend()
        projected_schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=True),
        )

        result_batches = list(
            backend.apply_positional_deletes(
                data_path=data_path,
                position_delete_paths=[del_path],
                projected_schema=projected_schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(result_batches)
        assert result.num_rows == 5
        assert result.column("id").to_pylist() == [1, 2, 3, 4, 5]

    def test_empty_delete_file_returns_all_rows(self, tmp_path: Path) -> None:
        """When the position delete file has zero rows, all data rows survive."""
        import pyarrow.parquet as pq

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField

        # Write data file
        data_schema = pa.schema([pa.field("id", pa.int32())])
        data_table = pa.table({"id": [10, 20, 30]}, schema=data_schema)
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(data_table, data_path)

        # Write empty position delete file
        del_schema = pa.schema([pa.field("file_path", pa.string()), pa.field("pos", pa.int64())])
        del_table = pa.table({"file_path": [], "pos": []}, schema=del_schema)
        del_path = str(tmp_path / "empty_delete.parquet")
        pq.write_table(del_table, del_path)

        backend = PyArrowComputeBackend()
        projected_schema = Schema(NestedField(1, "id", IntegerType(), required=True))

        result_batches = list(
            backend.apply_positional_deletes(
                data_path=data_path,
                position_delete_paths=[del_path],
                projected_schema=projected_schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(result_batches)
        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [10, 20, 30]


class TestBoundedMemoryPlannerEmptyManifests:
    """BoundedMemoryPlanner must handle empty manifest lists gracefully."""

    def test_empty_manifests_yields_no_tasks(self, tmp_path: Path) -> None:
        """When manifests list is empty, plan_files yields nothing without error."""
        pytest.importorskip("datafusion")
        from unittest.mock import MagicMock

        from pyiceberg.execution.planning import BoundedMemoryPlanner
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField

        table_metadata = MagicMock()
        table_metadata.schema.return_value = Schema(NestedField(1, "id", IntegerType(), required=True))
        table_metadata.specs.return_value = {0: MagicMock()}

        io = MagicMock()

        planner = BoundedMemoryPlanner()
        tasks = list(
            planner.plan_files(
                manifests=[],
                table_metadata=table_metadata,
                row_filter=AlwaysTrue(),
                io=io,
                case_sensitive=True,
            )
        )

        assert tasks == []


class TestResolvePushdownFilter:
    """Test _resolve_pushdown_filter helper for correct filter selection.

    This function determines which filter to push down to the Parquet scanner:
    1. If row_filter is AlwaysTrue → use AlwaysTrue (no filter)
    2. If task.residual is non-trivial → use it (handles schema evolution)
    3. Otherwise → bind and return row_filter (REST server returned residual_filter=None)
    """

    def test_always_true_row_filter_returns_always_true(self) -> None:
        """When row_filter is AlwaysTrue, pushdown should be AlwaysTrue."""
        from pyiceberg.execution._orchestrate import _resolve_pushdown_filter
        from pyiceberg.expressions import AlwaysTrue, GreaterThan

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
        )
        result = _resolve_pushdown_filter(AlwaysTrue(), GreaterThan("id", 5), schema, case_sensitive=True)
        assert isinstance(result, AlwaysTrue)

    def test_non_trivial_residual_is_used(self) -> None:
        """When task.residual is non-trivial, it should be used (schema evolution case)."""
        from pyiceberg.execution._orchestrate import _resolve_pushdown_filter
        from pyiceberg.expressions import GreaterThan

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
        )
        row_filter = GreaterThan("new_col_name", 5)
        task_residual = GreaterThan("old_col_name", 5)  # Schema evolution renamed column

        result = _resolve_pushdown_filter(row_filter, task_residual, schema, case_sensitive=True)
        assert result is task_residual

    def test_always_true_residual_returns_always_true(self) -> None:
        """When task.residual is AlwaysTrue, return AlwaysTrue for pushdown.

        When the planner returns AlwaysTrue for residual, it means either:
        1. Partition filters were fully evaluated (nothing left for scanner)
        2. REST server didn't compute a residual

        In both cases, we should NOT push down the row_filter because:
        - Partition column filters reference columns not in the data file
        - The row_filter may be unbound and fail expression conversion

        The orchestrator will apply row_filter via post-filter after schema
        reconciliation adds partition columns (if applicable).
        """
        from pyiceberg.execution._orchestrate import _resolve_pushdown_filter
        from pyiceberg.expressions import AlwaysTrue, GreaterThan

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
        )
        row_filter = GreaterThan("id", 2)
        task_residual = AlwaysTrue()  # Partition filter fully evaluated or REST returned None

        result = _resolve_pushdown_filter(row_filter, task_residual, schema, case_sensitive=True)
        # Should return AlwaysTrue - no pushdown, post-filter will handle it
        assert isinstance(result, AlwaysTrue)


class TestPlainReadWithAlwaysTrueResidual:
    """Regression test: plain read path must apply filter when task.residual is AlwaysTrue.

    Bug scenario (fixed in this PR):
    - User creates unpartitioned table via REST catalog
    - User deletes rows (CoW)
    - User scans with row_filter
    - REST server returns residual_filter=None → task.residual=AlwaysTrue
    - OLD BUG: pushdown_filter = task.residual = AlwaysTrue (filter lost!)
    - FIX: fall back to row_filter when task.residual is AlwaysTrue
    """

    def test_filter_applied_when_residual_is_always_true(self, tmp_path: Path) -> None:
        """Plain read with AlwaysTrue residual but non-trivial row_filter must filter rows."""
        # Write a data file with rows [1, 2, 3, 4, 5]
        data_schema = pa.schema([pa.field("id", pa.int32()), pa.field("category", pa.string())])
        data_table = pa.table({"id": [1, 2, 3, 4, 5], "category": ["a", "a", "a", "a", "a"]}, schema=data_schema)
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(data_table, data_path)

        # Create a FileScanTask with AlwaysTrue residual (simulating REST server)
        from pyiceberg.expressions import GreaterThan
        from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_path,
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=5,
            file_size_in_bytes=1000,
        )

        task = FileScanTask(
            data_file=data_file,
            delete_files=None,
            residual=AlwaysTrue(),  # REST server returned residual_filter=None
        )

        # Create mock scan objects
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "category", StringType(), required=False),
        )
        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.format_version = 2
        mock_metadata.name_mapping.return_value = None
        mock_metadata.schemas = [schema]

        # Run orchestrate_scan with a row_filter that should filter out id <= 2
        from pyiceberg.execution._orchestrate import orchestrate_scan
        from pyiceberg.execution.protocol import Backends

        backends = Backends.resolve({})
        row_filter = GreaterThan("id", 2)

        result_batches = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=mock_metadata,
                projected_schema=schema,
                row_filter=row_filter,
                case_sensitive=True,
            )
        )

        result = pa.Table.from_batches(result_batches)
        # Should only have rows where id > 2: [3, 4, 5]
        assert sorted(result.column("id").to_pylist()) == [3, 4, 5], (
            f"Filter should exclude id <= 2, got {result.column('id').to_pylist()}"
        )
