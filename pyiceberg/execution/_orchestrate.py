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

"""Orchestration: routes table operations through resolved backends.

This module contains the dispatch logic that connects table operations to the
pluggable backend protocols. It handles:
- Per-task scan execution (read + delete resolution + filter + reconcile)
- CoW delete execution (read + complement filter + write, streaming)
- Data file writing (optional sort + streaming write via write_data_files)

All Iceberg-specific logic (delete file classification, schema reconciliation,
sort order resolution) lives here. Backends receive only generic instructions
(read file, filter batches, sort files, write batches).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Executor
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER_ID

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.execution.protocol import Backends, SortKeyList
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Default positional delete file size threshold (1 MB). Below this, the PyArrow
#: set-based approach is used. Above, the DataFusion bounded-memory path kicks in.
#: Configurable via execution.pos-delete-threshold in .pyiceberg.yaml (bytes)
#: or PYICEBERG_EXECUTION__POS_DELETE_THRESHOLD env var.
_POS_DELETE_THRESHOLD_DEFAULT: int = 1 * 1024 * 1024


def _get_pos_delete_threshold() -> int:
    """Read the positional delete routing threshold from config or default (1 MB).

    Below this threshold (total compressed size of pos delete files), the PyArrow
    set-based approach is used. Above, DataFusion's bounded-memory path kicks in.
    """
    from pyiceberg.execution.engine import get_execution_config_int

    return get_execution_config_int("pos-delete-threshold", _POS_DELETE_THRESHOLD_DEFAULT)


#: Sentinel object returned by _build_reconcile_fn when the batch's schema already
#: matches the projected schema (no reconciliation needed). Distinct from a callable
#: to avoid the overhead of an identity-function call on every batch in the common case.
_NO_RECONCILIATION = object()  # Sentinel: schema already matches, skip reconciliation

# Configurable via execution.spill-batch-threshold.
_SPILL_BATCH_THRESHOLD_DEFAULT: int = 4


def _get_spill_batch_threshold() -> int:
    """Read the spill batch threshold from config or default (4 batches)."""
    from pyiceberg.execution.engine import get_execution_config_int

    return get_execution_config_int("spill-batch-threshold", _SPILL_BATCH_THRESHOLD_DEFAULT)


def _get_max_inflight_tasks(executor: Executor) -> int:
    """Determine the maximum number of in-flight tasks for bounded submission."""
    max_workers = getattr(executor, "_max_workers", None)
    if max_workers is None:
        import os

        max_workers = os.cpu_count() or 4
    return max_workers * 2


def _bounded_map(
    executor: Executor,
    fn: Callable[[FileScanTask], list[pa.RecordBatch]],
    items: Iterator[FileScanTask],
    max_inflight: int,
) -> Iterator[list[pa.RecordBatch]]:
    """Submit tasks with bounded concurrency, yielding results in submission order.

    Limits the number of in-flight futures to prevent the thread pool from
    accumulating completed results faster than the caller consumes them
    (which would exhaust memory for large scans).

    Args:
        executor: Thread pool executor for parallel task execution.
        fn: Function to apply to each item (returns list of RecordBatch).
        items: Iterator of FileScanTasks to process.
        max_inflight: Maximum concurrent futures before blocking on the oldest.
            Typically 2× the thread pool size to keep workers saturated while
            bounding memory from queued results.
    """
    from collections import deque
    from concurrent.futures import Future

    inflight: deque[Future[list[pa.RecordBatch]]] = deque()

    for item in items:
        inflight.append(executor.submit(fn, item))

        if len(inflight) >= max_inflight:
            yield inflight.popleft().result()

    while inflight:
        yield inflight.popleft().result()


def orchestrate_scan(
    backends: Backends,
    tasks: Iterator[FileScanTask],
    table_metadata: TableMetadata,
    projected_schema: Schema,
    row_filter: BooleanExpression,
    case_sensitive: bool = True,
    dictionary_columns: tuple[str, ...] = (),
    streaming: bool = False,
) -> Iterator[pa.RecordBatch]:
    """Execute scan tasks through the resolved backends with parallel execution.

    Yields RecordBatches from each task with deletes resolved and filter applied.

    Args:
        backends: Resolved read/write/compute backend instances.
        tasks: Iterator of FileScanTasks from the planner.
        table_metadata: Table metadata for schema reconciliation.
        projected_schema: Desired output schema (column projection).
        row_filter: Row-level filter expression.
        case_sensitive: Whether to use case-sensitive column matching.
        dictionary_columns: Column names to read as dictionary-encoded.
        streaming: If True, spill large task results to temp Parquet for O(batch_size) memory.

    Yields:
        RecordBatches with deletes resolved, filter applied, and schema reconciled.
    """
    # Resolve once per scan.
    from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
    from pyiceberg.utils.concurrent import ExecutorFactory
    from pyiceberg.utils.config import Config

    io_properties = backends.io_properties
    downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False

    # Schema inference cache keyed by metadata-stripped Arrow schema.
    # Thread-safe via lock for concurrent thread pool execution.
    # The cached computation is idempotent: same Arrow schema always produces
    # the same Iceberg Schema, so concurrent races on the same key are harmless.
    schema_cache: dict[pa.Schema, Schema | None] = {}
    schema_cache_lock = threading.Lock()

    def _execute_task(task: FileScanTask) -> list[pa.RecordBatch]:
        """Execute a single scan task: read, resolve deletes, filter, reconcile schema."""
        eq_deletes = [d for d in task.delete_files if d.content == DataFileContent.EQUALITY_DELETES]
        # Only include Parquet position delete files. Puffin files (delete vectors in v3)
        # require different handling that's not yet implemented - they will be silently
        # skipped, which may return a superset of correct results.
        pos_deletes = [
            d for d in task.delete_files if d.content == DataFileContent.POSITION_DELETES and d.file_format == FileFormat.PARQUET
        ]

        # Warn if there are unsupported delete file formats (e.g., Puffin DVs)
        unsupported_deletes = [
            d for d in task.delete_files if d.content == DataFileContent.POSITION_DELETES and d.file_format != FileFormat.PARQUET
        ]
        if unsupported_deletes:
            import warnings

            formats = {d.file_format.name for d in unsupported_deletes}
            warnings.warn(
                f"Skipping {len(unsupported_deletes)} position delete file(s) with unsupported format(s): {formats}. "
                f"Delete vectors (Puffin files) in Iceberg v3 are not yet supported. "
                f"Results may include rows that should have been deleted.",
                UserWarning,
                stacklevel=2,
            )

        if pos_deletes and eq_deletes:
            batches: Iterator[pa.RecordBatch] = _apply_positional_deletes(
                backends,
                task,
                pos_deletes,
                projected_schema,
                io_properties,
            )
            eq_cols = _get_equality_field_names(eq_deletes, table_metadata)

            if eq_cols is None:
                import warnings

                warnings.warn(
                    "Equality delete files do not specify equality_ids. "
                    "Cannot apply equality deletes -- returning superset of correct results. "
                    "This may include rows that should have been deleted.",
                    UserWarning,
                    stacklevel=2,
                )
            elif not eq_cols:
                # equality_ids present but all referenced columns dropped via schema
                # evolution. _get_equality_field_names already emitted a warning.
                # Skip anti-join (no columns to join on); results are a superset.
                pass
            elif backends.supports_bounded_memory:
                from pyiceberg.execution.materialize import (
                    materialize_batches_to_parquet,
                )
                from pyiceberg.io.pyarrow import schema_to_pyarrow

                arrow_schema = schema_to_pyarrow(projected_schema, include_field_ids=False)
                with materialize_batches_to_parquet(batches, arrow_schema) as tmp_path:
                    joined_batches = list(
                        backends.compute.anti_join_from_files(
                            left_paths=[tmp_path],
                            right_paths=[d.file_path for d in eq_deletes],
                            on=eq_cols,
                            io_properties=io_properties,
                        )
                    )
                batches = iter(joined_batches)
            else:
                eq_schema = _build_equality_schema(eq_deletes, table_metadata)
                batches = backends.compute.anti_join(
                    left=batches,
                    right=_read_equality_delete_batches(eq_deletes, eq_schema, io_properties, backends),
                    on=eq_cols,
                )
        elif eq_deletes:
            eq_cols = _get_equality_field_names(eq_deletes, table_metadata)
            if eq_cols is None:
                import warnings

                warnings.warn(
                    "Equality delete files do not specify equality_ids. "
                    "Cannot apply equality deletes -- returning superset of correct results. "
                    "This may include rows that should have been deleted.",
                    UserWarning,
                    stacklevel=2,
                )
                # Use task.residual for pushdown (handles schema evolution column names).
                # If row_filter is AlwaysTrue, caller wants all rows - use AlwaysTrue for pushdown too.
                pushdown_filter = AlwaysTrue() if isinstance(row_filter, AlwaysTrue) else task.residual
                batches = backends.read.read_parquet(
                    task.file.file_path,
                    projected_schema,
                    pushdown_filter,
                    io_properties,
                    dictionary_columns=dictionary_columns,
                )
            elif not eq_cols:
                # equality_ids present but all referenced columns dropped via schema
                # evolution. _get_equality_field_names already emitted a warning.
                # Skip anti-join; fall through to plain read (superset of correct results).
                pushdown_filter = AlwaysTrue() if isinstance(row_filter, AlwaysTrue) else task.residual
                batches = backends.read.read_parquet(
                    task.file.file_path,
                    projected_schema,
                    pushdown_filter,
                    io_properties,
                    dictionary_columns=dictionary_columns,
                )
            else:
                batches = backends.compute.anti_join_from_files(
                    left_paths=[task.file.file_path],
                    right_paths=[d.file_path for d in eq_deletes],
                    on=eq_cols,
                    io_properties=io_properties,
                )
        elif pos_deletes:
            batches = _apply_positional_deletes(
                backends,
                task,
                pos_deletes,
                projected_schema,
                io_properties,
            )
        else:
            # Use task.residual for pushdown (handles schema evolution column names).
            # If row_filter is AlwaysTrue, caller wants all rows - use AlwaysTrue for pushdown too.
            pushdown_filter = AlwaysTrue() if isinstance(row_filter, AlwaysTrue) else task.residual
            batches = backends.read.read_parquet(
                task.file.file_path,
                projected_schema,
                pushdown_filter,
                io_properties,
                dictionary_columns=dictionary_columns,
            )

        # Schema reconciliation must happen BEFORE post-filter because:
        # 1. The filter may reference columns that don't exist in the file but are projected
        #    from partition values (e.g., partition_id column projected from manifest metadata)
        # 2. Column names may have changed (e.g., "idx" renamed to "id" via schema evolution)
        # The reconciled batches have the correct schema with all projected columns.
        result_batches: list[pa.RecordBatch] = []
        reconcile_fn: Callable[[pa.RecordBatch], pa.RecordBatch] | object | None = None

        for batch in batches:
            if reconcile_fn is None:
                reconcile_fn = _build_reconcile_fn(
                    batch,
                    projected_schema,
                    table_metadata,
                    downcast_ns_timestamp_to_us,
                    task=task,
                    schema_cache=schema_cache,
                    schema_cache_lock=schema_cache_lock,
                )

            if reconcile_fn is _NO_RECONCILIATION:
                result_batches.append(batch)
            elif callable(reconcile_fn):
                result_batches.append(reconcile_fn(batch))
            else:
                result_batches.append(batch)

        # Post-filter guarantees correctness; read_parquet pushdown is best-effort only.
        # Use the row_filter parameter (not task.residual) for post-filtering.
        # This allows callers like Transaction.delete's _read_live_rows to override
        # the task's residual (e.g., pass AlwaysTrue() to read all rows).
        #
        # The residual from ManifestGroupPlanner may contain unbound predicates
        # (e.g., for unpartitioned tables or predicates not involving partition columns).
        # We must bind to the projected schema before converting to PyArrow expression.
        # However, some residuals may already be bound (from tests or REST scan planning),
        # so we catch the TypeError from bind() and use the residual as-is.
        #
        # Post-filter happens AFTER reconciliation so that:
        # - Projected partition columns are available for filtering
        # - Renamed columns use their current names
        if not isinstance(row_filter, AlwaysTrue):
            from pyiceberg.expressions.visitors import bind

            try:
                bound_filter = bind(projected_schema, row_filter, case_sensitive)
            except TypeError:
                # Predicate is already bound
                bound_filter = row_filter
            result_batches = list(backends.compute.filter(iter(result_batches), bound_filter))

        return result_batches

    executor = ExecutorFactory.get_or_create()
    max_inflight = _get_max_inflight_tasks(executor)

    if streaming:
        for task_batches in _bounded_map(executor, _execute_task, tasks, max_inflight):
            yield from _spill_and_stream(task_batches)
    else:
        for task_batches in _bounded_map(executor, _execute_task, tasks, max_inflight):
            yield from task_batches


def _spill_and_stream(batches: list[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    """Write task result to temp Parquet and stream back at O(batch_size) memory.

    Uses streaming ParquetWriter (batch-at-a-time) to avoid 2× peak memory
    that would result from pa.Table.from_batches() intermediate.

    Temp file cleanup:
    - Primary: finally block (runs when generator is exhausted or GC'd).
    - Secondary: atexit handler in materialize.py (process-exit safety net for
      abandoned generators whose GC is delayed by reference cycles).
    """
    import tempfile
    from pathlib import Path

    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    from pyiceberg.execution.materialize import _active_temp_files, _temp_files_lock

    if not batches:
        return

    if len(batches) < _get_spill_batch_threshold():
        yield from batches
        return

    # Use NamedTemporaryFile for cross-platform temp path, then close immediately.
    # Pattern is intentional: we need the path for parquet writer, and manually
    # control cleanup via _active_temp_files tracker.
    tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", prefix="pyiceberg_stream_", delete=False)  # noqa: SIM115
    tmp_path = tmp_file.name
    tmp_file.close()

    with _temp_files_lock:
        _active_temp_files.add(tmp_path)

    try:
        schema = batches[0].schema
        writer = pq.ParquetWriter(tmp_path, schema=schema)
        for batch in batches:
            if batch.num_rows > 0:
                writer.write_batch(batch)
        writer.close()
        del batches

        dataset = ds.dataset(tmp_path, format="parquet")
        for batch in dataset.scanner().to_batches():
            yield batch
    finally:
        with _temp_files_lock:
            _active_temp_files.discard(tmp_path)
        Path(tmp_path).unlink(missing_ok=True)


def _apply_positional_deletes(
    backends: Backends,
    task: FileScanTask,
    pos_deletes: list[DataFile],
    projected_schema: Schema,
    io_properties: Mapping[str, Any],
) -> Iterator[pa.RecordBatch]:
    """Route positional deletes to the optimal implementation.

    Uses the PyArrow set-based approach (O(num_positions) memory, zero temp I/O)
    when the total compressed size of delete files is small. Falls back to the
    compute backend's apply_positional_deletes (DataFusion bounded-memory path)
    only when delete files are large enough that the position set would be risky.
    """
    from pyiceberg.execution.backends.pyarrow_backend import (
        _apply_positional_deletes_impl,
    )

    total_delete_bytes = sum(d.file_size_in_bytes for d in pos_deletes)

    if total_delete_bytes < _get_pos_delete_threshold() or not backends.supports_bounded_memory:
        return _apply_positional_deletes_impl(
            task.file.file_path,
            [d.file_path for d in pos_deletes],
            projected_schema,
            io_properties,
        )
    else:
        return backends.compute.apply_positional_deletes(
            data_path=task.file.file_path,
            position_delete_paths=[d.file_path for d in pos_deletes],
            projected_schema=projected_schema,
            io_properties=io_properties,
        )


def _read_equality_delete_batches(
    delete_files: list[DataFile],
    equality_schema: Schema,
    io_properties: Mapping[str, Any],
    backends: Backends,
) -> Iterator[pa.RecordBatch]:
    """Read and chain batches from multiple equality delete files."""
    for df in delete_files:
        yield from backends.read.read_parquet(df.file_path, equality_schema, AlwaysTrue(), io_properties)


def _is_widening_promotion(file_type: Any, projected_type: Any) -> bool:
    """Check if file_type can be promoted to projected_type (widening conversion).

    Widening conversions are safe and supported by Iceberg's schema evolution:
    - int → long
    - float → double
    - decimal(P1, S) → decimal(P2, S) where P2 > P1

    This function also handles nested types (lists, maps) by recursively checking
    their element/value types for widening promotions.

    Returns True if file_type is narrower than projected_type (needs promotion).
    Returns False if types are equal, or if conversion would be narrowing/unsupported.
    """
    from pyiceberg.types import DecimalType, DoubleType, FloatType, IntegerType, ListType, LongType, MapType

    # int → long
    if isinstance(file_type, IntegerType) and isinstance(projected_type, LongType):
        return True
    # float → double
    if isinstance(file_type, FloatType) and isinstance(projected_type, DoubleType):
        return True
    # decimal precision widening (same scale)
    if isinstance(file_type, DecimalType) and isinstance(projected_type, DecimalType):
        if file_type.scale == projected_type.scale and file_type.precision < projected_type.precision:
            return True

    # list<T1> → list<T2> where T1 can be promoted to T2
    if isinstance(file_type, ListType) and isinstance(projected_type, ListType):
        return _is_widening_promotion(file_type.element_type, projected_type.element_type)

    # map<K, V1> → map<K, V2> where V1 can be promoted to V2
    # Note: key types cannot be promoted in Iceberg schema evolution
    if isinstance(file_type, MapType) and isinstance(projected_type, MapType):
        return _is_widening_promotion(file_type.value_type, projected_type.value_type)

    return False


def _infer_file_schema_from_batch(batch: pa.RecordBatch, table_metadata: TableMetadata, downcast_ns: bool) -> Schema | None:
    """Infer the file's Iceberg schema from a batch's Arrow schema."""
    from pyiceberg.io.pyarrow import pyarrow_to_schema

    try:
        # Use table_metadata.name_mapping() which returns the stored name mapping
        # from table properties (schema.name-mapping.default). This mapping
        # includes aliases for old column names after renames, enabling schema
        # reconciliation for files written before schema evolution.
        # Fallback to schema-derived mapping if no stored mapping exists.
        name_mapping = table_metadata.name_mapping() or table_metadata.schema().name_mapping
        return pyarrow_to_schema(
            batch.schema,
            name_mapping=name_mapping,
            downcast_ns_timestamp_to_us=downcast_ns,
            format_version=table_metadata.format_version,
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def _build_reconcile_fn(
    batch: pa.RecordBatch,
    projected_schema: Schema,
    table_metadata: TableMetadata,
    downcast_ns: bool,
    *,
    task: FileScanTask | None = None,
    schema_cache: dict[pa.Schema, Schema | None] | None = None,
    schema_cache_lock: threading.Lock | None = None,
) -> Callable[[pa.RecordBatch], pa.RecordBatch] | object:
    """Determine whether schema reconciliation is needed and return the appropriate function.

    Reconciliation is needed when:
    1. Field IDs differ (columns missing/added via schema evolution)
    2. Nullability differs (file has required=True but table schema has optional=True)

    We deliberately skip reconciliation when only types differ (e.g., int64 vs int32)
    because the inferred file schema from Arrow types may not match Iceberg types exactly,
    and attempting type promotion in such cases can fail (e.g., long → int is invalid).
    """
    from pyiceberg.io.pyarrow import _get_column_projection_values, _to_requested_schema

    file_schema: Schema | None
    if schema_cache is not None:
        cache_key = batch.schema.remove_metadata()
        if schema_cache_lock is not None:
            with schema_cache_lock:
                if cache_key in schema_cache:
                    file_schema = schema_cache[cache_key]
                else:
                    file_schema = _infer_file_schema_from_batch(batch, table_metadata, downcast_ns)
                    schema_cache[cache_key] = file_schema
        else:
            file_schema = schema_cache.get(cache_key)
            if file_schema is None and cache_key not in schema_cache:
                file_schema = _infer_file_schema_from_batch(batch, table_metadata, downcast_ns)
                schema_cache[cache_key] = file_schema
    else:
        file_schema = _infer_file_schema_from_batch(batch, table_metadata, downcast_ns)

    if file_schema is not None:
        # Check if reconciliation is actually needed:
        # 1. Field IDs differ (schema evolution - columns added/removed)
        # 2. Column names differ (schema evolution - column renamed)
        # 3. File field is required but projected field is optional (need to widen nullability)
        # 4. File type needs widening promotion (e.g., int32 → int64)
        #
        # We do NOT reconcile for narrowing type conversions (e.g., int64 → int32) because:
        # - These would fail in _to_requested_schema's promote() call
        # - This typically indicates the inferred file schema doesn't match actual Iceberg types
        #   (e.g., PyArrow infers int64 from Python ints but Iceberg schema says int32)
        needs_reconciliation = file_schema.field_ids != projected_schema.field_ids

        if not needs_reconciliation:
            for proj_field in projected_schema.fields:
                file_field = file_schema.find_field(proj_field.field_id)
                if file_field is not None:
                    # Check column name: file name differs from projected name → column was renamed
                    if file_field.name != proj_field.name:
                        needs_reconciliation = True
                        break
                    # Check nullability: file required but projected optional → need to widen
                    if file_field.required and not proj_field.required:
                        needs_reconciliation = True
                        break
                    # Check type promotion: only reconcile for widening conversions
                    if file_field.field_type != proj_field.field_type:
                        if _is_widening_promotion(file_field.field_type, proj_field.field_type):
                            needs_reconciliation = True
                            break

        if needs_reconciliation:
            # Use the file's spec_id to get the correct partition spec for this file.
            # Files may have been written with different specs before partition evolution,
            # so we can't use default_spec_id which is the current spec.
            partition_spec = table_metadata.specs().get(task.file.spec_id) if task is not None else None
            projected_missing_fields = (
                _get_column_projection_values(
                    task.file, projected_schema, table_metadata.schema(), partition_spec, file_schema.field_ids
                )
                if task is not None and partition_spec is not None
                else {}
            )

            # Capture per-file constants for the closure.
            _file_schema = file_schema
            _downcast = downcast_ns
            _missing_fields = projected_missing_fields

            def _reconcile(b: pa.RecordBatch) -> pa.RecordBatch:
                return _to_requested_schema(
                    projected_schema,
                    _file_schema,
                    b,
                    downcast_ns_timestamp_to_us=_downcast,
                    projected_missing_fields=_missing_fields,
                    allow_timestamp_tz_mismatch=True,
                )

            return _reconcile

        # Schemas are compatible (same field IDs, compatible nullability/types) - no reconciliation needed
        return _NO_RECONCILIATION

    # file_schema is None -- schema inference failed
    logger.debug(
        "Schema inference failed for batch (Arrow schema fingerprint: %s). "
        "Skipping schema reconciliation -- batches will pass through unchanged. "
        "If columns are missing or have wrong types, check that the table has "
        "a name mapping or that Parquet files include field IDs in metadata.",
        batch.schema.fingerprint if hasattr(batch.schema, "fingerprint") else str(batch.schema),
    )

    return _NO_RECONCILIATION


def _get_equality_field_names(delete_files: list[DataFile], table_metadata: TableMetadata) -> list[str] | None:
    """Extract equality field column names from delete files.

    Returns:
        list[str]: Resolved column names (may be partial if some IDs are unresolvable).
        None: Delete files have no equality_ids metadata recorded at all.
            This distinguishes "metadata absent" (cannot apply) from "IDs present
            but all columns dropped" (schema evolution edge case).
    """
    schema = table_metadata.schema()
    field_ids: set[int] = set()
    for df in delete_files:
        if df.equality_ids:
            field_ids.update(df.equality_ids)

    if not field_ids:
        # No equality_ids recorded in any delete file metadata.
        return None

    names = []
    for fid in sorted(field_ids):
        name = schema.find_column_name(fid)
        if name is not None:
            names.append(name)

    if field_ids and not names:
        import warnings

        warnings.warn(
            f"Equality delete files reference field IDs {sorted(field_ids)} which do not exist "
            f"in the current table schema. This can occur after schema evolution drops columns "
            f"used by equality deletes. The affected delete files will not be applied -- "
            f"results may include rows that should have been deleted. "
            f"Run a compaction to resolve these orphaned equality delete files.",
            UserWarning,
            stacklevel=2,
        )

    return names


def _build_equality_schema(delete_files: list[DataFile], table_metadata: TableMetadata) -> Schema:
    """Build a Schema containing only the equality field columns from delete files."""
    table_schema = table_metadata.schema()
    field_ids: set[int] = set()
    for df in delete_files:
        if df.equality_ids:
            field_ids.update(df.equality_ids)

    if not field_ids:
        raise ValueError("Equality delete files do not specify equality_ids. Cannot build equality schema.")

    fields = []
    for fid in sorted(field_ids):
        field = table_schema.find_field(fid)
        if field is not None:
            fields.append(field)

    if not fields:
        raise ValueError(f"Could not resolve any equality field IDs {field_ids} to schema fields.")

    return Schema(*fields)


def _cow_filter_batches(
    batches: Iterator[pa.RecordBatch],
    predicate: Any,
) -> Iterator[pa.RecordBatch]:
    """Filter RecordBatches by a PyArrow expression (streaming, O(batch_size) memory).

    Used by the CoW delete path to apply the complement filter per-batch without
    materializing the full file. Accepts a PyArrow compute expression (as produced
    by _expression_to_complementary_pyarrow).

    Args:
        batches: Input RecordBatches to filter (streaming).
        predicate: A PyArrow compute expression (pc.Expression).

    Yields:
        RecordBatches with only rows that satisfy the predicate.
    """
    for batch in batches:
        filtered = batch.filter(predicate)
        if filtered.num_rows > 0:
            yield filtered


def _get_sort_order(table_metadata: TableMetadata) -> SortKeyList | None:
    """Extract the default sort order as (column_name, direction) pairs."""
    if table_metadata.default_sort_order_id == UNSORTED_SORT_ORDER_ID:
        return None

    schema = table_metadata.schema()
    sort_order = next(
        (so for so in table_metadata.sort_orders if so.order_id == table_metadata.default_sort_order_id),
        None,
    )
    if sort_order is None or not sort_order.fields:
        return None

    result: SortKeyList = []
    for field in sort_order.fields:
        col_name = schema.find_column_name(field.source_id)
        if col_name is None:
            return None  # Cannot resolve sort field
        direction: Literal["ascending", "descending"] = "ascending" if field.direction.name == "ASC" else "descending"
        result.append((col_name, direction))
    return result
