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

"""PyArrow execution backend: default fallback (in-memory, no spill-to-disk).

Split into three independent classes:
- PyArrowReadBackend: reads Parquet via pyarrow.dataset
- PyArrowWriteBackend: writes Parquet via pyarrow.parquet
- PyArrowComputeBackend: sort/join/filter/aggregate via pyarrow.compute

All are always available (PyArrow is a required dependency). None support
bounded-memory execution: operations on large data will OOM.
"""

from __future__ import annotations

__all__ = ["PyArrowComputeBackend", "PyArrowReadBackend", "PyArrowWriteBackend"]

import logging
import os
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NewType

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from pyiceberg.execution.protocol import SortKeyList
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Properties

logger = logging.getLogger(__name__)

# =============================================================================
# Internal Write Types (not part of the pluggable WriteBackend protocol)
# =============================================================================
# These types support the standalone write helpers (write_parquet, write_data_files,
# write_to_stream) which bypass the FileFormatModel pipeline. The protocol method
# write_data_file() composes with FileFormatModel and returns DataFileStatistics
# (from pyiceberg.io.fileformat). These types exist for direct Parquet I/O in
# materialize.py and tests.

#: Parquet column index (0-based positional in the flattened schema).
#: Distinct from Iceberg field IDs — the caller maps indices to field IDs
#: via parquet_path_to_id_mapping when constructing DataFile manifest entries.
_ColumnIndex = NewType("_ColumnIndex", int)


@dataclass(frozen=True)
class _ParquetWriteConfig:
    """Typed configuration for physical Parquet encoding.

    Each field maps to an Iceberg table property:
        compression        ← write.parquet.compression-codec
        compression_level  ← write.parquet.compression-level
        row_group_size     ← write.parquet.row-group-limit
        data_page_size     ← write.parquet.page-size-bytes
        dictionary_pagesize_limit ← write.parquet.dict-size-bytes
        write_batch_size   ← write.parquet.page-row-limit
    """

    compression: str = "zstd"
    compression_level: int | None = None
    row_group_size: int = 1_048_576
    data_page_size: int = 1_048_576
    dictionary_pagesize_limit: int = 2_097_152
    write_batch_size: int = 20_000


@dataclass(frozen=True)
class _WriteResult:
    """Metadata returned after writing a Parquet file via standalone helpers.

    Dict keys are **column indices** (0-based positional in the flattened Parquet
    schema), NOT Iceberg field IDs. The caller maps indices to field IDs via
    parquet_path_to_id_mapping when constructing DataFile objects.
    """

    file_path: str
    file_size_in_bytes: int
    record_count: int
    column_sizes: dict[_ColumnIndex, int]
    value_counts: dict[_ColumnIndex, int]
    null_value_counts: dict[_ColumnIndex, int]
    lower_bounds: dict[_ColumnIndex, bytes]
    upper_bounds: dict[_ColumnIndex, bytes]
    split_offsets: list[int]


#: Default number of batches to prefetch per file during scanning. Lower than
#: PyArrow's default (16) to limit per-file memory buffering — since
#: orchestrate_scan runs multiple files in parallel via the thread pool, total
#: prefetch memory is batch_readahead × batch_size × num_concurrent_tasks.
#: Value of 2 keeps ~2 MB prefetched per file at PyArrow's default 128K-row
#: batch size.
#: Configurable via execution.scanner-batch-readahead in .pyiceberg.yaml
#: or PYICEBERG_EXECUTION__SCANNER_BATCH_READAHEAD env var.
_SCANNER_BATCH_READAHEAD_DEFAULT: int = 2


def _get_scanner_batch_readahead() -> int:
    """Read the scanner batch readahead from config or default (2).

    Controls PyArrow scanner prefetch depth. Higher values improve throughput on
    fast storage (NVMe, fast object stores) at the cost of per-file memory.
    Lower values reduce memory in concurrent-scan or memory-constrained environments.
    """
    from pyiceberg.execution.engine import get_execution_config_int

    return get_execution_config_int("scanner-batch-readahead", _SCANNER_BATCH_READAHEAD_DEFAULT)


# =============================================================================
# Helpers: Filesystem Resolution from io_properties
# =============================================================================


def _resolve_filesystem(location: str, io_properties: Mapping[str, Any]) -> tuple[Any, str]:
    """Resolve a PyArrow FileSystem and path from a location URI and io_properties.

    Reuses PyArrowFileIO's filesystem construction to ensure credential handling
    is consistent with the rest of PyIceberg (catalog-vended credentials, custom
    endpoints, STS tokens, etc.).

    For local paths (no scheme or file://), returns the local filesystem directly
    without constructing a PyArrowFileIO instance (avoids unnecessary overhead).

    Args:
        location: URI or path (e.g., "s3://bucket/key.parquet", "/tmp/file.parquet").
        io_properties: Storage credentials and configuration from the catalog.

    Returns:
        A (filesystem, path) tuple suitable for pyarrow.dataset.dataset(path, filesystem=fs).
    """
    from pyarrow.fs import LocalFileSystem

    from pyiceberg.io.pyarrow import PyArrowFileIO

    # Convert Mapping to dict for PyArrowFileIO which expects dict
    props_dict = dict(io_properties) if io_properties else {}

    scheme, netloc, path = PyArrowFileIO.parse_location(location, props_dict)

    # Local filesystem: "file" scheme, or single-char scheme on Windows (drive letter, e.g., "c").
    if scheme == "file" or (len(scheme) == 1 and scheme.isalpha()):
        return LocalFileSystem(), os.path.abspath(location)

    # Cloud or remote filesystem — use PyArrowFileIO's credential resolution.
    file_io = PyArrowFileIO(properties=props_dict)
    fs = file_io.fs_by_scheme(scheme, netloc)
    return fs, path


# =============================================================================
# Helpers: Parquet Statistics Extraction
# =============================================================================


def _extract_parquet_statistics(
    metadata_collector: list[pq.FileMetaData],
) -> tuple[
    dict[_ColumnIndex, int],
    dict[_ColumnIndex, int],
    dict[_ColumnIndex, int],
    dict[_ColumnIndex, bytes],
    dict[_ColumnIndex, bytes],
    list[int],
]:
    """Extract column statistics from Parquet FileMetaData collected during writes."""
    column_sizes: dict[_ColumnIndex, int] = {}
    value_counts: dict[_ColumnIndex, int] = {}
    null_value_counts: dict[_ColumnIndex, int] = {}
    lower_bounds: dict[_ColumnIndex, bytes] = {}
    upper_bounds: dict[_ColumnIndex, bytes] = {}
    split_offsets: list[int] = []

    for file_metadata in metadata_collector:
        for row_group_idx in range(file_metadata.num_row_groups):
            rg = file_metadata.row_group(row_group_idx)
            if row_group_idx > 0:
                split_offsets.append(rg.column(0).data_page_offset)
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                idx = _ColumnIndex(col_idx)
                column_sizes[idx] = column_sizes.get(idx, 0) + col.total_compressed_size
                value_counts[idx] = value_counts.get(idx, 0) + col.num_values
                # pyarrow-stubs doesn't have has_null_count yet; it exists in pyarrow
                if col.statistics and col.statistics.has_null_count:
                    null_value_counts[idx] = null_value_counts.get(idx, 0) + col.statistics.null_count
                if col.statistics and col.statistics.has_min_max:
                    try:
                        min_val = col.statistics.min_raw
                        max_val = col.statistics.max_raw
                        if min_val is not None and (idx not in lower_bounds or min_val < lower_bounds[idx]):
                            lower_bounds[idx] = min_val
                        if max_val is not None and (idx not in upper_bounds or max_val > upper_bounds[idx]):
                            upper_bounds[idx] = max_val
                    except (TypeError, AttributeError):
                        pass

    return column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets


# =============================================================================
# READ
# =============================================================================


class PyArrowReadBackend:
    """Reads Parquet files via pyarrow.dataset.Scanner with predicate pushdown."""

    def read_parquet(
        self,
        location: str,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: Mapping[str, Any],
        dictionary_columns: tuple[str, ...] = (),
    ) -> Iterator[pa.RecordBatch]:
        """Read Parquet with projection and optional filter pushdown."""
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.io.pyarrow import expression_to_pyarrow, schema_to_pyarrow

        pa_schema = schema_to_pyarrow(projected_schema, include_field_ids=False)
        columns = [field.name for field in pa_schema]

        pa_filter = None
        if not isinstance(row_filter, AlwaysTrue):
            try:
                pa_filter = expression_to_pyarrow(row_filter)
            except (TypeError, ValueError, KeyError, NotImplementedError):
                pa_filter = None

        filesystem, path = _resolve_filesystem(location, io_properties)
        dataset = ds.dataset(path, format="parquet", filesystem=filesystem)

        # Only request columns that exist in the file. Missing columns (from schema
        # evolution) will be filled with NULLs by the schema reconciliation layer in
        # _orchestrate.py. Without this intersection, PyArrow raises
        # "No match for FieldRef.Name(col)" for columns added after the file was written.
        file_columns = set(dataset.schema.names)
        available_columns = [c for c in columns if c in file_columns]

        scanner = dataset.scanner(
            columns=available_columns if available_columns else None,
            filter=pa_filter,
            use_threads=True,
            batch_readahead=_get_scanner_batch_readahead(),
        )
        return scanner.to_batches()


# =============================================================================
# WRITE
# =============================================================================


class PyArrowWriteBackend:
    """Writes data files by delegating to the FileFormatModel's writer.

    Composes with upstream's FileFormatWriter abstraction (#3381):
    - write_data_file() → format_model.create_writer() → writer.write() → statistics

    The PyArrow backend delegates directly. A future DataFusion backend could
    intercept and use DataFusion's ParquetSink for single-pass bounded-memory writes.

    Legacy methods (write_to_stream, write_parquet, write_data_files) are retained
    for standalone use (materialize.py, tests) but are NOT part of the WriteBackend
    protocol contract.
    """

    def write_data_file(
        self,
        output_file: Any,
        file_schema: Any,
        properties: Any,
        arrow_table: pa.Table,
        format_model: Any,
    ) -> Any:
        """Write an Arrow table to a data file via the format model's writer.

        Delegates to format_model.create_writer() which returns a FileFormatWriter.
        The writer handles physical encoding (Parquet/ORC) and returns statistics.

        Args:
            output_file: OutputFile from Iceberg's FileIO.
            file_schema: Iceberg Schema for field ID mapping and statistics.
            properties: Table properties (compression, row group size, etc.).
            arrow_table: Data to write (schema already reconciled by caller).
            format_model: FileFormatModel instance (e.g., ParquetFormatModel).

        Returns:
            DataFileStatistics from the format writer.
        """
        writer = format_model.create_writer(output_file, file_schema, properties)
        with writer:
            if arrow_table.num_rows > 0:
                writer.write(arrow_table)
        return writer.result()

    def write_to_stream(
        self,
        batches: Iterator[pa.RecordBatch],
        output_stream: Any,
        schema: pa.Schema,
        config: _ParquetWriteConfig,
    ) -> pq.FileMetaData:
        """Write RecordBatches to an open output stream, return raw FileMetaData."""
        with pq.ParquetWriter(
            output_stream,
            schema=schema,
            store_decimal_as_integer=True,
            compression=config.compression,
            compression_level=config.compression_level,
            data_page_size=config.data_page_size,
            dictionary_pagesize_limit=config.dictionary_pagesize_limit,
            write_batch_size=config.write_batch_size,
        ) as writer:
            for batch in batches:
                if batch.num_rows > 0:
                    writer.write_batch(batch, row_group_size=config.row_group_size)

        return writer.writer.metadata

    def write_parquet(
        self,
        batches: Iterator[pa.RecordBatch],
        location: str,
        schema: Schema,
        write_properties: Properties,
        io_properties: Properties,
    ) -> _WriteResult:
        """Write RecordBatches to a single Parquet file with full column statistics."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        pa_schema = schema_to_pyarrow(schema, include_field_ids=True)

        metadata_collector: list[pq.FileMetaData] = []
        writer = pq.ParquetWriter(location, schema=pa_schema, metadata_collector=metadata_collector)

        record_count = 0
        for batch in batches:
            if batch.num_rows > 0:
                writer.write_batch(batch)
                record_count += batch.num_rows
        writer.close()

        file_size = os.path.getsize(location) if os.path.exists(location) else 0

        column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets = _extract_parquet_statistics(
            metadata_collector
        )

        return _WriteResult(
            file_path=location,
            file_size_in_bytes=file_size,
            record_count=record_count,
            column_sizes=column_sizes,
            value_counts=value_counts,
            null_value_counts=null_value_counts,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            split_offsets=split_offsets,
        )

    def write_data_files(
        self,
        batches: Iterator[pa.RecordBatch],
        base_location: str,
        schema: Schema,
        target_file_size: int,
        write_properties: Properties,
        io_properties: Properties,
    ) -> list[_WriteResult]:
        """Write RecordBatches to multiple Parquet files, splitting at target size."""
        results: list[_WriteResult] = []
        current_writer: pq.ParquetWriter | None = None
        current_path: str | None = None
        current_rows = 0
        current_size = 0
        current_metadata_collector: list[pq.FileMetaData] = []
        pa_schema: pa.Schema | None = None

        def _close_current() -> None:
            nonlocal current_writer, current_path, current_rows, current_size, current_metadata_collector
            if current_writer is not None and current_path is not None:
                current_writer.close()
                file_size = os.path.getsize(current_path) if os.path.exists(current_path) else 0

                column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets = (
                    _extract_parquet_statistics(current_metadata_collector)
                )

                results.append(
                    _WriteResult(
                        file_path=current_path,
                        file_size_in_bytes=file_size,
                        record_count=current_rows,
                        column_sizes=column_sizes,
                        value_counts=value_counts,
                        null_value_counts=null_value_counts,
                        lower_bounds=lower_bounds,
                        upper_bounds=upper_bounds,
                        split_offsets=split_offsets,
                    )
                )
                current_writer = None
                current_path = None
                current_rows = 0
                current_size = 0
                current_metadata_collector = []

        def _open_new() -> None:
            nonlocal current_writer, current_path, current_rows, current_size, current_metadata_collector, pa_schema
            assert pa_schema is not None  # Set from first batch before this is called
            file_name = f"{uuid.uuid4()}.parquet"
            if "://" in base_location:
                current_path = f"{base_location.rstrip('/')}/{file_name}"
            else:
                current_path = os.path.join(base_location, file_name)
            current_metadata_collector = []
            current_writer = pq.ParquetWriter(current_path, schema=pa_schema, metadata_collector=current_metadata_collector)
            current_rows = 0
            current_size = 0

        for batch in batches:
            if batch.num_rows == 0:
                continue
            if pa_schema is None:
                pa_schema = batch.schema
            if current_writer is None:
                _open_new()
            assert current_writer is not None  # Set by _open_new()
            current_writer.write_batch(batch)
            current_rows += batch.num_rows
            current_size += batch.nbytes
            if current_size >= target_file_size:
                _close_current()

        _close_current()
        return results


# =============================================================================
# COMPUTE
# =============================================================================


class PyArrowComputeBackend:
    """PyArrow compute: in-memory sort/join/filter/aggregate. No spill-to-disk."""

    @property
    def supports_bounded_memory(self) -> bool:
        """Return False because this backend cannot spill to disk."""
        return False

    def sort(
        self,
        data: Iterator[pa.RecordBatch],
        sort_keys: SortKeyList,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Sort by materializing into a pa.Table and sorting in-memory."""
        batches = list(data)
        if not batches:
            return iter(())
        table = pa.Table.from_batches(batches)
        sort_indices = pc.sort_indices(table, sort_keys=[(col, direction) for col, direction in sort_keys])
        sorted_table = table.take(sort_indices)
        return iter(sorted_table.to_batches())

    def sort_from_files(
        self,
        file_paths: list[str],
        sort_keys: SortKeyList,
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Sort data from Parquet files (materializes in memory)."""
        if not file_paths:
            return iter(())
        resolved = [_resolve_filesystem(p, io_properties) for p in file_paths]
        # All files should share the same filesystem; use the first.
        fs = resolved[0][0]
        paths = [r[1] for r in resolved]
        dataset = ds.dataset(paths, format="parquet", filesystem=fs)
        table = dataset.to_table()
        sort_indices = pc.sort_indices(table, sort_keys=[(col, direction) for col, direction in sort_keys])
        sorted_table = table.take(sort_indices)
        return iter(sorted_table.to_batches())

    def anti_join(
        self,
        left: Iterator[pa.RecordBatch],
        right: Iterator[pa.RecordBatch],
        on: list[str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Anti-join using struct-based is_in for correct NULL semantics."""
        left_batches = list(left)
        right_batches = list(right)
        if not left_batches:
            return iter(())
        left_table = pa.Table.from_batches(left_batches)
        if left_table.num_rows == 0:
            return iter(())
        if not right_batches:
            return iter(left_table.to_batches())
        right_table = pa.Table.from_batches(right_batches)
        if right_table.num_rows == 0:
            return iter(left_table.to_batches())
        result = _anti_join_tables(left_table, right_table, on, null_equals_null=True)
        return iter(result.to_batches())

    def anti_join_from_files(
        self,
        left_paths: list[str],
        right_paths: list[str],
        on: list[str],
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """LEFT ANTI JOIN from Parquet files (materializes in memory)."""
        from pyarrow.fs import LocalFileSystem

        left_resolved = [_resolve_filesystem(p, io_properties) for p in left_paths]
        right_resolved = [_resolve_filesystem(p, io_properties) for p in right_paths]
        left_fs = left_resolved[0][0] if left_resolved else LocalFileSystem()
        right_fs = right_resolved[0][0] if right_resolved else LocalFileSystem()
        left_table = ds.dataset([r[1] for r in left_resolved], format="parquet", filesystem=left_fs).to_table()
        right_table = ds.dataset([r[1] for r in right_resolved], format="parquet", filesystem=right_fs).to_table()
        if left_table.num_rows == 0:
            return iter(())
        if right_table.num_rows == 0:
            return iter(left_table.to_batches())
        result = _anti_join_tables(left_table, right_table, on, null_equals_null=True)
        return iter(result.to_batches())

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]:
        """Filter per-batch using PyArrow compute expressions (streaming, O(1) memory)."""
        return _filter_batches(data, predicate)

    def apply_positional_deletes(
        self,
        data_path: str,
        position_delete_paths: list[str],
        projected_schema: Schema,
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Read a data file and exclude rows at positions listed in delete files."""
        return _apply_positional_deletes_impl(data_path, position_delete_paths, projected_schema, io_properties)


# =============================================================================
# Shared: Positional Deletes
# =============================================================================


def _apply_positional_deletes_impl(
    data_path: str,
    position_delete_paths: list[str],
    projected_schema: Schema | None = None,
    io_properties: Mapping[str, Any] | None = None,
) -> Iterator[pa.RecordBatch]:
    """Apply positional deletes by filtering out rows at specified positions.

    Args:
        data_path: Path to the data file.
        position_delete_paths: Paths to position delete files.
        projected_schema: Output schema (column projection).
        io_properties: Storage credentials for cloud paths. When None, uses
            PyArrow's default filesystem resolution (environment-based).
    """
    _props: dict[str, Any] = dict(io_properties) if io_properties is not None else {}

    # Read positions to delete, filtering to entries for THIS data file.
    positions_to_delete: set[int] = set()
    for del_path in position_delete_paths:
        del_fs, del_resolved = _resolve_filesystem(del_path, _props)
        del_dataset = ds.dataset(del_resolved, format="parquet", filesystem=del_fs)
        file_path_filter = ds.field("file_path") == data_path
        scanner = del_dataset.scanner(columns=["pos"], filter=file_path_filter)
        for batch in scanner.to_batches():
            if batch.num_rows > 0:
                # Iceberg position delete files always have int64 pos column; None values
                # would be spec-violation but we filter them defensively.
                pos_values = (p for p in batch.column("pos").to_pylist() if p is not None)
                positions_to_delete.update(pos_values)

    # Determine column projection.
    columns: list[str] | None = None
    if projected_schema is not None:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        pa_schema = schema_to_pyarrow(projected_schema, include_field_ids=False)
        columns = [field.name for field in pa_schema]

    data_fs, data_resolved = _resolve_filesystem(data_path, _props)

    if not positions_to_delete:
        dataset = ds.dataset(data_resolved, format="parquet", filesystem=data_fs)
        yield from dataset.scanner(columns=columns).to_batches()
        return

    # Vectorized positional delete using is_in.
    dataset = ds.dataset(data_resolved, format="parquet", filesystem=data_fs)
    scanner = dataset.scanner(columns=columns)
    positions_array = pa.array(sorted(positions_to_delete), type=pa.int64())

    row_offset = 0
    for batch in scanner.to_batches():
        batch_size = batch.num_rows

        batch_indices = pa.array(range(row_offset, row_offset + batch_size), type=pa.int64())
        is_deleted = pc.is_in(batch_indices, value_set=positions_array)
        keep_mask = pc.invert(is_deleted)
        row_offset += batch_size

        if is_deleted.true_count == 0:
            yield batch
        elif is_deleted.true_count < batch_size:
            yield batch.filter(keep_mask)


# =============================================================================
# Shared: Streaming Filter
# =============================================================================


def _filter_batches(
    data: Iterator[pa.RecordBatch],
    predicate: BooleanExpression,
) -> Iterator[pa.RecordBatch]:
    """Filter RecordBatches by an Iceberg BooleanExpression (streaming, O(1) memory).

    Shared implementation used by both PyArrowComputeBackend and DataFusionComputeBackend.
    Filter is always streaming (per-batch) regardless of backend — no benefit from
    DataFusion's query engine for a simple predicate evaluation.
    """
    from pyiceberg.io.pyarrow import expression_to_pyarrow

    pa_expr = expression_to_pyarrow(predicate)
    for batch in data:
        filtered = batch.filter(pa_expr)
        if filtered.num_rows > 0:
            yield filtered


# =============================================================================
# Helpers
# =============================================================================


def _anti_join_tables(
    left: pa.Table,
    right: pa.Table,
    on: list[str],
    null_equals_null: bool = False,
) -> pa.Table:
    """LEFT ANTI JOIN with configurable NULL semantics.

    For single-column joins, uses pc.is_in() with O(n + m) complexity.
    For multi-column joins, creates a composite string key and uses pc.is_in()
    on the combined key — also O(n + m). Both approaches handle NULL semantics
    per the Iceberg spec (IS NOT DISTINCT FROM: NULL matches NULL).

    Args:
        left: Left-side table (data rows to keep or exclude).
        right: Right-side table (delete entries to match against).
        on: Column names to join on.
        null_equals_null: If True, NULL == NULL (Iceberg IS NOT DISTINCT FROM).

    Returns:
        Left table with rows that matched any right row removed.
    """
    if len(on) == 1:
        col = on[0]
        left_col = left.column(col)
        right_keys = right.column(col)

        if null_equals_null:
            # IS NOT DISTINCT FROM: NULL matches NULL
            in_right = pc.is_in(left_col, value_set=right_keys)
            right_has_null = right_keys.null_count > 0
            if right_has_null:
                left_is_null = pc.is_null(left_col)
                in_right = pc.or_(in_right, left_is_null)
            mask = pc.invert(in_right)
        else:
            mask = pc.invert(pc.is_in(left_col, value_set=right_keys))
        return left.filter(mask)
    else:
        # Multi-column anti-join using composite string keys for O(n + m) hash lookup.
        # Encodes each row's join columns into a single deterministic string key,
        # then uses pc.is_in() on the composite key (hash-set membership: O(1) per row).
        return _multi_column_anti_join(left, right, on, null_equals_null)


def _multi_column_anti_join(
    left: pa.Table,
    right: pa.Table,
    on: list[str],
    null_equals_null: bool,
) -> pa.Table:
    """Multi-column LEFT ANTI JOIN using composite key hashing — O(n + m).

    Creates a deterministic string representation of each row's join columns,
    then uses pc.is_in() on the composite key. The key encoding uses a separator
    that cannot appear in the encoded values to prevent false collisions.

    NULL handling:
    - null_equals_null=True (Iceberg IS NOT DISTINCT FROM): NULLs are replaced with
      a sentinel string so they match each other. All rows participate in the join.
    - null_equals_null=False (standard SQL): Rows with ANY NULL join column are
      automatically kept (never match), since NULL comparison yields UNKNOWN.
    """
    if not null_equals_null:
        # Standard SQL: rows with any NULL in join columns never match → always kept.
        # Only non-null rows participate in the composite key lookup.
        left_has_null_mask = _any_null_mask(left, on)
        right_has_null_mask = _any_null_mask(right, on)

        # Split left into null-containing (always kept) and non-null (checked)
        left_null_rows = left.filter(left_has_null_mask)
        left_nonnull_rows = left.filter(pc.invert(left_has_null_mask))

        if left_nonnull_rows.num_rows == 0:
            return left  # All rows have nulls → all kept

        # Filter right to non-null rows only (null right rows never match anything)
        right_nonnull = right.filter(pc.invert(right_has_null_mask))

        if right_nonnull.num_rows == 0:
            return left  # No non-null right rows → nothing excluded

        # Composite key join on non-null portions only
        left_keys = _build_composite_key_nonnull(left_nonnull_rows, on)
        right_keys = _build_composite_key_nonnull(right_nonnull, on)
        mask = pc.invert(pc.is_in(left_keys, value_set=right_keys))
        surviving_nonnull = left_nonnull_rows.filter(mask)

        # Recombine: null rows (always kept) + surviving non-null rows
        return pa.concat_tables([left_null_rows, surviving_nonnull])
    else:
        # IS NOT DISTINCT FROM: NULLs match NULLs.
        # Replace NULLs with sentinel, then all rows participate in is_in.
        left_keys = _build_composite_key_null_safe(left, on)
        right_keys = _build_composite_key_null_safe(right, on)
        mask = pc.invert(pc.is_in(left_keys, value_set=right_keys))
        return left.filter(mask)


def _any_null_mask(table: pa.Table, on: list[str]) -> Any:
    """Return a boolean mask that is True for rows with ANY null in the join columns."""
    masks = [pc.is_null(table.column(col)) for col in on]
    result = masks[0]
    for m in masks[1:]:
        result = pc.or_(result, m)
    return result


#: Composite key separator: ASCII Record Separator + Unit Separator (0x1E 0x1F).
#: Cannot appear in valid text data. Runtime assertion validates no collisions.
_KEY_SEPARATOR = "\x1e\x1f"
#: Sentinel for NULL values in composite keys. When null_equals_null=True,
#: NULLs are encoded as this sentinel so they match each other via is_in.
#: Uses NUL bytes as bookends to avoid collision with any real string value.
_NULL_SENTINEL = "\x00\x01NULL\x01\x00"


def _build_composite_key_nonnull(table: pa.Table, on: list[str]) -> Any:
    """Build composite string key for rows guaranteed to have no NULLs in join columns.

    Uses a two-byte control character separator (0x1E 0x1F) that cannot appear in
    valid text data. A runtime check validates no values contain the separator to
    guarantee collision-free encoding.

    Returns a ChunkedArray of strings (type Any due to pyarrow-stubs limitations).
    """
    str_cols: list[Any] = []
    for col_name in on:
        col = table.column(col_name)
        if not pa.types.is_string(col.type) and not pa.types.is_large_string(col.type):
            str_cols.append(pc.cast(col, pa.string()))
        else:
            str_cols.append(col)

    # Validate no values contain the separator (guarantees collision-free keys).
    if len(str_cols) > 1:
        for str_col in str_cols:
            if pc.any(pc.match_substring(str_col, _KEY_SEPARATOR)).as_py():
                raise ValueError(
                    "Equality delete join column contains the composite key separator "
                    "(0x1E 0x1F). Multi-column anti-join requires column values to not "
                    "contain control characters used for key encoding. This is a data "
                    "limitation of the PyArrow composite-key anti-join path. "
                    "Install DataFusion (`pip install 'pyiceberg[datafusion]'`) to use "
                    "SQL-based anti-join which handles arbitrary column values."
                )

    result: Any = str_cols[0]
    for str_col in str_cols[1:]:
        result = pc.binary_join_element_wise(result, str_col, _KEY_SEPARATOR)
    return result


def _build_composite_key_null_safe(table: pa.Table, on: list[str]) -> Any:
    """Build composite string key with NULL sentinel replacement for IS NOT DISTINCT FROM.

    Uses a two-byte control character separator (0x1E 0x1F) that cannot appear in
    valid text data. A runtime check validates no values contain the separator to
    guarantee collision-free encoding.

    Returns a ChunkedArray of strings (type Any due to pyarrow-stubs limitations).
    """
    str_cols: list[Any] = []
    # Keep pre-sentinel versions for separator validation (sentinel doesn't contain
    # the separator, so we must validate against the original non-null values).
    raw_str_cols: list[Any] = []
    for col_name in on:
        col = table.column(col_name)
        if not pa.types.is_string(col.type) and not pa.types.is_large_string(col.type):
            str_col: Any = pc.cast(col, pa.string())
        else:
            str_col = col
        raw_str_cols.append(str_col)
        # Replace NULLs with sentinel so they match each other
        str_cols.append(pc.if_else(pc.is_null(str_col), _NULL_SENTINEL, str_col))

    # Validate no non-null values contain the separator (guarantees collision-free keys).
    if len(str_cols) > 1:
        for str_col in raw_str_cols:
            non_null_mask = pc.invert(pc.is_null(str_col))
            non_null_vals = pc.filter(str_col, non_null_mask)
            if len(non_null_vals) > 0 and pc.any(pc.match_substring(non_null_vals, _KEY_SEPARATOR)).as_py():
                raise ValueError(
                    "Equality delete join column contains the composite key separator "
                    "(0x1E 0x1F). Multi-column anti-join requires column values to not "
                    "contain control characters used for key encoding. This is a data "
                    "limitation of the PyArrow composite-key anti-join path. "
                    "Install DataFusion (`pip install 'pyiceberg[datafusion]'`) to use "
                    "SQL-based anti-join which handles arbitrary column values."
                )

    result: Any = str_cols[0]
    for str_col in str_cols[1:]:
        result = pc.binary_join_element_wise(result, str_col, _KEY_SEPARATOR)
    return result
