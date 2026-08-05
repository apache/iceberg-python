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

"""Protocol definitions for pluggable read, write, and compute backends.

Three independent axes, composable via Arrow RecordBatch at every boundary:
    ReadBackend  : Path → Iterator[RecordBatch]
    WriteBackend : (OutputFile, Schema, Table, FormatModel) → DataFileStatistics
    ComputeBackend : sort, join, filter with optional spill-to-disk
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Properties

#: Type alias for a single sort key: (column_name, direction).
SortKey = tuple[str, Literal["ascending", "descending"]]

#: Type alias for a list of sort keys defining a sort order.
#: Named ``SortKeyList`` (not ``SortOrder``) to avoid shadowing
#: ``pyiceberg.table.sorting.SortOrder`` (the Pydantic model used
#: throughout the codebase for Iceberg's spec-level sort order).
SortKeyList = list[SortKey]

#: Default memory budget for compute operations (512 MB).
DEFAULT_MEMORY_LIMIT: int = 512 * 1024 * 1024


# =============================================================================
# Axis 1: READ
# =============================================================================


@runtime_checkable
class ReadBackend(Protocol):
    """Decodes Parquet files into Arrow RecordBatches with projection and predicate pushdown."""

    def read_parquet(
        self,
        location: str,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: Mapping[str, Any],
        dictionary_columns: tuple[str, ...] = (),
    ) -> Iterator[pa.RecordBatch]:
        """Read a Parquet file with projection and optional filter pushdown.

        Returns a superset of matching rows if the backend cannot evaluate the full filter.
        """
        ...


# =============================================================================
# Axis 2: WRITE
# =============================================================================


@runtime_checkable
class WriteBackend(Protocol):
    """Executes the physical write of Arrow data to storage via FileFormatModel."""

    def write_data_file(
        self,
        output_file: Any,
        file_schema: Schema,
        properties: Properties,
        arrow_table: pa.Table,
        format_model: Any,
    ) -> Any:
        """Write an Arrow table to a single data file via the format model.

        Returns:
            DataFileStatistics with record count, column sizes, bounds, etc.
        """
        ...


# =============================================================================
# Axis 3: COMPUTE
# =============================================================================


@runtime_checkable
class ComputeBackend(Protocol):
    """Sort, join, filter, and positional delete operations on Arrow data.

    File-based methods (sort_from_files, anti_join_from_files) let the backend
    control the read lifecycle for spill-to-disk. All implementations MUST produce
    identical results for the same input regardless of supports_bounded_memory.
    """

    @property
    def supports_bounded_memory(self) -> bool:
        """Whether this backend can spill to disk when memory_limit is exceeded.

        Used by callers to gate best-effort optimizations (e.g., sort-on-write)
        that would be unsafe without bounded-memory guarantees.
        """
        ...

    def sort(
        self,
        data: Iterator[pa.RecordBatch],
        sort_keys: SortKeyList,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Sort pre-materialized Arrow data."""
        ...

    def anti_join(
        self,
        left: Iterator[pa.RecordBatch],
        right: Iterator[pa.RecordBatch],
        on: list[str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """LEFT ANTI JOIN on pre-materialized Arrow data."""
        ...

    def sort_from_files(
        self,
        file_paths: list[str],
        sort_keys: SortKeyList,
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Sort data from Parquet files with bounded memory."""
        ...

    def anti_join_from_files(
        self,
        left_paths: list[str],
        right_paths: list[str],
        on: list[str],
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """LEFT ANTI JOIN from Parquet files with bounded memory."""
        ...

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]:
        """Filter Arrow data by a BooleanExpression (streaming, O(1) memory)."""
        ...

    def apply_positional_deletes(
        self,
        data_path: str,
        position_delete_paths: list[str],
        projected_schema: Schema,
        io_properties: Mapping[str, Any],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Read a data file and exclude rows at positions listed in delete files."""
        ...


# =============================================================================
# Resolved Backends
# =============================================================================


@dataclass(frozen=True)
class Backends:
    """Resolved backends for read, write, and compute."""

    read: ReadBackend
    write: WriteBackend
    compute: ComputeBackend
    io_properties: Mapping[str, Any]

    @property
    def supports_bounded_memory(self) -> bool:
        """Whether the compute backend can spill to disk."""
        return self.compute.supports_bounded_memory

    @classmethod
    def resolve(cls, io_properties: Properties, operation: str = "scan", **overrides: Any) -> Backends:
        """Resolve all three backends from properties and auto-detection.

        Args:
            io_properties: Storage credentials and configuration.
            operation: Name of the operation requesting backends.
            **overrides: Optional keys: read, write, compute (instances or string names).
        """
        from pyiceberg.execution.engine import build_backends

        return build_backends(io_properties, operation=operation, **overrides)
