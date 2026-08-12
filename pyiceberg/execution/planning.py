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

"""Planning backend implementations.

InMemoryPlanner: Wraps the existing ManifestGroupPlanner. Uses a Python dict
(DeleteFileIndex) to assign delete files to data files. Fast for tables with
fewer than ~100K delete files. This is the default and handles the vast
majority of real-world tables.

BoundedMemoryPlanner: Uses DataFusion SQL joins with spill-to-disk for
extreme-scale tables with millions of uncompacted delete files. Activated
automatically when the delete file count exceeds the planning threshold.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile, ManifestFile
    from pyiceberg.table import FileScanTask, ManifestGroupPlanner
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.typedef import Record

__all__ = ["BoundedMemoryPlanner", "InMemoryPlanner"]

#: Batch size for streaming manifest entries to temp Parquet files.
_ENTRY_BATCH_SIZE: int = 8192


class InMemoryPlanner:
    """Default planning backend: in-memory DeleteFileIndex.

    Wraps ManifestGroupPlanner.plan_files() which uses a Python dict to hold
    delete file metadata and performs O(1) lookup per data file.

    Memory: O(num_manifest_entries × ~200 bytes).
    Suitable for tables with fewer than ~100K delete files (<20 MB).

    For extreme-scale tables (millions of delete files), consider a
    bounded-memory planner that uses a compute backend for the assignment join.
    """

    def plan_files(
        self,
        manifests: Iterable[ManifestFile],
        table_metadata: TableMetadata,
        row_filter: BooleanExpression,
        io: FileIO,
        case_sensitive: bool = True,
    ) -> Iterator[FileScanTask]:
        """Plan files using the existing ManifestGroupPlanner (in-memory index).

        Delegates to ManifestGroupPlanner which builds a DeleteFileIndex (Python dict)
        for O(1) per-data-file delete assignment. Suitable for tables with <100K delete
        files. For larger tables, use BoundedMemoryPlanner.
        """
        from pyiceberg.table import ManifestGroupPlanner

        planner = ManifestGroupPlanner(
            table_metadata=table_metadata,
            io=io,
            row_filter=row_filter,
            case_sensitive=case_sensitive,
        )
        yield from planner.plan_files(manifests)


class BoundedMemoryPlanner:
    """Bounded-memory planning backend using DataFusion for delete assignment.

    All three phases operate within bounded memory:
    - Phase 1 (_stream_entries_to_parquet): O(batch_size) -- entries serialized
      and flushed in batches of 8192, never all held simultaneously.
    - Phase 2 (_execute_assignment_join): O(memory_limit) -- DataFusion spills
      to disk if the join exceeds the configured budget.
    - Phase 3 (_yield_scan_tasks): O(batch_size) -- iterates join output one
      batch at a time. Delete file blobs are carried through the SQL join
      via ARRAY_AGG(del.data_file_json), so no Python-side lookup dicts
      are needed. Each batch is independent -- nothing accumulates.

    Total memory: O(memory_limit + batch_size) -- truly bounded regardless
    of table scale. The only per-table growth is DataFusion's internal state,
    which spills to disk via FairSpillPool when memory_limit is exceeded.

    Requires: pip install 'pyiceberg[datafusion]'

    Use when: table has >100K delete files and in-memory planning OOMs during the
    assignment join (not during entry enumeration).
    """

    #: SQL output column aliases — shared between _ASSIGNMENT_SQL and _yield_scan_tasks.
    _COL_DATA_BLOB: str = "data_blob"
    _COL_DELETE_BLOBS: str = "delete_blobs"

    #: SQL for assigning delete files to data files.
    #: Per Iceberg spec: position deletes apply when del.seq >= data.seq,
    #: equality deletes apply when del.seq > data.seq (strictly greater).
    _ASSIGNMENT_SQL: str = """
        SELECT
            d.file_path AS data_path,
            d.sequence_number AS data_seq,
            d.data_file_json AS data_blob,
            ARRAY_AGG(del.data_file_json) FILTER (WHERE del.file_path IS NOT NULL) AS delete_blobs
        FROM data_entries d
        LEFT JOIN delete_entries del
            ON d.partition_key = del.partition_key
            AND CASE
                WHEN del.content = 2 THEN del.sequence_number > d.sequence_number
                ELSE del.sequence_number >= d.sequence_number
            END
        GROUP BY d.file_path, d.sequence_number, d.data_file_json
    """

    def __init__(self, memory_limit: int | None = None) -> None:
        """Initialize the bounded-memory planner with optional memory limit."""
        from pyiceberg.execution.engine import get_memory_limit

        self._memory_limit = memory_limit if memory_limit is not None else get_memory_limit()
        # Holds the DataFusion SessionContext alive while the stream from
        # _execute_assignment_join is being consumed by _yield_scan_tasks.
        # Without this reference, Python could GC the context before the lazy
        # stream is fully consumed, risking use-after-free in Rust internals.
        self._active_ctx: Any = None

    def plan_files(
        self,
        manifests: Iterable[ManifestFile],
        table_metadata: TableMetadata,
        row_filter: BooleanExpression,
        io: FileIO,
        case_sensitive: bool = True,
    ) -> Iterator[FileScanTask]:
        """Plan files using DataFusion SQL join for delete assignment."""
        import tempfile
        from pathlib import Path

        from pyiceberg.table import ManifestGroupPlanner

        planner = ManifestGroupPlanner(
            table_metadata=table_metadata,
            io=io,
            row_filter=row_filter,
            case_sensitive=case_sensitive,
        )

        # Use NamedTemporaryFile for cross-platform temp paths, then close immediately.
        # Pattern is intentional: we need paths for parquet streaming, and manually
        # control cleanup via try/finally.
        data_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)  # noqa: SIM115
        delete_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)  # noqa: SIM115
        data_tmp_path = data_tmp.name
        delete_tmp_path = delete_tmp.name
        data_tmp.close()
        delete_tmp.close()

        try:
            # Phase 1: Stream entries to temp Parquet (no dicts accumulated)
            self._stream_entries_to_parquet(planner, manifests, data_tmp_path, delete_tmp_path)

            # Phase 2: Execute SQL join for delete assignment
            result = self._execute_assignment_join(data_tmp_path, delete_tmp_path)

            # Phase 3: Yield FileScanTasks from join result (deserialize from blobs)
            yield from self._yield_scan_tasks(
                result,
                data_tmp_path,
                delete_tmp_path,
                table_metadata,
                row_filter,
                case_sensitive,
            )
        finally:
            Path(data_tmp_path).unlink(missing_ok=True)
            Path(delete_tmp_path).unlink(missing_ok=True)

    def _stream_entries_to_parquet(
        self,
        planner: ManifestGroupPlanner,
        manifests: Iterable[ManifestFile],
        data_tmp_path: str,
        delete_tmp_path: str,
    ) -> None:
        """Phase 1: Stream manifest entries to temp Parquet files."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from pyiceberg.manifest import DataFileContent

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

        data_writer = pq.ParquetWriter(data_tmp_path, schema=data_schema)
        delete_writer = pq.ParquetWriter(delete_tmp_path, schema=delete_schema)

        data_buffer: list[dict[str, Any]] = []
        delete_buffer: list[dict[str, Any]] = []

        # Process manifest-by-manifest for GC of each manifest's entries.
        for manifest_entries in planner.plan_manifest_entries(manifests):
            for entry in manifest_entries:
                data_file = entry.data_file
                seq = entry.sequence_number or 0
                partition_key = _serialize_partition_key(data_file.spec_id, data_file.partition)
                blob = _serialize_data_file(data_file)

                if data_file.content == DataFileContent.DATA:
                    data_buffer.append(
                        {
                            "file_path": data_file.file_path,
                            "partition_key": partition_key,
                            "sequence_number": seq,
                            "record_count": data_file.record_count,
                            "spec_id": data_file.spec_id,
                            "data_file_json": blob,
                        }
                    )
                    if len(data_buffer) >= _ENTRY_BATCH_SIZE:
                        data_writer.write_batch(pa.RecordBatch.from_pylist(data_buffer, schema=data_schema))
                        data_buffer.clear()
                else:
                    delete_buffer.append(
                        {
                            "file_path": data_file.file_path,
                            "partition_key": partition_key,
                            "sequence_number": seq,
                            "content": data_file.content.value,
                            "data_file_json": blob,
                        }
                    )
                    if len(delete_buffer) >= _ENTRY_BATCH_SIZE:
                        delete_writer.write_batch(pa.RecordBatch.from_pylist(delete_buffer, schema=delete_schema))
                        delete_buffer.clear()

            del manifest_entries

        if data_buffer:
            data_writer.write_batch(pa.RecordBatch.from_pylist(data_buffer, schema=data_schema))
        if delete_buffer:
            delete_writer.write_batch(pa.RecordBatch.from_pylist(delete_buffer, schema=delete_schema))

        data_writer.close()
        delete_writer.close()

    def _execute_assignment_join(self, data_tmp_path: str, delete_tmp_path: str) -> Iterator[Any]:
        """Phase 2: Execute SQL LEFT JOIN for delete-to-data assignment."""
        from datafusion import RuntimeEnvBuilder, SessionContext

        runtime = RuntimeEnvBuilder().with_fair_spill_pool(self._memory_limit).with_disk_manager_os()
        ctx = SessionContext(runtime=runtime)
        ctx.register_parquet("data_entries", data_tmp_path)
        ctx.register_parquet("delete_entries", delete_tmp_path)

        stream = ctx.sql(self._ASSIGNMENT_SQL).execute_stream()
        # Hold ctx reference on self to prevent GC before stream is consumed.
        # DataFusion's Rust internals use Arc<SessionState>, so this is likely
        # redundant — but it makes the lifetime contract explicit and protects
        # against future datafusion-python refactors.
        self._active_ctx = ctx
        return stream

    def _yield_scan_tasks(
        self,
        join_result_stream: Iterator[Any],
        data_tmp_path: str,
        delete_tmp_path: str,
        table_metadata: TableMetadata,
        row_filter: BooleanExpression,
        case_sensitive: bool,
    ) -> Iterator[FileScanTask]:
        """Phase 3: Yield FileScanTasks from the join result stream."""
        from pyiceberg.expressions.visitors import residual_evaluator_of
        from pyiceberg.table import FileScanTask

        residual_evaluators: dict[int, Any] = {}

        for batch in join_result_stream:
            pa_batch = batch.to_pyarrow() if hasattr(batch, "to_pyarrow") else batch
            for i in range(pa_batch.num_rows):
                data_blob = pa_batch.column(self._COL_DATA_BLOB)[i].as_py()
                delete_blobs_col = pa_batch.column(self._COL_DELETE_BLOBS)[i].as_py()

                if data_blob is None:
                    continue

                data_file_obj = _deserialize_data_file(data_blob)

                delete_files: set[DataFile] = set()
                if delete_blobs_col:
                    for del_blob in delete_blobs_col:
                        if del_blob is not None:
                            delete_files.add(_deserialize_data_file(del_blob))

                spec_id = data_file_obj.spec_id
                if spec_id not in residual_evaluators:
                    spec = table_metadata.specs()[spec_id]
                    residual_evaluators[spec_id] = residual_evaluator_of(
                        spec=spec,
                        expr=row_filter,
                        case_sensitive=case_sensitive,
                        schema=table_metadata.schema(),
                    )

                residual = residual_evaluators[spec_id].residual_for(data_file_obj.partition)

                yield FileScanTask(
                    data_file=data_file_obj,
                    delete_files=delete_files if delete_files else None,
                    residual=residual,
                )


def _serialize_data_file(df: DataFile) -> bytes:
    """Serialize a DataFile to bytes for temp Parquet storage during planning.

    Uses pickle because DataFile extends Record with a complex internal _data
    array (partition Records, dict[int, bytes] for bounds, nested types) that
    has no existing JSON/Avro serialization path.

    Security boundary: strictly process-local. The serialized bytes are written
    to temp Parquet files (same-process, same-machine, deleted on completion)
    and never cross network boundaries, persist beyond the operation, or accept
    external input. The deserialization call site only processes bytes that this
    function produced within the same process invocation.
    """
    import pickle

    return pickle.dumps(df)


def _deserialize_data_file(blob: bytes) -> DataFile:
    """Deserialize a DataFile from bytes produced by _serialize_data_file.

    Security: only called on blobs produced by _serialize_data_file within the
    same process. The temp Parquet files containing these blobs are process-private
    and deleted immediately after use (see plan_files() finally block).
    """
    import pickle

    return pickle.loads(blob)


def _serialize_partition_key(spec_id: int, partition: Record | None) -> str:
    """Serialize a (spec_id, partition_record) pair to a stable string for SQL joining.

    Produces a deterministic JSON array: [spec_id, val1, val2, ...] with special
    float handling (NaN/Infinity as strings) and custom serialization for bytes,
    Decimal, datetime, and UUID partition values.

    Examples:
        >>> from pyiceberg.typedef import Record
        >>> _serialize_partition_key(0, None)
        '0'
        >>> _serialize_partition_key(1, Record(2024, "us-east-1"))
        '[1,2024,"us-east-1"]'
    """
    if partition is None:
        return str(spec_id)

    try:
        values: list[Any] = [partition[i] for i in range(len(partition))]
    except (TypeError, IndexError):
        values = [repr(partition)]

    # Sanitize IEEE 754 special floats for RFC 8259-compliant JSON.
    sanitized = [_sanitize_partition_value(v) for v in values]
    return json.dumps([spec_id] + sanitized, separators=(",", ":"), default=_partition_value_serializer, sort_keys=False)


def _sanitize_partition_value(value: Any) -> Any:
    """Pre-process a partition value before JSON serialization (handles special floats)."""
    import math

    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        if math.isnan(value):
            return "NaN"
        elif value > 0:
            return "Infinity"
        else:
            return "-Infinity"
    return value


def _partition_value_serializer(value: Any) -> Any:
    """JSON default serializer for Iceberg partition value types."""
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    elif isinstance(value, Decimal):
        return str(value)
    elif isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    elif isinstance(value, UUID):
        return str(value)
    else:
        raise TypeError(
            f"Unsupported partition value type for serialization: {type(value).__name__}. "
            f"Iceberg partition values must be one of: int, float, bool, str, None, "
            f"bytes, memoryview, Decimal, date, datetime, UUID."
        )
