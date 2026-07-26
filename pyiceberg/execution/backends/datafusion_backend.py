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

"""DataFusion execution backend -- bounded-memory compute via spill-to-disk.

This backend uses Apache DataFusion (via datafusion-python) for all compute
operations. It configures FairSpillPool to ensure sort, join, and filter
operations complete within a configurable memory budget by spilling
intermediate state to local SSD as Arrow IPC.

Key properties:
- Per-session memory isolation (each SessionContext has its own pool)
- External merge sort for ORDER BY (spills sorted runs to disk)
- Grace Hash Join for anti-join/join (partitions and spills)
- Streaming filter with O(batch_size) memory
- Apache 2.0 licensed (including object store access)

Output materialization:
    File-based methods (sort_from_files, anti_join_from_files, etc.) call
    to_arrow_table() inside the _scoped_env_vars block. This materializes the
    full result in Python memory AFTER the bounded-memory operation completes.
    The sort/join itself is bounded (DataFusion spills), but the delivery to Python
    is O(result_size). This is a known limitation of the credential-scoping approach:
    lazy evaluation (execute_stream) would restore env vars before data is read.

    TODO(datafusion-python#1624): Switch to execute_stream() once per-session object
    store config lands. Unblocked by:
      - https://github.com/apache/datafusion-python/issues/1624
      - https://github.com/apache/datafusion-python/pull/1625

Requires: pip install 'pyiceberg[datafusion]'
"""

from __future__ import annotations

__all__ = ["DataFusionComputeBackend", "DataFusionReadBackend"]

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from pyiceberg.execution.protocol import SortKeyList
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Properties

logger = logging.getLogger(__name__)


def _resolve_memory_limit(limit: int | None) -> int:
    """Return memory limit in bytes, using configured default when not specified.

    Guards against zero or negative values which would cause undefined behavior
    in DataFusion's FairSpillPool (division by zero in pool allocation).
    """
    from pyiceberg.execution.engine import get_memory_limit

    if limit is not None and limit > 0:
        return limit
    return get_memory_limit()


#: Default threshold above which a ResourceWarning is emitted for large materializations.
#: Configurable via execution.materialization-warning-threshold in .pyiceberg.yaml (bytes)
#: or PYICEBERG_EXECUTION__MATERIALIZATION_WARNING_THRESHOLD env var.
#: Set to 0 to disable. Default: 1 GB.
_MATERIALIZATION_WARNING_THRESHOLD_DEFAULT: int = 1 * 1024 * 1024 * 1024


def _get_materialization_warning_threshold() -> int:
    """Read the materialization warning threshold from config or default (1 GB)."""
    from pyiceberg.execution.engine import get_execution_config_int

    return get_execution_config_int("materialization-warning-threshold", _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT)


def _warn_if_large_materialization(table: pa.Table) -> None:
    """Emit a ResourceWarning if the materialized result exceeds the configured threshold."""
    import warnings

    threshold = _get_materialization_warning_threshold()
    if threshold <= 0:
        return

    nbytes = table.nbytes
    if nbytes > threshold:
        size_gb = nbytes / (1024 * 1024 * 1024)
        warnings.warn(
            f"DataFusion operation materialized {size_gb:.1f} GB into Python memory. "
            f"The compute was bounded-memory (spilled to disk), but result delivery "
            f"to Python requires full materialization due to credential scoping. "
            f"Consider writing intermediate results to temp Parquet via "
            f"materialize_to_parquet() for downstream operations.",
            ResourceWarning,
            stacklevel=3,
        )


def _create_session(memory_limit: int | None = None):
    """Create a DataFusion SessionContext with bounded memory and spill-to-disk."""
    from datafusion import RuntimeEnvBuilder, SessionContext

    limit = _resolve_memory_limit(memory_limit)
    runtime = RuntimeEnvBuilder().with_fair_spill_pool(limit).with_disk_manager_os()
    return SessionContext(runtime=runtime)


class DataFusionComputeBackend:
    """DataFusion compute backend -- bounded-memory via FairSpillPool."""

    @property
    def supports_bounded_memory(self) -> bool:
        """Return True because this backend supports spill-to-disk for all operations."""
        return True

    def sort(
        self,
        data: Iterator[pa.RecordBatch],
        sort_keys: SortKeyList,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """External merge sort with spill-to-disk on pre-materialized data."""
        ctx = _create_session(memory_limit)

        batches = list(data)
        if not batches:
            return iter(())

        ctx.register_record_batches("sort_input", [batches])

        from pyiceberg.execution.expression_to_sql import sort_direction_to_sql

        order_clause = ", ".join(f'"{col}" {sort_direction_to_sql(direction)}' for col, direction in sort_keys)
        result = ctx.sql(f"SELECT * FROM sort_input ORDER BY {order_clause}")
        return iter(result.to_arrow_table().to_batches())

    def sort_from_files(
        self,
        file_paths: list[str],
        sort_keys: SortKeyList,
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Sort from Parquet files with bounded memory via DataFusion spill-to-disk."""
        if not file_paths:
            return iter(())

        from pyiceberg.execution.object_store import _scoped_env_vars, datafusion_env_vars_from_properties

        ctx = _create_session(memory_limit)
        env_vars = datafusion_env_vars_from_properties(io_properties)

        with _scoped_env_vars(env_vars):
            for i, path in enumerate(file_paths):
                ctx.register_parquet(f"file_{i}", path)

            union = " UNION ALL ".join(f"SELECT * FROM file_{i}" for i in range(len(file_paths)))

            from pyiceberg.execution.expression_to_sql import sort_direction_to_sql

            order = ", ".join(f'"{col}" {sort_direction_to_sql(d)}' for col, d in sort_keys)
            result = ctx.sql(f"SELECT * FROM ({union}) ORDER BY {order}")
            # Must materialize inside _scoped_env_vars for cloud auth.
            # TODO(datafusion-python#1624): Switch to execute_stream() for true streaming.
            arrow_table = result.to_arrow_table()
            _warn_if_large_materialization(arrow_table)
            return iter(arrow_table.to_batches())

    def anti_join(
        self,
        left: Iterator[pa.RecordBatch],
        right: Iterator[pa.RecordBatch],
        on: list[str],
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """LEFT ANTI JOIN via Grace Hash Join with spill on pre-materialized data."""
        ctx = _create_session(memory_limit)

        left_batches = list(left)
        right_batches = list(right)
        if not left_batches:
            return iter(())
        if not right_batches:
            return iter(left_batches)

        ctx.register_record_batches("left_tbl", [left_batches])
        ctx.register_record_batches("right_tbl", [right_batches])

        # Parenthesization required: sqlparser-rs (v0.62) precedence for IS NOT DISTINCT FROM
        # is lower than AND, causing `a ISNDFO b AND c ISNDFO d` to mis-parse as
        # `a ISNDFO (b AND c) ISNDFO d`. Explicit parens avoid the ambiguity.
        # See: https://github.com/apache/datafusion/issues/23692
        join_cond = " AND ".join(f'(l."{col}" IS NOT DISTINCT FROM r."{col}")' for col in on)
        sql = f"SELECT l.* FROM left_tbl l LEFT ANTI JOIN right_tbl r ON {join_cond}"

        result = ctx.sql(sql)
        return iter(result.to_arrow_table().to_batches())

    def anti_join_from_files(
        self,
        left_paths: list[str],
        right_paths: list[str],
        on: list[str],
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """LEFT ANTI JOIN from Parquet files with bounded memory via DataFusion spill-to-disk."""
        from pyiceberg.execution.object_store import _scoped_env_vars, datafusion_env_vars_from_properties

        ctx = _create_session(memory_limit)
        env_vars = datafusion_env_vars_from_properties(io_properties)

        with _scoped_env_vars(env_vars):
            left_tables = []
            for i, path in enumerate(left_paths):
                name = f"left_{i}"
                ctx.register_parquet(name, path)
                left_tables.append(name)

            right_tables = []
            for i, path in enumerate(right_paths):
                name = f"right_{i}"
                ctx.register_parquet(name, path)
                right_tables.append(name)

            left_union = " UNION ALL ".join(f"SELECT * FROM {t}" for t in left_tables)
            right_union = " UNION ALL ".join(f"SELECT * FROM {t}" for t in right_tables)

            # Parenthesization required: sqlparser-rs (v0.62) precedence for IS NOT DISTINCT FROM
            # is lower than AND, causing mis-parsing without explicit parens.
            # See: https://github.com/apache/datafusion/issues/23692
            join_cond = " AND ".join(f'(l."{col}" IS NOT DISTINCT FROM r."{col}")' for col in on)
            sql = f"SELECT l.* FROM ({left_union}) l LEFT ANTI JOIN ({right_union}) r ON {join_cond}"

            result = ctx.sql(sql)
            # Must materialize inside _scoped_env_vars for cloud auth.
            # TODO(datafusion-python#1624): Switch to execute_stream() for true streaming.
            arrow_table = result.to_arrow_table()
            _warn_if_large_materialization(arrow_table)
            return iter(arrow_table.to_batches())

    def filter(
        self,
        data: Iterator[pa.RecordBatch],
        predicate: BooleanExpression,
    ) -> Iterator[pa.RecordBatch]:
        """Filter using PyArrow compute -- streaming, O(1) memory."""
        from pyiceberg.execution.backends.pyarrow_backend import _filter_batches

        return _filter_batches(data, predicate)

    def apply_positional_deletes(
        self,
        data_path: str,
        position_delete_paths: list[str],
        projected_schema: Schema,
        io_properties: Properties,
        memory_limit: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Apply positional deletes via DataFusion LEFT ANTI JOIN with bounded memory."""
        import tempfile
        from pathlib import Path

        import pyarrow.dataset as ds
        import pyarrow.parquet as pq

        from pyiceberg.execution.object_store import _scoped_env_vars, datafusion_env_vars_from_properties
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        ctx = _create_session(memory_limit)
        env_vars = datafusion_env_vars_from_properties(io_properties)

        # Phase 1: Stream data file to temp Parquet with _pyiceberg_pos column.
        tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", prefix="pyiceberg_posdelete_", delete=False)
        tmp_path = tmp_file.name
        tmp_file.close()

        # Register with atexit tracker for cleanup if process is killed mid-operation.
        from pyiceberg.execution.materialize import _active_temp_files, _temp_files_lock

        with _temp_files_lock:
            _active_temp_files.add(tmp_path)

        try:
            with _scoped_env_vars(env_vars):
                dataset = ds.dataset(data_path, format="parquet")

                # Project only the columns we need. The _pyiceberg_pos column is
                # derived from row order (independent of column projection), so
                # reading fewer columns doesn't affect position correctness.
                pa_schema = schema_to_pyarrow(projected_schema, include_field_ids=False)
                projected_columns = [field.name for field in pa_schema]

                # Only request columns that exist in the file (schema evolution:
                # columns added after this file was written won't be in the file).
                file_columns = set(dataset.schema.names)
                available_columns = [c for c in projected_columns if c in file_columns]

                # use_threads=False ensures batches arrive in physical row order,
                # which is required for correct _pyiceberg_pos assignment.
                scanner = dataset.scanner(
                    columns=available_columns if available_columns else None,
                    use_threads=False,
                )

                source_schema = scanner.projected_schema
                pos_field = pa.field("_pyiceberg_pos", pa.int64())
                tmp_schema = pa.schema(list(source_schema) + [pos_field])

                writer = pq.ParquetWriter(tmp_path, schema=tmp_schema)
                row_offset = 0
                for batch in scanner.to_batches():
                    pos_array = pa.array(range(row_offset, row_offset + batch.num_rows), type=pa.int64())
                    batch_with_pos = batch.append_column(pos_field, pos_array)
                    writer.write_batch(batch_with_pos)
                    row_offset += batch.num_rows
                writer.close()

                # Phase 2: Register files with DataFusion.
                ctx.register_parquet("data_file", tmp_path)

                for i, del_path in enumerate(position_delete_paths):
                    ctx.register_parquet(f"del_{i}", del_path)

                # Phase 3: LEFT ANTI JOIN on position column.
                escaped_data_path = data_path.replace("'", "''")
                del_union = " UNION ALL ".join(
                    f"SELECT pos FROM del_{i} WHERE file_path = '{escaped_data_path}'" for i in range(len(position_delete_paths))
                )

                columns = ", ".join(f'd."{field.name}"' for field in pa_schema)

                sql = f"""
                    SELECT {columns}
                    FROM data_file d
                    LEFT ANTI JOIN ({del_union}) del
                        ON d._pyiceberg_pos = del.pos
                """

                result = ctx.sql(sql)
                # TODO(datafusion-python#1624): Switch to execute_stream() for true streaming.
                arrow_table = result.to_arrow_table()
                _warn_if_large_materialization(arrow_table)
                return iter(arrow_table.to_batches())
        finally:
            with _temp_files_lock:
                _active_temp_files.discard(tmp_path)
            Path(tmp_path).unlink(missing_ok=True)


class DataFusionReadBackend:
    """DataFusion IO backend -- reads Parquet via DataFusion's native reader (experimental)."""

    def read_parquet(
        self,
        location: str,
        projected_schema: Schema,
        row_filter: BooleanExpression,
        io_properties: Properties,
        dictionary_columns: tuple[str, ...] = (),
    ) -> Iterator[pa.RecordBatch]:
        """Read Parquet via DataFusion register_parquet + SQL."""
        from pyiceberg.execution.expression_to_sql import expression_to_sql
        from pyiceberg.execution.object_store import _scoped_env_vars, datafusion_env_vars_from_properties
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        ctx = _create_session(None)
        env_vars = datafusion_env_vars_from_properties(io_properties)

        with _scoped_env_vars(env_vars):
            ctx.register_parquet("read_source", location)

            pa_schema = schema_to_pyarrow(projected_schema, include_field_ids=False)
            columns = ", ".join(f'"{field.name}"' for field in pa_schema)

            if isinstance(row_filter, AlwaysTrue):
                sql = f"SELECT {columns} FROM read_source"
            else:
                try:
                    where = expression_to_sql(row_filter)
                    sql = f"SELECT {columns} FROM read_source WHERE {where}"
                except (TypeError, ValueError, KeyError, NotImplementedError) as e:
                    logger.debug("Could not convert row_filter to SQL (will post-filter): %s", e)
                    sql = f"SELECT {columns} FROM read_source"

            result = ctx.sql(sql)
            # Must materialize inside _scoped_env_vars for cloud auth.
            # TODO(datafusion-python#1624): Switch to execute_stream() once per-session
            # object store config lands.
            # Track: https://github.com/apache/datafusion-python/pull/1625
            arrow_table = result.to_arrow_table()
            _warn_if_large_materialization(arrow_table)
            return iter(arrow_table.to_batches())
