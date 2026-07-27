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

"""Behavioral parity tests: old ArrowScan vs new orchestrate_scan.

These tests verify that the new pluggable backend (orchestrate_scan) produces
IDENTICAL output to the deprecated ArrowScan for the same input data and
configuration. This is the critical regression guard during the transition.

Tests cover:
1. Basic scan (no deletes, no filter)
2. Scan with row filter (residual evaluation)
3. Scan with positional deletes
4. Scan with limit
5. Empty scan (no matching files)

NOTE: These tests suppress the DeprecationWarning from ArrowScan so they
can compare outputs without noise. ArrowScan will be removed once these
parity tests pass for a full release cycle.

Verifies behavioral parity between old ArrowScan.to_table() and the new
orchestrate_scan path for the same input.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, EqualTo, GreaterThan, LessThan
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.types import IntegerType, NestedField, StringType

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="ArrowScan requires URI-style paths; Windows local paths are parsed incorrectly",
)


@pytest.fixture
def parity_schema() -> Schema:
    """Simple schema for parity tests."""
    return Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )


@pytest.fixture
def parity_table_metadata(parity_schema: Schema, tmp_path: Path) -> TableMetadataV2:
    """Minimal TableMetadata for parity tests."""
    return TableMetadataV2(
        location=str(tmp_path),
        last_column_id=2,
        format_version=2,
        current_schema_id=0,
        schemas=[parity_schema],
        partition_specs=[PartitionSpec()],
        default_spec_id=0,
        last_partition_id=0,
        sort_orders=[],
        default_sort_order_id=0,
        properties={},
    )


@pytest.fixture
def parity_data_file(tmp_path: Path, parity_schema: Schema) -> tuple[str, DataFile]:
    """Write a test Parquet file and return (path, DataFile)."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    file_path = str(tmp_path / "data" / "part-00000.parquet")
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    arrow_schema = schema_to_pyarrow(parity_schema, include_field_ids=True)
    table = pa.table(
        {"id": [1, 2, 3, 4, 5], "name": ["alpha", "beta", "gamma", "delta", "epsilon"]},
        schema=arrow_schema,
    )
    pq.write_table(table, file_path)

    import os

    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=None,
        record_count=5,
        file_size_in_bytes=os.path.getsize(file_path),
    )
    data_file.spec_id = 0
    return file_path, data_file


def _arrowscan_to_table(
    table_metadata: TableMetadataV2,
    io: FileIO,
    projected_schema: Schema,
    row_filter: BooleanExpression,
    tasks: list[FileScanTask],
    limit: int | None = None,
) -> pa.Table:
    """Call deprecated ArrowScan and return result, suppressing deprecation warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from pyiceberg.io.pyarrow import ArrowScan

        scan = ArrowScan(
            table_metadata=table_metadata,
            io=io,
            projected_schema=projected_schema,
            row_filter=row_filter,
            limit=limit,
        )
        return scan.to_table(tasks)


def _orchestrate_to_table(
    table_metadata: TableMetadataV2,
    io: FileIO,
    projected_schema: Schema,
    row_filter: BooleanExpression,
    tasks: list[FileScanTask],
    limit: int | None = None,
) -> pa.Table:
    """Call orchestrate_scan via the new backend path and return result."""
    from pyiceberg.execution._orchestrate import orchestrate_scan
    from pyiceberg.execution.protocol import Backends
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    backends = Backends.resolve(io.properties)

    # orchestrate_scan uses task.residual for post-filtering (not row_filter param).
    # When tasks are constructed directly (not via planner), set residual explicitly.
    if not isinstance(row_filter, AlwaysTrue):
        from pyiceberg.expressions.visitors import bind

        bound_filter = bind(table_metadata.schema(), row_filter, case_sensitive=True)
        tasks = [FileScanTask(data_file=t.file, delete_files=t.delete_files, residual=bound_filter) for t in tasks]

    batches = orchestrate_scan(
        backends=backends,
        tasks=iter(tasks),
        table_metadata=table_metadata,
        projected_schema=projected_schema,
        row_filter=row_filter,
        case_sensitive=True,
    )

    arrow_schema = schema_to_pyarrow(projected_schema, include_field_ids=False)
    all_batches = list(batches)
    if not all_batches:
        return arrow_schema.empty_table()

    result = pa.concat_tables(
        (pa.Table.from_batches([b]) for b in all_batches),
        promote_options="permissive",
    )

    if limit is not None:
        result = result.slice(0, limit)

    return result


class TestArrowScanParityBasicScan:
    """Verify basic scan (no filter, no deletes) produces identical output."""

    def test_full_scan_same_result(
        self,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
        parity_data_file: tuple[str, DataFile],
    ) -> None:
        """ArrowScan and orchestrate_scan must produce the same table for a full scan."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        _, data_file = parity_data_file
        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=data_file)]

        old_result = _arrowscan_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks)
        new_result = _orchestrate_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks)

        # Both must have same row count and data
        assert old_result.num_rows == new_result.num_rows == 5
        assert old_result.column("id").to_pylist() == new_result.column("id").to_pylist()
        assert old_result.column("name").to_pylist() == new_result.column("name").to_pylist()

    def test_column_projection_same_result(
        self,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
        parity_data_file: tuple[str, DataFile],
    ) -> None:
        """Column projection produces same output from both paths."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        _, data_file = parity_data_file
        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=data_file)]

        # Project only "id" column
        projected = parity_schema.select("id")

        old_result = _arrowscan_to_table(parity_table_metadata, io, projected, AlwaysTrue(), tasks)
        new_result = _orchestrate_to_table(parity_table_metadata, io, projected, AlwaysTrue(), tasks)

        assert old_result.num_rows == new_result.num_rows == 5
        assert old_result.column("id").to_pylist() == new_result.column("id").to_pylist()
        assert old_result.num_columns == new_result.num_columns == 1


class TestArrowScanParityWithFilter:
    """Verify scans with row filters produce identical output."""

    def test_equality_filter_same_result(self,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
        parity_data_file: tuple[str, DataFile],
    ) -> None:
        """Equality filter produces same survivors from both paths."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        _, data_file = parity_data_file
        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=data_file)]

        row_filter = EqualTo("name", "gamma")

        old_result = _arrowscan_to_table(parity_table_metadata, io, parity_schema, row_filter, tasks)
        new_result = _orchestrate_to_table(parity_table_metadata, io, parity_schema, row_filter, tasks)

        assert old_result.num_rows == new_result.num_rows
        assert old_result.column("id").to_pylist() == new_result.column("id").to_pylist()

    def test_range_filter_same_result(self,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
        parity_data_file: tuple[str, DataFile],
    ) -> None:
        """Range filter produces same survivors from both paths."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        _, data_file = parity_data_file
        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=data_file)]

        row_filter = And(GreaterThan("id", 2), LessThan("id", 5))

        old_result = _arrowscan_to_table(parity_table_metadata, io, parity_schema, row_filter, tasks)
        new_result = _orchestrate_to_table(parity_table_metadata, io, parity_schema, row_filter, tasks)

        assert old_result.num_rows == new_result.num_rows
        assert sorted(old_result.column("id").to_pylist()) == sorted(new_result.column("id").to_pylist())


class TestArrowScanParityWithLimit:
    """Verify scans with limit produce identical output."""

    def test_limit_same_result(self,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
        parity_data_file: tuple[str, DataFile],
    ) -> None:
        """Limit produces same number of rows from both paths."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        _, data_file = parity_data_file
        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=data_file)]

        old_result = _arrowscan_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks, limit=3)
        new_result = _orchestrate_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks, limit=3)

        assert old_result.num_rows == 3
        assert new_result.num_rows == 3


class TestArrowScanParityEmptyScan:
    """Verify empty scans produce identical output."""

    def test_empty_tasks_same_result(self,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
    ) -> None:
        """Empty task list produces empty table from both paths."""
        from pyiceberg.io.pyarrow import PyArrowFileIO

        io = PyArrowFileIO()
        tasks: list[FileScanTask] = []

        old_result = _arrowscan_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks)
        new_result = _orchestrate_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks)

        assert old_result.num_rows == 0
        assert new_result.num_rows == 0


class TestArrowScanParityWithPositionalDeletes:
    """Verify scans with positional deletes produce identical output."""

    def test_positional_deletes_same_survivors(
        self,
        tmp_path: Path,
        parity_schema: Schema,
        parity_table_metadata: TableMetadataV2,
    ) -> None:
        """Positional deletes produce same surviving rows from both paths."""
        from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow

        io = PyArrowFileIO()

        # Write data file
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        data_path = str(data_dir / "data.parquet")

        arrow_schema = schema_to_pyarrow(parity_schema, include_field_ids=True)
        data_table = pa.table(
            {"id": [10, 20, 30, 40, 50], "name": ["a", "b", "c", "d", "e"]},
            schema=arrow_schema,
        )
        pq.write_table(data_table, data_path)

        # Write position delete file (delete rows at positions 1 and 3 → id=20 and id=40)
        del_path = str(data_dir / "pos_delete.parquet")
        del_table = pa.table(
            {
                "file_path": [data_path, data_path],
                "pos": pa.array([1, 3], type=pa.int64()),
            }
        )
        pq.write_table(del_table, del_path)

        import os

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_path,
            file_format=FileFormat.PARQUET,
            partition=None,
            record_count=5,
            file_size_in_bytes=os.path.getsize(data_path),
        )
        data_file.spec_id = 0
        delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path=del_path,
            file_format=FileFormat.PARQUET,
            partition=None,
            record_count=2,
            file_size_in_bytes=os.path.getsize(del_path),
        )
        delete_file.spec_id = 0

        tasks = [FileScanTask(data_file=data_file, delete_files={delete_file})]

        old_result = _arrowscan_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks)
        new_result = _orchestrate_to_table(parity_table_metadata, io, parity_schema, AlwaysTrue(), tasks)

        # Both should have 3 rows: id=10, 30, 50 (positions 0, 2, 4 survive)
        expected_ids = [10, 30, 50]
        assert sorted(old_result.column("id").to_pylist()) == expected_ids
        assert sorted(new_result.column("id").to_pylist()) == expected_ids


class TestSchemaEvolutionDuringScan:
    """Verify scans correctly handle files written under an older schema.

    When file schema differs from table schema (old files after
    schema evolution), the schema reconciliation path in orchestrate_scan
    where the file schema has fewer columns than the projected schema.

    Scenario: Table schema has columns (id, name, category). A data file was written
    when the table only had (id, name). Scanning with the full schema should produce
    NULL for the 'category' column in rows from the old file.
    """

    def test_old_file_missing_column_returns_nulls(self, tmp_path: Path) -> None:
        """File written before schema evolution has NULLs for new columns."""
        from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow

        # Current table schema (after evolution): id, name, category
        current_schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "category", StringType(), required=False),
        )

        # The file was written with old schema (id, name only)
        old_schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        # Write file with old schema
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        file_path = str(data_dir / "old_file.parquet")

        arrow_old_schema = schema_to_pyarrow(old_schema, include_field_ids=True)
        old_table = pa.table(
            {"id": [1, 2, 3], "name": ["a", "b", "c"]},
            schema=arrow_old_schema,
        )
        pq.write_table(old_table, file_path)

        import os

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=None,
            record_count=3,
            file_size_in_bytes=os.path.getsize(file_path),
        )
        data_file.spec_id = 0

        table_metadata = TableMetadataV2(
            location=str(tmp_path),
            last_column_id=3,
            format_version=2,
            current_schema_id=0,
            schemas=[current_schema],
            partition_specs=[PartitionSpec()],
            default_spec_id=0,
            last_partition_id=0,
            sort_orders=[],
            default_sort_order_id=0,
            properties={},
        )

        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=data_file)]

        # Scan with full current schema -- 'category' should be NULL
        result = _orchestrate_to_table(table_metadata, io, current_schema, AlwaysTrue(), tasks)

        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["a", "b", "c"]
        # New column should be all NULLs
        assert result.column("category").to_pylist() == [None, None, None]

    def test_old_and_new_files_combined(self, tmp_path: Path) -> None:
        """Scan combining old-schema and new-schema files produces correct result."""
        from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow

        current_schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "category", StringType(), required=False),
        )
        old_schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # Old file (missing category)
        old_path = str(data_dir / "old.parquet")
        arrow_old = schema_to_pyarrow(old_schema, include_field_ids=True)
        pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=arrow_old), old_path)

        # New file (has category)
        new_path = str(data_dir / "new.parquet")
        arrow_new = schema_to_pyarrow(current_schema, include_field_ids=True)
        pq.write_table(
            pa.table({"id": [3, 4], "name": ["c", "d"], "category": ["x", "y"]}, schema=arrow_new),
            new_path,
        )

        import os

        old_data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=old_path,
            file_format=FileFormat.PARQUET,
            partition=None,
            record_count=2,
            file_size_in_bytes=os.path.getsize(old_path),
        )
        old_data_file.spec_id = 0
        new_data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=new_path,
            file_format=FileFormat.PARQUET,
            partition=None,
            record_count=2,
            file_size_in_bytes=os.path.getsize(new_path),
        )
        new_data_file.spec_id = 0

        table_metadata = TableMetadataV2(
            location=str(tmp_path),
            last_column_id=3,
            format_version=2,
            current_schema_id=0,
            schemas=[current_schema],
            partition_specs=[PartitionSpec()],
            default_spec_id=0,
            last_partition_id=0,
            sort_orders=[],
            default_sort_order_id=0,
            properties={},
        )

        io = PyArrowFileIO()
        tasks = [FileScanTask(data_file=old_data_file), FileScanTask(data_file=new_data_file)]

        result = _orchestrate_to_table(table_metadata, io, current_schema, AlwaysTrue(), tasks)

        assert result.num_rows == 4
        ids = sorted(result.column("id").to_pylist())
        assert ids == [1, 2, 3, 4]

        # Category: old file rows have NULL, new file rows have values
        rows = result.to_pydict()
        id_to_cat = dict(zip(rows["id"], rows["category"], strict=False))
        assert id_to_cat[1] is None  # old file
        assert id_to_cat[2] is None  # old file
        assert id_to_cat[3] == "x"  # new file
        assert id_to_cat[4] == "y"  # new file
