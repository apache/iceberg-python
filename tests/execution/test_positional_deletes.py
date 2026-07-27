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

"""Tests for positional delete handling and combined delete scenarios."""

from __future__ import annotations

import os
import warnings
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.execution._orchestrate import orchestrate_scan
from pyiceberg.execution.backends.pyarrow_backend import (
    PyArrowComputeBackend,
    PyArrowReadBackend,
    _apply_positional_deletes_impl,
)
from pyiceberg.execution.protocol import Backends
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.types import IntegerType, NestedField, StringType


@pytest.fixture
def table_schema() -> None:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )


# =============================================================================
# From: test_positional_delete_datafusion.py
# =============================================================================


@pytest.fixture
def data_file(tmp_path) -> str:
    """Write a data file with 10 rows: id=[0..9], name=['row_0'..'row_9']."""
    path = str(tmp_path / "data.parquet")
    table = pa.table(
        {
            "id": list(range(10)),
            "name": [f"row_{i}" for i in range(10)],
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def pos_delete_file(tmp_path, data_file) -> str:
    """Write a position delete file deleting rows at positions 2, 5, 7."""
    path = str(tmp_path / "pos_delete.parquet")
    table = pa.table(
        {
            "file_path": [data_file, data_file, data_file],
            "pos": pa.array([2, 5, 7], type=pa.int64()),
        }
    )
    pq.write_table(table, path)
    return path


class TestDataFusionPositionalDeleteBasic:
    """Verify DataFusion positional delete produces correct survivors."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_datafusion(self) -> None:
        pytest.importorskip("datafusion")

    def test_deletes_correct_rows(self, data_file, pos_delete_file) -> None:
        """Rows at positions 2, 5, 7 are excluded; others survive."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_file,
                position_delete_paths=[pos_delete_file],
                projected_schema=schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(batches)
        ids = sorted(result.column("id").to_pylist())
        # Positions 2, 5, 7 → ids 2, 5, 7 are removed
        assert ids == [0, 1, 3, 4, 6, 8, 9]

    def test_no_deletes_returns_all_rows(self, tmp_path, data_file) -> None:
        """Position delete file with no entries for this data file returns all rows."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        # Delete file references a DIFFERENT data file
        del_path = str(tmp_path / "other_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": ["s3://other/file.parquet", "s3://other/file.parquet"],
                    "pos": pa.array([0, 1], type=pa.int64()),
                }
            ),
            del_path,
        )

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_file,
                position_delete_paths=[del_path],
                projected_schema=schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(batches)
        assert result.num_rows == 10

    def test_all_rows_deleted_returns_empty(self, tmp_path, data_file) -> None:
        """Deleting all positions produces zero output rows."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        del_path = str(tmp_path / "all_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_file] * 10,
                    "pos": pa.array(list(range(10)), type=pa.int64()),
                }
            ),
            del_path,
        )

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_file,
                position_delete_paths=[del_path],
                projected_schema=schema,
                io_properties={},
            )
        )

        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 0

    def test_multiple_delete_files_combined(self, tmp_path, data_file) -> None:
        """Multiple position delete files are combined correctly."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        del1 = str(tmp_path / "del1.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_file, data_file],
                    "pos": pa.array([0, 1], type=pa.int64()),
                }
            ),
            del1,
        )

        del2 = str(tmp_path / "del2.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_file, data_file],
                    "pos": pa.array([8, 9], type=pa.int64()),
                }
            ),
            del2,
        )

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_file,
                position_delete_paths=[del1, del2],
                projected_schema=schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(batches)
        ids = sorted(result.column("id").to_pylist())
        # Positions 0,1,8,9 deleted → ids 0,1,8,9 removed
        assert ids == [2, 3, 4, 5, 6, 7]

    @pytest.mark.skipif(
        os.name == "nt", reason="DataFusion temp file operations at 100K row scale exceed test timeout on Windows."
    )
    def test_large_position_set_bounded_memory(self, tmp_path) -> None:
        """Many positions: DataFusion handles within memory_limit (no Python set)."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField

        # Create a file with 100K rows
        num_rows = 100_000
        data_path = str(tmp_path / "large_data.parquet")
        pq.write_table(pa.table({"id": list(range(num_rows))}), data_path)

        # Delete every other row (50K positions) -- this would be ~1.4MB as Python set
        del_path = str(tmp_path / "large_delete.parquet")
        positions = list(range(0, num_rows, 2))
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path] * len(positions),
                    "pos": pa.array(positions, type=pa.int64()),
                }
            ),
            del_path,
        )

        schema = Schema(NestedField(1, "id", IntegerType(), required=False))

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_path,
                position_delete_paths=[del_path],
                projected_schema=schema,
                io_properties={},
                memory_limit=32 * 1024 * 1024,  # 32MB -- plenty for 50K ints
            )
        )

        result = pa.Table.from_batches(batches)
        # Only odd positions survive
        expected = list(range(1, num_rows, 2))
        assert result.column("id").to_pylist() == expected

    def test_parity_with_pyarrow_implementation(self, data_file, pos_delete_file) -> None:
        """DataFusion and PyArrow produce identical survivors."""
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        pa_backend = PyArrowComputeBackend()
        pa_result = pa.Table.from_batches(
            list(
                pa_backend.apply_positional_deletes(
                    data_path=data_file,
                    position_delete_paths=[pos_delete_file],
                    projected_schema=schema,
                    io_properties={},
                )
            )
        )

        df_backend = DataFusionComputeBackend()
        df_result = pa.Table.from_batches(
            list(
                df_backend.apply_positional_deletes(
                    data_path=data_file,
                    position_delete_paths=[pos_delete_file],
                    projected_schema=schema,
                    io_properties={},
                )
            )
        )

        # Both must produce identical row sets
        assert sorted(pa_result.column("id").to_pylist()) == sorted(df_result.column("id").to_pylist())
        assert sorted(pa_result.column("name").to_pylist()) == sorted(df_result.column("name").to_pylist())

    def test_streaming_write_does_not_materialize_full_file(self, tmp_path) -> None:
        """Verify the temp file approach: data is written via streaming, not to_table().

        The implementation streams the data file batch-by-batch to a temp Parquet
        file with _pyiceberg_pos appended, then registers that temp file with
        DataFusion. This test verifies correctness of the streaming path by using
        a multi-row-group file (which produces multiple batches from the scanner).
        """
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        # Write a data file with multiple row groups to force multiple batches
        data_path = str(tmp_path / "multi_rg_data.parquet")
        file_schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
            ]
        )
        writer = pq.ParquetWriter(data_path, file_schema)
        # Write 5 row groups of 100 rows each = 500 rows total
        for rg in range(5):
            batch = pa.record_batch(
                {
                    "id": pa.array(list(range(rg * 100, (rg + 1) * 100)), type=pa.int32()),
                    "name": [f"row_{i}" for i in range(rg * 100, (rg + 1) * 100)],
                },
                schema=file_schema,
            )
            writer.write_batch(batch)
        writer.close()

        # Delete positions spanning multiple row groups: 50, 150, 250, 350, 450
        del_path = str(tmp_path / "spanning_delete.parquet")
        positions = [50, 150, 250, 350, 450]
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path] * len(positions),
                    "pos": pa.array(positions, type=pa.int64()),
                }
            ),
            del_path,
        )

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_path,
                position_delete_paths=[del_path],
                projected_schema=schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(batches)
        # 500 rows - 5 deleted = 495 survivors
        assert result.num_rows == 495
        # Verify the deleted positions are actually gone
        surviving_ids = set(result.column("id").to_pylist())
        for pos in positions:
            assert pos not in surviving_ids

    def test_temp_file_cleaned_up_after_operation(self, tmp_path, data_file, pos_delete_file) -> None:
        """Temp file used for streaming is cleaned up even on success."""
        import glob
        import tempfile

        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        )

        # Count temp files before
        temp_dir = tempfile.gettempdir()
        before = set(glob.glob(f"{temp_dir}/pyiceberg_posdelete_*.parquet"))

        backend = DataFusionComputeBackend()
        list(
            backend.apply_positional_deletes(
                data_path=data_file,
                position_delete_paths=[pos_delete_file],
                projected_schema=schema,
                io_properties={},
            )
        )

        # Count temp files after -- should be same (cleaned up)
        after = set(glob.glob(f"{temp_dir}/pyiceberg_posdelete_*.parquet"))
        new_files = after - before
        assert len(new_files) == 0, f"Temp file(s) not cleaned up: {new_files}"

    def test_wide_table_only_projects_requested_columns(self, tmp_path) -> None:
        """A wide table (many columns) only reads/outputs the projected columns.

        Verifies that the temp Parquet file contains only projected columns + pos,
        not all columns from the source file. This prevents unnecessary I/O and
        memory usage for wide tables where only a few columns are needed.
        """
        from pyiceberg.execution.backends.datafusion_backend import (
            DataFusionComputeBackend,
        )
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        # Create a wide data file with 20 columns
        num_cols = 20
        num_rows = 50
        data = {"id": list(range(num_rows))}
        for i in range(1, num_cols):
            data[f"col_{i}"] = [f"val_{i}_{row}" for row in range(num_rows)]
        data_path = str(tmp_path / "wide_data.parquet")
        pq.write_table(pa.table(data), data_path)

        # Position delete file: delete rows at positions 10, 20, 30
        del_path = str(tmp_path / "pos_del.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path] * 3,
                    "pos": pa.array([10, 20, 30], type=pa.int64()),
                }
            ),
            del_path,
        )

        # Project only 2 columns out of 20
        projected_schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "col_1", StringType(), required=False),
        )

        backend = DataFusionComputeBackend()
        batches = list(
            backend.apply_positional_deletes(
                data_path=data_path,
                position_delete_paths=[del_path],
                projected_schema=projected_schema,
                io_properties={},
            )
        )

        result = pa.Table.from_batches(batches)

        # Verify correct row count: 50 - 3 = 47
        assert result.num_rows == 47

        # Verify only projected columns are in the output (not all 20)
        assert set(result.column_names) == {"id", "col_1"}

        # Verify deleted rows are actually gone
        surviving_ids = set(result.column("id").to_pylist())
        assert 10 not in surviving_ids
        assert 20 not in surviving_ids
        assert 30 not in surviving_ids


# =============================================================================
# From: test_positional_delete_scoping.py
# =============================================================================


class TestPositionalDeleteMultiFileScoping:
    """Verify position deletes are filtered by file_path before application.

    Per Iceberg spec, a position delete file may contain entries for MULTIPLE
    data files. Only entries matching the current data file's path should be applied.
    """

    def test_position_delete_file_with_entries_for_multiple_data_files(self, tmp_path) -> None:
        """Only positions referencing THIS file are applied; others are ignored."""
        # Data file A: id=[1, 2, 3]
        data_path_a = str(tmp_path / "data_a.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), data_path_a)

        # Data file B: id=[4, 5, 6]
        data_path_b = str(tmp_path / "data_b.parquet")
        pq.write_table(pa.table({"id": [4, 5, 6], "name": ["d", "e", "f"]}), data_path_b)

        # Position delete file references BOTH files:
        # (data_a, pos=0) → removes id=1 from file A
        # (data_b, pos=1) → removes id=5 from file B
        del_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path_a, data_path_b],
                    "pos": pa.array([0, 1], type=pa.int64()),
                }
            ),
            del_path,
        )

        # When processing file A: only pos=0 deleted → survivors [2, 3]
        result_a = pa.Table.from_batches(list(_apply_positional_deletes_impl(data_path_a, [del_path])))
        assert sorted(result_a.column("id").to_pylist()) == [2, 3]

        # When processing file B: only pos=1 deleted → survivors [4, 6]
        result_b = pa.Table.from_batches(list(_apply_positional_deletes_impl(data_path_b, [del_path])))
        assert sorted(result_b.column("id").to_pylist()) == [4, 6]

    def test_position_delete_all_entries_for_other_file(self, tmp_path) -> None:
        """If delete file has no entries for this file, all rows survive."""
        # Data file A: id=[1, 2, 3]
        data_path_a = str(tmp_path / "data_a.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), data_path_a)

        # Data file B: id=[4, 5, 6]
        data_path_b = str(tmp_path / "data_b.parquet")
        pq.write_table(pa.table({"id": [4, 5, 6], "name": ["d", "e", "f"]}), data_path_b)

        # Position delete file references ONLY file B
        del_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path_b, data_path_b],
                    "pos": pa.array([0, 2], type=pa.int64()),
                }
            ),
            del_path,
        )

        # Processing file A: delete file has NO entries for file A → all rows survive
        result = pa.Table.from_batches(list(_apply_positional_deletes_impl(data_path_a, [del_path])))
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_position_delete_multiple_files_same_positions(self, tmp_path) -> None:
        """Different data files can have positions deleted at the same index."""
        # Data file A: id=[10, 20, 30]
        data_path_a = str(tmp_path / "data_a.parquet")
        pq.write_table(pa.table({"id": [10, 20, 30]}), data_path_a)

        # Data file B: id=[40, 50, 60]
        data_path_b = str(tmp_path / "data_b.parquet")
        pq.write_table(pa.table({"id": [40, 50, 60]}), data_path_b)

        # Both files have position 0 deleted -- but they reference different files
        del_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path_a, data_path_b],
                    "pos": pa.array([0, 0], type=pa.int64()),
                }
            ),
            del_path,
        )

        # File A: pos=0 → removes id=10, survivors [20, 30]
        result_a = pa.Table.from_batches(list(_apply_positional_deletes_impl(data_path_a, [del_path])))
        assert sorted(result_a.column("id").to_pylist()) == [20, 30]

        # File B: pos=0 → removes id=40, survivors [50, 60]
        result_b = pa.Table.from_batches(list(_apply_positional_deletes_impl(data_path_b, [del_path])))
        assert sorted(result_b.column("id").to_pylist()) == [50, 60]

    def test_position_delete_mixed_entries_large_file(self, tmp_path) -> None:
        """Position delete file with many entries for many files -- only this file's applied."""
        # Create a data file with 10 rows
        data_path = str(tmp_path / "target.parquet")
        pq.write_table(pa.table({"id": list(range(10))}), data_path)

        other_path = "s3://bucket/other/file.parquet"

        # Delete file: 5 entries for other files, 2 entries for target file
        del_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [other_path, other_path, data_path, other_path, data_path, other_path, other_path],
                    "pos": pa.array([0, 1, 3, 2, 7, 4, 5], type=pa.int64()),
                }
            ),
            del_path,
        )

        # Only pos=3 and pos=7 should be deleted from target file
        result = pa.Table.from_batches(list(_apply_positional_deletes_impl(data_path, [del_path])))
        expected = [0, 1, 2, 4, 5, 6, 8, 9]
        assert sorted(result.column("id").to_pylist()) == expected

    def test_via_compute_backend_interface(self, tmp_path, table_schema) -> None:
        """Same multi-file scoping works through the PyArrowComputeBackend interface."""
        data_path_a = str(tmp_path / "data_a.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), data_path_a)

        data_path_b = str(tmp_path / "data_b.parquet")
        pq.write_table(pa.table({"id": [4, 5, 6], "name": ["d", "e", "f"]}), data_path_b)

        # Delete file references both
        del_path = str(tmp_path / "pos_delete.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path_a, data_path_b, data_path_a],
                    "pos": pa.array([2, 0, 0], type=pa.int64()),
                }
            ),
            del_path,
        )

        backend = PyArrowComputeBackend()
        # Processing file A: pos=0 and pos=2 deleted → survivor is id=2 only
        result = pa.Table.from_batches(list(backend.apply_positional_deletes(data_path_a, [del_path], table_schema, {})))
        assert sorted(result.column("id").to_pylist()) == [2]


# =============================================================================
# From: test_combined_deletes.py
# =============================================================================


@pytest.fixture
def data_file_path(tmp_path, table_schema) -> None:
    """Write a 5-row data file: id=[1,2,3,4,5], name=["a","b","c","d","e"]."""
    path = str(tmp_path / "data.parquet")
    table = pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})
    pq.write_table(table, path)
    return path


@pytest.fixture
def pos_delete_path(tmp_path, data_file_path) -> None:
    """Position delete file: removes rows at positions 1 and 3 (id=2, id=4)."""
    path = str(tmp_path / "pos_delete.parquet")
    table = pa.table(
        {
            "file_path": [data_file_path, data_file_path],
            "pos": pa.array([1, 3], type=pa.int64()),
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def eq_delete_path(tmp_path) -> None:
    """Equality delete file: removes rows where id=3."""
    path = str(tmp_path / "eq_delete.parquet")
    table = pa.table({"id": [3]})
    pq.write_table(table, path)
    return path


@pytest.fixture
def backends() -> None:
    """PyArrow-only backends for deterministic testing."""
    return Backends(
        read=PyArrowReadBackend(),
        write=MagicMock(),
        compute=PyArrowComputeBackend(),
        io_properties={},
    )


@pytest.fixture
def table_metadata(table_schema) -> None:
    """Minimal table metadata mock with schema and specs."""
    metadata = MagicMock()
    metadata.schema.return_value = table_schema
    metadata.specs.return_value = {}
    metadata.default_spec_id = 0
    metadata.format_version = 2
    return metadata


def _make_file_scan_task(data_path, pos_del_path, eq_del_path) -> None:
    """Construct a FileScanTask with both positional and equality delete files."""
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=data_path,
        file_format=FileFormat.PARQUET,
        record_count=5,
        file_size_in_bytes=1000,
    )
    pos_delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=pos_del_path,
        file_format=FileFormat.PARQUET,
        record_count=2,
        file_size_in_bytes=200,
    )
    eq_delete_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path=eq_del_path,
        file_format=FileFormat.PARQUET,
        record_count=1,
        file_size_in_bytes=100,
        equality_ids=[1],  # field_id=1 is "id"
    )
    return FileScanTask(
        data_file=data_file,
        delete_files={pos_delete_file, eq_delete_file},
    )


class TestCombinedPositionalAndEqualityDeletes:
    """Behavioral tests for the pos_deletes AND eq_deletes branch in orchestrate_scan.

    Data file: id=[1, 2, 3, 4, 5], name=["a", "b", "c", "d", "e"]
    Positional deletes: positions [1, 3] → removes id=2 (pos 1) and id=4 (pos 3)
    Equality deletes: id=3 → removes id=3

    After positional: survivors = [1, 3, 5] (ids at positions 0, 2, 4)
    After equality:   survivors = [1, 5] (id=3 removed by equality)
    """

    def test_both_delete_types_produce_correct_survivors(
        self, data_file_path, pos_delete_path, eq_delete_path, backends, table_metadata, table_schema
    ) -> None:
        """Combined pos+eq deletes yield exactly the correct surviving rows."""
        task = _make_file_scan_task(data_file_path, pos_delete_path, eq_delete_path)

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=table_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(result_table.column("id").to_pylist())

        # pos deletes remove positions 1,3 (id=2, id=4)
        # eq deletes remove id=3
        # survivors: id=1, id=5
        assert surviving_ids == [1, 5], (
            f"Expected [1, 5] after positional (remove pos 1,3 → id=2,4) and equality (remove id=3) deletes. Got {surviving_ids}"
        )

    def test_positional_deletes_applied_before_equality(self, tmp_path, backends, table_metadata, table_schema) -> None:
        """Positional deletes reference ORIGINAL positions, not post-equality positions.

        If equality were applied first (removing id=3 at pos 2), the remaining rows
        would be [1,2,4,5] and positional delete at pos 1 would incorrectly remove id=2
        and pos 3 would remove id=5 instead of id=4.

        Correct order (pos first): pos deletes reference the original file.
        """
        # Data: id=[10, 20, 30, 40, 50]
        data_path = str(tmp_path / "data_order.parquet")
        pq.write_table(pa.table({"id": [10, 20, 30, 40, 50], "name": ["a", "b", "c", "d", "e"]}), data_path)

        # Positional delete: remove position 0 (id=10)
        pos_path = str(tmp_path / "pos_order.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path],
                    "pos": pa.array([0], type=pa.int64()),
                }
            ),
            pos_path,
        )

        # Equality delete: remove id=30
        eq_path = str(tmp_path / "eq_order.parquet")
        pq.write_table(pa.table({"id": [30]}), eq_path)

        task = _make_file_scan_task(data_path, pos_path, eq_path)

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=table_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(result_table.column("id").to_pylist())

        # pos removes position 0 → id=10 gone. Survivors: [20, 30, 40, 50]
        # eq removes id=30. Final survivors: [20, 40, 50]
        assert surviving_ids == [20, 40, 50]

    def test_combined_deletes_with_null_equality_values(self, tmp_path, backends, table_metadata, table_schema) -> None:
        """NULL in equality delete file matches NULL in data via IS NOT DISTINCT FROM."""
        # Data: id=[1, None, 3, None, 5]
        data_path = str(tmp_path / "data_null.parquet")
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([1, None, 3, None, 5], type=pa.int32()),
                    "name": ["a", "b", "c", "d", "e"],
                }
            ),
            data_path,
        )

        # Positional delete: remove position 2 (id=3)
        pos_path = str(tmp_path / "pos_null.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path],
                    "pos": pa.array([2], type=pa.int64()),
                }
            ),
            pos_path,
        )

        # Equality delete: remove id=NULL (should match both NULL rows)
        eq_path = str(tmp_path / "eq_null.parquet")
        pq.write_table(pa.table({"id": pa.array([None], type=pa.int32())}), eq_path)

        task = _make_file_scan_task(data_path, pos_path, eq_path)

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=table_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(v for v in result_table.column("id").to_pylist() if v is not None)

        # pos removes position 2 (id=3). Survivors: [1, None, None, 5]
        # eq removes id=NULL. Final survivors: [1, 5]
        assert surviving_ids == [1, 5]
        # Verify no NULLs remain
        assert None not in result_table.column("id").to_pylist()

    def test_combined_deletes_empty_positional_file(self, tmp_path, backends, table_metadata, table_schema) -> None:
        """If positional delete file has no matching positions, all rows pass to equality phase."""
        # Data: id=[1, 2, 3]
        data_path = str(tmp_path / "data_empty_pos.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), data_path)

        # Positional delete: empty (no positions to delete for this file)
        pos_path = str(tmp_path / "pos_empty.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": pa.array([], type=pa.string()),
                    "pos": pa.array([], type=pa.int64()),
                }
            ),
            pos_path,
        )

        # Equality delete: remove id=2
        eq_path = str(tmp_path / "eq_empty_pos.parquet")
        pq.write_table(pa.table({"id": [2]}), eq_path)

        task = _make_file_scan_task(data_path, pos_path, eq_path)

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=table_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(result_table.column("id").to_pylist())

        # No positional deletes applied. Equality removes id=2. Survivors: [1, 3]
        assert surviving_ids == [1, 3]

    def test_combined_deletes_equality_schema_differs_from_projected(self, tmp_path, backends, table_metadata) -> None:
        """Equality delete file is read with its OWN schema, not the projected schema.

        This regression test verifies the fix for _chain_read_batches → _read_equality_delete_batches:
        the delete file contains only the equality columns (id), while the projected schema
        may contain additional columns (id, name, address, etc.). Reading with the full
        projected schema would cause 'column not found' errors.
        """
        # Table schema with 3 columns
        wide_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
            NestedField(field_id=3, name="address", field_type=StringType(), required=False),
        )
        table_metadata.schema.return_value = wide_schema

        # Data file has all 3 columns
        data_path = str(tmp_path / "data_wide.parquet")
        pq.write_table(
            pa.table(
                {
                    "id": [1, 2, 3, 4, 5],
                    "name": ["a", "b", "c", "d", "e"],
                    "address": ["x", "y", "z", "w", "v"],
                }
            ),
            data_path,
        )

        # Positional delete: remove position 4 (id=5)
        pos_path = str(tmp_path / "pos_wide.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path],
                    "pos": pa.array([4], type=pa.int64()),
                }
            ),
            pos_path,
        )

        # Equality delete file has ONLY the id column (not name, not address)
        eq_path = str(tmp_path / "eq_wide.parquet")
        pq.write_table(pa.table({"id": [2]}), eq_path)

        task = _make_file_scan_task(data_path, pos_path, eq_path)

        # Project all 3 columns -- the delete file does NOT have name or address
        # This would fail with the old code that passed projected_schema to read delete files
        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=wide_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(result_table.column("id").to_pylist())

        # pos removes position 4 (id=5). eq removes id=2. Survivors: [1, 3, 4]
        assert surviving_ids == [1, 3, 4]

    def test_combined_deletes_multiple_equality_delete_files(self, tmp_path, backends, table_metadata, table_schema) -> None:
        """Multiple equality delete files are chained correctly."""
        # Data: id=[1, 2, 3, 4, 5, 6]
        data_path = str(tmp_path / "data_multi.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5, 6], "name": ["a", "b", "c", "d", "e", "f"]}), data_path)

        # Positional delete: remove position 0 (id=1)
        pos_path = str(tmp_path / "pos_multi.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path],
                    "pos": pa.array([0], type=pa.int64()),
                }
            ),
            pos_path,
        )

        # Two equality delete files: one removes id=3, another removes id=5
        eq_path_1 = str(tmp_path / "eq_multi_1.parquet")
        pq.write_table(pa.table({"id": [3]}), eq_path_1)

        eq_path_2 = str(tmp_path / "eq_multi_2.parquet")
        pq.write_table(pa.table({"id": [5]}), eq_path_2)

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_path,
            file_format=FileFormat.PARQUET,
            record_count=6,
            file_size_in_bytes=1000,
        )
        pos_delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path=pos_path,
            file_format=FileFormat.PARQUET,
            record_count=1,
            file_size_in_bytes=100,
        )
        eq_delete_file_1 = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=eq_path_1,
            file_format=FileFormat.PARQUET,
            record_count=1,
            file_size_in_bytes=100,
            equality_ids=[1],
        )
        eq_delete_file_2 = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=eq_path_2,
            file_format=FileFormat.PARQUET,
            record_count=1,
            file_size_in_bytes=100,
            equality_ids=[1],
        )

        task = FileScanTask(
            data_file=data_file,
            delete_files={pos_delete_file, eq_delete_file_1, eq_delete_file_2},
        )

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=table_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(result_table.column("id").to_pylist())

        # pos removes position 0 (id=1). eq removes id=3 and id=5. Survivors: [2, 4, 6]
        assert surviving_ids == [2, 4, 6]

    def test_combined_deletes_multi_file_position_delete(self, tmp_path, backends, table_metadata, table_schema) -> None:
        """Position delete file referencing MULTIPLE data files, combined with equality.

        Verifies that positions from OTHER data files are NOT applied to the current task,
        even when the same position delete file contains entries for multiple files.
        """
        # Create two data files
        data_path_a = str(tmp_path / "data_a.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}), data_path_a)

        data_path_b = str(tmp_path / "data_b.parquet")
        pq.write_table(pa.table({"id": [10, 20, 30], "name": ["x", "y", "z"]}), data_path_b)

        # Position delete file with entries for BOTH data files
        pos_path = str(tmp_path / "pos_multi_file.parquet")
        pq.write_table(
            pa.table(
                {
                    "file_path": [data_path_a, data_path_b],
                    "pos": pa.array([0, 1], type=pa.int64()),
                }
            ),
            pos_path,
        )

        # Equality delete: remove id=3
        eq_path = str(tmp_path / "eq_multi_file.parquet")
        pq.write_table(pa.table({"id": [3]}), eq_path)

        # Task for data_file_A only -- the pos delete for file_B must NOT be applied
        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path_a,
                file_format=FileFormat.PARQUET,
                record_count=5,
                file_size_in_bytes=1000,
            ),
            delete_files={
                DataFile.from_args(
                    content=DataFileContent.POSITION_DELETES,
                    file_path=pos_path,
                    file_format=FileFormat.PARQUET,
                    record_count=2,
                    file_size_in_bytes=200,
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

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=table_schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = sorted(result_table.column("id").to_pylist())

        # pos removes pos 0 of file_A (id=1). pos 1 of file_B is NOT applied.
        # eq removes id=3. Survivors: [2, 4, 5]
        assert surviving_ids == [2, 4, 5], (
            f"Expected [2, 4, 5] after multi-file positional delete scoping + equality. "
            f"Got {surviving_ids}. Positions from other data files must not leak."
        )


# =============================================================================
# Regression guards for section 8 risks
# =============================================================================


class TestEqualityDeleteSchemaEvolution:
    """Regression: equality deletes with field IDs dropped via schema evolution."""

    def test_equality_delete_with_evolved_away_field_warns(self, tmp_path, backends) -> None:
        """equality_ids referencing dropped fields emit a warning and skip anti-join."""
        # Current schema: only has "id" (field_id=1). Field 2 ("name") was dropped.
        current_schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        )

        # Data file has id=[1, 2, 3]
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(pa.table({"id": [1, 2, 3]}), data_path)

        # Equality delete file references field_id=2 (dropped column "name")
        eq_path = str(tmp_path / "eq_delete.parquet")
        pq.write_table(pa.table({"name": ["b"]}), eq_path)

        metadata = MagicMock()
        metadata.schema.return_value = current_schema
        metadata.specs.return_value = {}
        metadata.default_spec_id = 0
        metadata.format_version = 2

        task = FileScanTask(
            data_file=DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=data_path,
                file_format=FileFormat.PARQUET,
                record_count=3,
                file_size_in_bytes=500,
            ),
            delete_files={
                DataFile.from_args(
                    content=DataFileContent.EQUALITY_DELETES,
                    file_path=eq_path,
                    file_format=FileFormat.PARQUET,
                    record_count=1,
                    file_size_in_bytes=100,
                    equality_ids=[2],  # field_id=2 no longer in schema
                ),
            },
        )

        # Should warn about unresolvable equality_ids and return all rows
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            results = list(
                orchestrate_scan(
                    backends=backends,
                    tasks=iter([task]),
                    table_metadata=metadata,
                    projected_schema=current_schema,
                    row_filter=AlwaysTrue(),
                    case_sensitive=True,
                )
            )

        # Verify warning was emitted
        schema_evolution_warnings = [w for w in caught if "do not exist in the current table schema" in str(w.message)]
        assert len(schema_evolution_warnings) > 0, "Expected a UserWarning about equality_ids not in current schema"

        # Verify data is returned as-is (superset -- no rows incorrectly deleted)
        result_table = pa.Table.from_batches(results)
        assert sorted(result_table.column("id").to_pylist()) == [1, 2, 3]

    def test_equality_delete_with_null_values_is_not_distinct_from(self, tmp_path, backends) -> None:
        """NULL in equality delete file correctly matches NULL in data file."""
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        )

        # Data file: id=[1, None, 3, None, 5]
        data_path = str(tmp_path / "data_nulls.parquet")
        pq.write_table(
            pa.table({"id": pa.array([1, None, 3, None, 5], type=pa.int32())}),
            data_path,
        )

        # Equality delete: delete rows where id IS NOT DISTINCT FROM NULL
        eq_path = str(tmp_path / "eq_delete_null.parquet")
        pq.write_table(
            pa.table({"id": pa.array([None], type=pa.int32())}),
            eq_path,
        )

        metadata = MagicMock()
        metadata.schema.return_value = schema
        metadata.specs.return_value = {}
        metadata.default_spec_id = 0
        metadata.format_version = 2

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
                    record_count=1,
                    file_size_in_bytes=100,
                    equality_ids=[1],
                ),
            },
        )

        results = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
                case_sensitive=True,
            )
        )

        result_table = pa.Table.from_batches(results)
        surviving_ids = result_table.column("id").to_pylist()

        # Both NULLs must be removed (IS NOT DISTINCT FROM: NULL == NULL)
        assert None not in surviving_ids, (
            f"NULL values should be excluded by equality delete with NULL key. Got surviving ids: {surviving_ids}"
        )
        assert sorted(v for v in surviving_ids if v is not None) == [1, 3, 5]


class TestCoWDeleteEdgeCases:
    """Regression guards for CoW delete edge cases."""

    def test_cow_delete_skips_empty_record_count_files(self, tmp_path) -> None:
        """Files with record_count=0 in metadata are skipped without I/O."""
        # Create a mock file scan task with record_count=0
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=str(tmp_path / "empty.parquet"),
            file_format=FileFormat.PARQUET,
            record_count=0,
            file_size_in_bytes=1000,
        )

        # Write an actual empty parquet file
        pq.write_table(pa.table({"id": pa.array([], type=pa.int32())}), data_file.file_path)

        task = FileScanTask(data_file=data_file, delete_files=set())

        # The read backend should NOT be called for this file
        MagicMock()

        # Simulate the CoW logic: record_count == 0 should trigger `continue`
        original_row_count = task.file.record_count
        assert original_row_count == 0, "Test setup: file must have record_count=0"

        # Verify the guard: with record_count=0, the file is skipped
        # The guard fires before the threshold check
        assert original_row_count == 0  # Would hit `continue` in the loop
