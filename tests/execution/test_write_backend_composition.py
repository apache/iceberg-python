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

"""TDD tests for WriteBackend composition with FileFormatModel (#3381).

The WriteBackend protocol composes with upstream's FileFormatModel/FileFormatWriter
abstraction. WriteBackend controls HOW to execute the write (which engine), while
FileFormatModel controls WHAT format to write (Parquet, ORC, etc.).

Composition: WriteBackend.write_data_file(output_file, ..., format_model)
                └── format_model.create_writer(output_file, ...) → FileFormatWriter
                        └── writer.write(table) → statistics
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from pyiceberg.io.fileformat import DataFileStatistics, FileFormatFactory
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType


def _make_output_file(path: Path):
    """Create a minimal OutputFile for local testing on Windows and Unix.

    Uses PyArrowFileIO with a scheme-less path for Windows compatibility.
    Falls back to a file:// URI on non-Windows platforms.
    """
    import sys

    from pyiceberg.io.pyarrow import PyArrowFileIO

    io = PyArrowFileIO()
    if sys.platform == "win32":
        # Windows: use file scheme with the Path.as_uri() output which
        # produces the correct file:///C:/... format that PyArrow handles.
        # However, PyArrow's LocalFileSystem has issues with create_dir on Windows
        # when the path starts with /C:/. Use FsspecFileIO or direct write instead.
        # Workaround: patch in a local filesystem OutputFile that avoids the bug.
        return _LocalOutputFile(path)
    else:
        return io.new_output(str(path))


class _LocalOutputFile:
    """Minimal OutputFile implementation for local paths (test helper).

    Avoids PyArrow's LocalFileSystem.create_dir() bug on Windows where
    file:// URIs produce /C:/... paths that fail the Windows path check.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    def create(self, overwrite: bool = False) -> Any:
        import io as _io

        self._path.parent.mkdir(parents=True, exist_ok=True)
        mode = "wb" if overwrite else "xb"
        return _io.FileIO(str(self._path), mode=mode)

    def __len__(self) -> int:
        return self._path.stat().st_size if self._path.exists() else 0

    @property
    def location(self) -> str:
        return str(self._path)


class TestWriteBackendComposesWithFormatModel:
    """WriteBackend.write_data_file delegates to FileFormatModel.create_writer."""

    def test_pyarrow_write_backend_delegates_to_format_model(self, tmp_path):
        """PyArrowWriteBackend.write_data_file creates a writer from the format model."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )

        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "name": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        output_file = _make_output_file(tmp_path / "test_output.parquet")
        format_model = FileFormatFactory.get(FileFormat.PARQUET)
        backend = PyArrowWriteBackend()

        stats = backend.write_data_file(
            output_file=output_file,
            file_schema=schema,
            properties={},
            arrow_table=table,
            format_model=format_model,
        )

        assert isinstance(stats, DataFileStatistics)
        assert stats.record_count == 3

    def test_write_data_file_returns_correct_statistics(self, tmp_path):
        """Statistics reflect null counts and record count accurately."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )

        table = pa.table(
            {
                "id": pa.array([10, 20, 30, 40, 50], type=pa.int32()),
                "name": pa.array(["a", None, "c", None, "e"], type=pa.string()),
            }
        )
        output_file = _make_output_file(tmp_path / "stats_test.parquet")
        format_model = FileFormatFactory.get(FileFormat.PARQUET)
        backend = PyArrowWriteBackend()

        stats = backend.write_data_file(
            output_file=output_file,
            file_schema=schema,
            properties={},
            arrow_table=table,
            format_model=format_model,
        )

        assert stats.record_count == 5
        # null_value_counts should reflect the 2 nulls in "name" column
        assert any(v == 2 for v in stats.null_value_counts.values())
        assert isinstance(stats.split_offsets, list)

    def test_write_data_file_with_empty_table(self, tmp_path):
        """Writing an empty table raises ValueError (cannot close writer without data)."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))

        table = pa.table({"id": pa.array([], type=pa.int32())})
        output_file = _make_output_file(tmp_path / "empty_test.parquet")
        format_model = FileFormatFactory.get(FileFormat.PARQUET)
        backend = PyArrowWriteBackend()

        with pytest.raises(ValueError, match="Cannot close a writer that was never written to"):
            backend.write_data_file(
                output_file=output_file,
                file_schema=schema,
                properties={},
                arrow_table=table,
                format_model=format_model,
            )

    def test_write_backend_satisfies_protocol(self):
        """PyArrowWriteBackend satisfies the WriteBackend protocol."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        from pyiceberg.execution.protocol import WriteBackend

        backend = PyArrowWriteBackend()
        assert isinstance(backend, WriteBackend)

    def test_write_data_file_produces_readable_parquet(self, tmp_path):
        """Written file can be read back with identical data."""
        import pyarrow.parquet as pq

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "value", StringType(), required=False),
        )

        original = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "value": pa.array(["x", "y", "z"], type=pa.string()),
            }
        )
        output_path = tmp_path / "roundtrip.parquet"
        output_file = _make_output_file(output_path)
        format_model = FileFormatFactory.get(FileFormat.PARQUET)
        backend = PyArrowWriteBackend()

        backend.write_data_file(
            output_file=output_file,
            file_schema=schema,
            properties={},
            arrow_table=original,
            format_model=format_model,
        )

        read_back = pq.read_table(str(output_path))
        assert read_back.num_rows == 3
        assert read_back.column("id").to_pylist() == [1, 2, 3]
        assert read_back.column("value").to_pylist() == ["x", "y", "z"]

    def test_empty_table_raises_on_close(self, tmp_path):
        """write_data_file with 0-row table raises ValueError from format writer.

        This documents current behavior: the PyArrowWriteBackend guards
        writer.write() with `if num_rows > 0`, so an empty table results in
        no data written. The ParquetFormatWriter then raises on close() because
        it was never written to. In practice, empty tables never reach
        write_data_file (callers check row count before dispatching).
        """
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "value", StringType(), required=False),
        )
        empty_table = pa.table({"id": pa.array([], type=pa.int32()), "value": pa.array([], type=pa.string())})

        output_path = tmp_path / "empty.parquet"
        output_file = _make_output_file(output_path)
        format_model = FileFormatFactory.get(FileFormat.PARQUET)
        backend = PyArrowWriteBackend()

        with pytest.raises(ValueError, match="never written to"):
            backend.write_data_file(
                output_file=output_file,
                file_schema=schema,
                properties={},
                arrow_table=empty_table,
                format_model=format_model,
            )


class TestWriteFileUsesWriteBackend:
    """write_file() dispatches through WriteBackend when provided."""

    @pytest.mark.skipif(
        __import__("sys").platform == "win32", reason="PyArrowFileIO does not support bare Windows paths for write operations"
    )
    def test_write_file_accepts_write_backend_parameter(self, tmp_path):
        """write_file() composes WriteBackend with its internal format model."""
        import uuid

        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        from pyiceberg.io.pyarrow import PyArrowFileIO, write_file
        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
        from pyiceberg.table import WriteTask
        from pyiceberg.table.metadata import new_table_metadata
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        table_metadata = new_table_metadata(
            schema=schema,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            location=tmp_path.as_uri(),
            properties={},
        )

        io = PyArrowFileIO()
        write_uuid = uuid.uuid4()
        batches = [pa.record_batch({"id": [1, 2, 3]}, schema=pa.schema([pa.field("id", pa.int32())]))]
        task = WriteTask(write_uuid=write_uuid, task_id=0, record_batches=batches, schema=schema)

        backend = PyArrowWriteBackend()
        data_files = list(
            write_file(
                io=io,
                table_metadata=table_metadata,
                tasks=iter([task]),
                write_backend=backend,
            )
        )

        assert len(data_files) == 1
        assert data_files[0].record_count == 3

    @pytest.mark.skipif(
        __import__("sys").platform == "win32", reason="PyArrowFileIO does not support bare Windows paths for write operations"
    )
    def test_write_file_without_backend_is_backward_compatible(self, tmp_path):
        """write_file() without write_backend still works (no regression)."""
        import uuid

        from pyiceberg.io.pyarrow import PyArrowFileIO, write_file
        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
        from pyiceberg.table import WriteTask
        from pyiceberg.table.metadata import new_table_metadata
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        table_metadata = new_table_metadata(
            schema=schema,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            location=tmp_path.as_uri(),
            properties={},
        )

        io = PyArrowFileIO()
        write_uuid = uuid.uuid4()
        batches = [pa.record_batch({"id": [10, 20]}, schema=pa.schema([pa.field("id", pa.int32())]))]
        task = WriteTask(write_uuid=write_uuid, task_id=0, record_batches=batches, schema=schema)

        # No write_backend — uses default (format model directly)
        data_files = list(
            write_file(
                io=io,
                table_metadata=table_metadata,
                tasks=iter([task]),
            )
        )

        assert len(data_files) == 1
        assert data_files[0].record_count == 2


class TestDataframeToDataFilesComposition:
    """_dataframe_to_data_files passes write_backend through to write_file."""

    @pytest.mark.skipif(
        __import__("sys").platform == "win32", reason="PyArrowFileIO does not support bare Windows paths for write operations"
    )
    def test_dataframe_to_data_files_with_write_backend(self, tmp_path):
        """write_backend flows from _dataframe_to_data_files → write_file."""
        from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend
        from pyiceberg.io.pyarrow import PyArrowFileIO, _dataframe_to_data_files
        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
        from pyiceberg.table.metadata import new_table_metadata
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        table_metadata = new_table_metadata(
            schema=schema,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            location=tmp_path.as_uri(),
            properties={},
        )

        io = PyArrowFileIO()
        table = pa.table({"id": pa.array([1, 2, 3, 4, 5], type=pa.int32())})

        backend = PyArrowWriteBackend()
        data_files = list(
            _dataframe_to_data_files(
                table_metadata=table_metadata,
                df=table,
                io=io,
                write_backend=backend,
            )
        )

        assert len(data_files) >= 1
        total_rows = sum(df.record_count for df in data_files)
        assert total_rows == 5
