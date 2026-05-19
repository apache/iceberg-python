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

"""Parametrized format writer tests, modeled after Java's BaseFormatModelTests."""

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from pyiceberg.io.fileformat import FileFormatFactory, FileFormatModel
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


@pytest.fixture(params=FileFormatFactory.available_formats(), ids=lambda f: f.name.lower())
def format_model(request: pytest.FixtureRequest) -> FileFormatModel:
    return FileFormatFactory.get(request.param)


@pytest.fixture
def simple_table() -> pa.Table:
    return pa.table(
        {
            "foo": ["a", "b", "c"],
            "bar": pa.array([1, 2, 3], type=pa.int32()),
            "baz": [True, False, True],
        }
    )


def test_parquet_registered() -> None:
    """ParquetFormatModel is registered in the factory."""
    model = FileFormatFactory.get(FileFormat.PARQUET)
    assert model.format == FileFormat.PARQUET
    assert model.file_extension() == "parquet"


def test_round_trip(format_model: FileFormatModel, table_schema_simple: Schema, simple_table: pa.Table, tmp_path: Path) -> None:
    """Write a table and read it back, to verify equality and record count."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    writer.write(simple_table)
    statistics = writer.close()

    result = ds.dataset(file_path).to_table()
    assert result.equals(simple_table)
    assert statistics.record_count == 3


def test_statistics_record_count(format_model: FileFormatModel, table_schema_simple: Schema, tmp_path: Path) -> None:
    """close() returns DataFileStatistics with correct record count."""
    table = pa.table(
        {
            "foo": ["a", "b", "c", "d", "e"],
            "bar": pa.array([10, 20, 30, 40, 50], type=pa.int32()),
            "baz": [True] * 5,
        }
    )
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    writer.write(table)
    assert writer.close().record_count == 5


def test_null_handling(format_model: FileFormatModel, table_schema_simple: Schema, tmp_path: Path) -> None:
    """Nullable columns produce correct null_value_counts in statistics."""
    table = pa.table(
        {
            "foo": ["a", None, "c"],  # field_id=1, optional
            "bar": pa.array([1, 2, 3], type=pa.int32()),  # field_id=2, required
            "baz": [True, False, True],  # field_id=3, optional
        }
    )
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    writer.write(table)
    stats = writer.close()
    assert stats.record_count == 3
    assert stats.null_value_counts.get(1) == 1


def test_context_manager_caches_result(
    format_model: FileFormatModel, table_schema_simple: Schema, simple_table: pa.Table, tmp_path: Path
) -> None:
    """writer.result() returns cached statistics after context manager exit."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    with writer:
        writer.write(simple_table)
    assert writer.result().record_count == 3


def test_close_is_idempotent(
    format_model: FileFormatModel, table_schema_simple: Schema, simple_table: pa.Table, tmp_path: Path
) -> None:
    """Calling close() twice returns the same cached statistics object."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    writer.write(simple_table)
    stats1 = writer.close()
    stats2 = writer.close()
    assert stats1 is stats2


def test_close_without_write_raises(format_model: FileFormatModel, table_schema_simple: Schema, tmp_path: Path) -> None:
    """Closing a writer that was never written to raises ValueError."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    with pytest.raises(ValueError, match="Cannot close a writer that was never written to"):
        writer.close()


def test_construct_field_uses_orc_field_id_key() -> None:
    """ArrowProjectionVisitor uses ORC field ID and required keys when file_format is ORC."""
    from pyiceberg.io.pyarrow import (
        ORC_FIELD_ID_KEY,
        ORC_FIELD_REQUIRED_KEY,
        PYARROW_PARQUET_FIELD_ID_KEY,
        ArrowProjectionVisitor,
    )

    schema = Schema(NestedField(field_id=1, name="x", field_type=LongType(), required=True))

    visitor = ArrowProjectionVisitor(schema, include_field_ids=True, file_format=FileFormat.ORC)
    field = visitor._construct_field(schema.find_field(1), pa.int64())
    assert field.metadata is not None
    assert ORC_FIELD_ID_KEY in field.metadata
    assert ORC_FIELD_REQUIRED_KEY in field.metadata
    assert field.metadata[ORC_FIELD_REQUIRED_KEY] == b"true"
    assert PYARROW_PARQUET_FIELD_ID_KEY not in field.metadata

    visitor_pq = ArrowProjectionVisitor(schema, include_field_ids=True, file_format=FileFormat.PARQUET)
    field_pq = visitor_pq._construct_field(schema.find_field(1), pa.int64())
    assert field_pq.metadata is not None
    assert PYARROW_PARQUET_FIELD_ID_KEY in field_pq.metadata
    assert ORC_FIELD_ID_KEY not in field_pq.metadata
    assert ORC_FIELD_REQUIRED_KEY not in field_pq.metadata
