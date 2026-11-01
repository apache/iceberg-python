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
from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, FloatType, ListType, LongType, MapType, NestedField, StringType, StructType


@pytest.fixture(params=FileFormatFactory.available_formats(), ids=lambda f: f.name.lower())
def format_model(request: pytest.FixtureRequest) -> FileFormatModel:
    return FileFormatFactory.get(request.param)


def test_parquet_registered() -> None:
    """ParquetFormatModel is registered in the factory."""
    model = FileFormatFactory.get(FileFormat.PARQUET)
    assert model.format == FileFormat.PARQUET
    assert model.file_extension() == "parquet"


def test_round_trip(
    format_model: FileFormatModel, table_schema_simple: Schema, arrow_table_simple: pa.Table, tmp_path: Path
) -> None:
    """Write a table and read it back, to verify equality and record count."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    writer.write(arrow_table_simple)
    statistics = writer.close()

    result = ds.dataset(file_path).to_table()
    assert result.equals(arrow_table_simple)
    assert statistics.record_count == 3


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


def test_nan_value_counts(format_model: FileFormatModel, tmp_path: Path) -> None:
    """Float and double columns produce nan_value_counts, including nested and empty counts."""
    nan = float("nan")
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "f", FloatType(), required=False),
        NestedField(3, "d", DoubleType(), required=False),
        NestedField(4, "no_nans", DoubleType(), required=False),
        NestedField(5, "s", StructType(NestedField(6, "x", DoubleType(), required=False)), required=False),
        NestedField(7, "l", ListType(element_id=8, element_type=DoubleType(), element_required=False), required=False),
        NestedField(
            9,
            "m",
            MapType(key_id=10, key_type=StringType(), value_id=11, value_type=DoubleType(), value_required=False),
            required=False,
        ),
    )
    table = pa.Table.from_pylist(
        [
            {"id": 1, "f": nan, "d": nan, "no_nans": 1.0, "s": {"x": nan}, "l": [nan, 1.0], "m": {"a": nan}},
            {"id": 2, "f": 1.0, "d": nan, "no_nans": None, "s": None, "l": None, "m": None},
            {"id": 3, "f": None, "d": 2.0, "no_nans": 2.0, "s": {"x": 1.0}, "l": [nan, nan], "m": {"b": 1.0, "c": nan}},
        ],
        schema=schema_to_pyarrow(schema),
    )
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), schema, {})
    writer.write(table)
    writer.write(table.slice(0, 1))
    stats = writer.close()

    assert stats.nan_value_counts == {2: 2, 3: 3, 4: 0, 6: 2, 8: 4, 11: 3}


def test_nan_value_counts_respect_metrics_mode(format_model: FileFormatModel, tmp_path: Path) -> None:
    """Columns with metrics mode none do not get a NaN count."""
    schema = Schema(
        NestedField(1, "a", DoubleType(), required=False),
        NestedField(2, "b", DoubleType(), required=False),
    )
    table = pa.Table.from_pydict({"a": [float("nan")], "b": [float("nan")]}, schema=schema_to_pyarrow(schema))
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(
        PyArrowFileIO().new_output(file_path), schema, {"write.metadata.metrics.column.b": "none"}
    )
    writer.write(table)
    stats = writer.close()

    assert stats.nan_value_counts == {1: 1}


def test_context_manager_caches_result(
    format_model: FileFormatModel, table_schema_simple: Schema, arrow_table_simple: pa.Table, tmp_path: Path
) -> None:
    """writer.result() returns cached statistics after context manager exit."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    with writer:
        writer.write(arrow_table_simple)
    assert writer.result().record_count == 3


def test_close_is_idempotent(
    format_model: FileFormatModel, table_schema_simple: Schema, arrow_table_simple: pa.Table, tmp_path: Path
) -> None:
    """Calling close() twice returns the same cached statistics object."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    writer.write(arrow_table_simple)
    stats1 = writer.close()
    stats2 = writer.close()
    assert stats1 is stats2


def test_close_without_write_raises(format_model: FileFormatModel, table_schema_simple: Schema, tmp_path: Path) -> None:
    """Closing a writer that was never written to raises ValueError."""
    file_path = str(tmp_path / f"test.{format_model.file_extension()}")
    writer = format_model.create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})
    with pytest.raises(ValueError, match="Cannot close a writer that was never written to"):
        writer.close()


def test_parquet_writer_closes_output_stream_on_construction_failure(
    table_schema_simple: Schema,
    arrow_table_simple: pa.Table,
    tmp_path: Path,
) -> None:
    """ParquetWriter construction failure inside write() closes the opened output stream."""
    from unittest.mock import patch

    from pyiceberg.io.pyarrow import ParquetFormatModel

    file_path = str(tmp_path / "test.parquet")
    writer = ParquetFormatModel().create_writer(PyArrowFileIO().new_output(file_path), table_schema_simple, {})

    with patch("pyiceberg.io.pyarrow.pq.ParquetWriter", side_effect=RuntimeError("simulated failure")):
        with pytest.raises(RuntimeError, match="simulated failure"):
            writer.write(arrow_table_simple)

    assert writer._writer is None
    assert writer._fos is None


def test_parquet_format_model_adds_field_id_metadata() -> None:
    """ParquetFormatModel.add_field_metadata writes the Parquet field-id key when requested."""
    from pyiceberg.io.pyarrow import PYARROW_PARQUET_FIELD_ID_KEY, ParquetFormatModel

    field = NestedField(field_id=1, name="x", field_type=LongType(), required=True)

    metadata: dict[bytes, bytes] = {}
    ParquetFormatModel().add_field_metadata(field, metadata, include_field_ids=True)
    assert metadata == {PYARROW_PARQUET_FIELD_ID_KEY: b"1"}

    metadata_no_ids: dict[bytes, bytes] = {}
    ParquetFormatModel().add_field_metadata(field, metadata_no_ids, include_field_ids=False)
    assert metadata_no_ids == {}
