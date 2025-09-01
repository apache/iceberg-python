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
# pylint: disable=protected-access,unused-argument,redefined-outer-name
import logging
import os
import tempfile
import uuid
import warnings
from datetime import date, datetime, timezone
from typing import Any, List, Optional
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging import version
from pyarrow.fs import AwsDefaultS3RetryStrategy, FileType, LocalFileSystem, S3FileSystem

from pyiceberg.exceptions import ResolveError
from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundNotStartsWith,
    BoundReference,
    BoundStartsWith,
    GreaterThan,
    Not,
    Or,
)
from pyiceberg.expressions.literals import literal
from pyiceberg.io import S3_RETRY_STRATEGY_IMPL, InputStream, OutputStream, load_file_io
from pyiceberg.io.pyarrow import (
    ICEBERG_SCHEMA,
    PYARROW_PARQUET_FIELD_ID_KEY,
    ArrowScan,
    PyArrowFile,
    PyArrowFileIO,
    StatsAggregator,
    _check_pyarrow_schema_compatible,
    _ConvertToArrowSchema,
    _determine_partitions,
    _primitive_to_physical,
    _read_deletes,
    _task_to_record_batches,
    _to_requested_schema,
    bin_pack_arrow_table,
    compute_statistics_plan,
    data_file_statistics_from_parquet_metadata,
    expression_to_pyarrow,
    parquet_path_to_id_mapping,
    schema_to_pyarrow,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, make_compatible_name, visit
from pyiceberg.table import FileScanTask, TableProperties
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.name_mapping import create_mapping_from_schema
from pyiceberg.transforms import HourTransform, IdentityTransform
from pyiceberg.typedef import UTF8, Properties, Record, TableVersion
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampNanoType,
    TimestampType,
    TimestamptzType,
    TimeType,
)
from tests.catalog.test_base import InMemoryCatalog
from tests.conftest import UNIFIED_AWS_SESSION_PROPERTIES

skip_if_pyarrow_too_old = pytest.mark.skipif(
    version.parse(pyarrow.__version__) < version.parse("20.0.0"),
    reason="Requires pyarrow version >= 20.0.0",
)


def test_pyarrow_infer_local_fs_from_path() -> None:
    """Test path with `file` scheme and no scheme both use LocalFileSystem"""
    assert isinstance(PyArrowFileIO().new_output("file://tmp/warehouse")._filesystem, LocalFileSystem)
    assert isinstance(PyArrowFileIO().new_output("/tmp/warehouse")._filesystem, LocalFileSystem)


def test_pyarrow_local_fs_can_create_path_without_parent_dir() -> None:
    """Test LocalFileSystem can create path without first creating the parent directories"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = f"{tmpdirname}/foo/bar/baz.txt"
        output_file = PyArrowFileIO().new_output(file_path)
        parent_path = os.path.dirname(file_path)
        assert output_file._filesystem.get_file_info(parent_path).type == FileType.NotFound
        try:
            with output_file.create() as f:
                f.write(b"foo")
        except Exception:
            pytest.fail("Failed to write to file without parent directory")


def test_pyarrow_input_file() -> None:
    """Test reading a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowFileIO().new_input(location=f"{absolute_file_location}")

        # Test opening and reading the file
        r = input_file.open(seekable=False)
        assert isinstance(r, InputStream)  # Test that the file object abides by the InputStream protocol
        data = r.read()
        assert data == b"foo"
        assert len(input_file) == 3
        with pytest.raises(OSError) as exc_info:
            r.seek(0, 0)
        assert "only valid on seekable files" in str(exc_info.value)


def test_pyarrow_input_file_seekable() -> None:
    """Test reading a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowFileIO().new_input(location=f"{absolute_file_location}")

        # Test opening and reading the file
        r = input_file.open(seekable=True)
        assert isinstance(r, InputStream)  # Test that the file object abides by the InputStream protocol
        data = r.read()
        assert data == b"foo"
        assert len(input_file) == 3
        r.seek(0, 0)
        data = r.read()
        assert data == b"foo"
        assert len(input_file) == 3


def test_pyarrow_output_file() -> None:
    """Test writing a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = PyArrowFileIO().new_output(location=f"{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        assert isinstance(f, OutputStream)  # Test that the file object abides by the OutputStream protocol
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


def test_pyarrow_invalid_scheme() -> None:
    """Test that a ValueError is raised if a location is provided with an invalid scheme"""

    with pytest.raises(ValueError) as exc_info:
        PyArrowFileIO().new_input("foo://bar/baz.txt")

    assert "Unrecognized filesystem type in URI" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        PyArrowFileIO().new_output("foo://bar/baz.txt")

    assert "Unrecognized filesystem type in URI" in str(exc_info.value)


def test_pyarrow_violating_input_stream_protocol() -> None:
    """Test that a TypeError is raised if an input file is provided that violates the InputStream protocol"""

    # Missing seek, tell, closed, and close
    input_file_mock = MagicMock(spec=["read"])

    # Create a mocked filesystem that returns input_file_mock
    filesystem_mock = MagicMock()
    filesystem_mock.open_input_file.return_value = input_file_mock

    input_file = PyArrowFile("foo.txt", path="foo.txt", fs=filesystem_mock)

    f = input_file.open()
    assert not isinstance(f, InputStream)


def test_pyarrow_violating_output_stream_protocol() -> None:
    """Test that a TypeError is raised if an output stream is provided that violates the OutputStream protocol"""

    # Missing closed, and close
    output_file_mock = MagicMock(spec=["write", "exists"])
    output_file_mock.exists.return_value = False

    file_info_mock = MagicMock()
    file_info_mock.type = FileType.NotFound

    # Create a mocked filesystem that returns output_file_mock
    filesystem_mock = MagicMock()
    filesystem_mock.open_output_stream.return_value = output_file_mock
    filesystem_mock.get_file_info.return_value = file_info_mock

    output_file = PyArrowFile("foo.txt", path="foo.txt", fs=filesystem_mock)

    f = output_file.create()

    assert not isinstance(f, OutputStream)


def test_raise_on_opening_a_local_file_not_found() -> None:
    """Test that a PyArrowFile raises appropriately when a local file is not found"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        f = PyArrowFileIO().new_input(file_location)

        with pytest.raises(FileNotFoundError) as exc_info:
            f.open()

        assert "[Errno 2] Failed to open local file" in str(exc_info.value)


def test_raise_on_opening_an_s3_file_no_permission() -> None:
    """Test that opening a PyArrowFile raises a PermissionError when the pyarrow error includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.open_input_file.side_effect = OSError("AWS Error [code 15]")

    f = PyArrowFile("s3://foo/bar.txt", path="foo/bar.txt", fs=s3fs_mock)

    with pytest.raises(PermissionError) as exc_info:
        f.open()

    assert "Cannot open file, access denied:" in str(exc_info.value)


def test_raise_on_opening_an_s3_file_not_found() -> None:
    """Test that a PyArrowFile raises a FileNotFoundError when the pyarrow error includes 'Path does not exist'"""

    s3fs_mock = MagicMock()
    s3fs_mock.open_input_file.side_effect = OSError("Path does not exist")

    f = PyArrowFile("s3://foo/bar.txt", path="foo/bar.txt", fs=s3fs_mock)

    with pytest.raises(FileNotFoundError) as exc_info:
        f.open()

    assert "Cannot open file, does not exist:" in str(exc_info.value)


@patch("pyiceberg.io.pyarrow.PyArrowFile.exists", return_value=False)
def test_raise_on_creating_an_s3_file_no_permission(_: Any) -> None:
    """Test that creating a PyArrowFile raises a PermissionError when the pyarrow error includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.open_output_stream.side_effect = OSError("AWS Error [code 15]")

    f = PyArrowFile("s3://foo/bar.txt", path="foo/bar.txt", fs=s3fs_mock)

    with pytest.raises(PermissionError) as exc_info:
        f.create()

    assert "Cannot create file, access denied:" in str(exc_info.value)


def test_deleting_s3_file_no_permission() -> None:
    """Test that a PyArrowFile raises a PermissionError when the pyarrow OSError includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.delete_file.side_effect = OSError("AWS Error [code 15]")

    with patch.object(PyArrowFileIO, "_initialize_fs") as submocked:
        submocked.return_value = s3fs_mock

        with pytest.raises(PermissionError) as exc_info:
            PyArrowFileIO().delete("s3://foo/bar.txt")

    assert "Cannot delete file, access denied:" in str(exc_info.value)


def test_deleting_s3_file_not_found() -> None:
    """Test that a PyArrowFile raises a PermissionError when the pyarrow error includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.delete_file.side_effect = OSError("Path does not exist")

    with patch.object(PyArrowFileIO, "_initialize_fs") as submocked:
        submocked.return_value = s3fs_mock

        with pytest.raises(FileNotFoundError) as exc_info:
            PyArrowFileIO().delete("s3://foo/bar.txt")

        assert "Cannot delete file, does not exist:" in str(exc_info.value)


def test_deleting_hdfs_file_not_found() -> None:
    """Test that a PyArrowFile raises a PermissionError when the pyarrow error includes 'No such file or directory'"""

    hdfs_mock = MagicMock()
    hdfs_mock.delete_file.side_effect = OSError("Path does not exist")

    with patch.object(PyArrowFileIO, "_initialize_fs") as submocked:
        submocked.return_value = hdfs_mock

        with pytest.raises(FileNotFoundError) as exc_info:
            PyArrowFileIO().delete("hdfs://foo/bar.txt")

        assert "Cannot delete file, does not exist:" in str(exc_info.value)


def test_pyarrow_s3_session_properties() -> None:
    session_properties: Properties = {
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.session-token": "s3.session-token",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with patch("pyarrow.fs.S3FileSystem") as mock_s3fs, patch("pyarrow.fs.resolve_s3_region") as mock_s3_region_resolver:
        s3_fileio = PyArrowFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        # Mock `resolve_s3_region` to prevent from the location used resolving to a different s3 region
        mock_s3_region_resolver.side_effect = OSError("S3 bucket is not found")
        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            endpoint_override="http://localhost:9000",
            access_key="admin",
            secret_key="password",
            region="us-east-1",
            session_token="s3.session-token",
        )


def test_pyarrow_s3_session_properties_with_anonymous() -> None:
    session_properties: Properties = {
        "s3.anonymous": "true",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.session-token": "s3.session-token",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with patch("pyarrow.fs.S3FileSystem") as mock_s3fs, patch("pyarrow.fs.resolve_s3_region") as mock_s3_region_resolver:
        s3_fileio = PyArrowFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        # Mock `resolve_s3_region` to prevent from the location used resolving to a different s3 region
        mock_s3_region_resolver.side_effect = OSError("S3 bucket is not found")
        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anonymous=True,
            endpoint_override="http://localhost:9000",
            access_key="admin",
            secret_key="password",
            region="us-east-1",
            session_token="s3.session-token",
        )


def test_pyarrow_unified_session_properties() -> None:
    session_properties: Properties = {
        "s3.endpoint": "http://localhost:9000",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with patch("pyarrow.fs.S3FileSystem") as mock_s3fs, patch("pyarrow.fs.resolve_s3_region") as mock_s3_region_resolver:
        s3_fileio = PyArrowFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        mock_s3_region_resolver.return_value = "client.region"
        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            endpoint_override="http://localhost:9000",
            access_key="client.access-key-id",
            secret_key="client.secret-access-key",
            region="client.region",
            session_token="client.session-token",
        )


def test_schema_to_pyarrow_schema_include_field_ids(table_schema_nested: Schema) -> None:
    actual = schema_to_pyarrow(table_schema_nested)
    expected = """foo: large_string
  -- field metadata --
  PARQUET:field_id: '1'
bar: int32 not null
  -- field metadata --
  PARQUET:field_id: '2'
baz: bool
  -- field metadata --
  PARQUET:field_id: '3'
qux: large_list<element: large_string not null> not null
  child 0, element: large_string not null
    -- field metadata --
    PARQUET:field_id: '5'
  -- field metadata --
  PARQUET:field_id: '4'
quux: map<large_string, map<large_string, int32>> not null
  child 0, entries: struct<key: large_string not null, value: map<large_string, int32> not null> not null
      child 0, key: large_string not null
      -- field metadata --
      PARQUET:field_id: '7'
      child 1, value: map<large_string, int32> not null
          child 0, entries: struct<key: large_string not null, value: int32 not null> not null
              child 0, key: large_string not null
          -- field metadata --
          PARQUET:field_id: '9'
              child 1, value: int32 not null
          -- field metadata --
          PARQUET:field_id: '10'
      -- field metadata --
      PARQUET:field_id: '8'
  -- field metadata --
  PARQUET:field_id: '6'
location: large_list<element: struct<latitude: float, longitude: float> not null> not null
  child 0, element: struct<latitude: float, longitude: float> not null
      child 0, latitude: float
      -- field metadata --
      PARQUET:field_id: '13'
      child 1, longitude: float
      -- field metadata --
      PARQUET:field_id: '14'
    -- field metadata --
    PARQUET:field_id: '12'
  -- field metadata --
  PARQUET:field_id: '11'
person: struct<name: large_string, age: int32 not null>
  child 0, name: large_string
    -- field metadata --
    PARQUET:field_id: '16'
  child 1, age: int32 not null
    -- field metadata --
    PARQUET:field_id: '17'
  -- field metadata --
  PARQUET:field_id: '15'"""
    assert repr(actual) == expected


def test_schema_to_pyarrow_schema_exclude_field_ids(table_schema_nested: Schema) -> None:
    actual = schema_to_pyarrow(table_schema_nested, include_field_ids=False)
    expected = """foo: large_string
bar: int32 not null
baz: bool
qux: large_list<element: large_string not null> not null
  child 0, element: large_string not null
quux: map<large_string, map<large_string, int32>> not null
  child 0, entries: struct<key: large_string not null, value: map<large_string, int32> not null> not null
      child 0, key: large_string not null
      child 1, value: map<large_string, int32> not null
          child 0, entries: struct<key: large_string not null, value: int32 not null> not null
              child 0, key: large_string not null
              child 1, value: int32 not null
location: large_list<element: struct<latitude: float, longitude: float> not null> not null
  child 0, element: struct<latitude: float, longitude: float> not null
      child 0, latitude: float
      child 1, longitude: float
person: struct<name: large_string, age: int32 not null>
  child 0, name: large_string
  child 1, age: int32 not null"""
    assert repr(actual) == expected


def test_fixed_type_to_pyarrow() -> None:
    length = 22
    iceberg_type = FixedType(length)
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.binary(length)


def test_decimal_type_to_pyarrow() -> None:
    precision = 25
    scale = 19
    iceberg_type = DecimalType(precision, scale)
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.decimal128(precision, scale)


def test_boolean_type_to_pyarrow() -> None:
    iceberg_type = BooleanType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.bool_()


def test_integer_type_to_pyarrow() -> None:
    iceberg_type = IntegerType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.int32()


def test_long_type_to_pyarrow() -> None:
    iceberg_type = LongType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.int64()


def test_float_type_to_pyarrow() -> None:
    iceberg_type = FloatType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.float32()


def test_double_type_to_pyarrow() -> None:
    iceberg_type = DoubleType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.float64()


def test_date_type_to_pyarrow() -> None:
    iceberg_type = DateType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.date32()


def test_time_type_to_pyarrow() -> None:
    iceberg_type = TimeType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.time64("us")


def test_timestamp_type_to_pyarrow() -> None:
    iceberg_type = TimestampType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.timestamp(unit="us")


def test_timestamptz_type_to_pyarrow() -> None:
    iceberg_type = TimestamptzType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.timestamp(unit="us", tz="UTC")


def test_string_type_to_pyarrow() -> None:
    iceberg_type = StringType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.large_string()


def test_binary_type_to_pyarrow() -> None:
    iceberg_type = BinaryType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.large_binary()


def test_struct_type_to_pyarrow(table_schema_simple: Schema) -> None:
    expected = pa.struct(
        [
            pa.field("foo", pa.large_string(), nullable=True, metadata={"field_id": "1"}),
            pa.field("bar", pa.int32(), nullable=False, metadata={"field_id": "2"}),
            pa.field("baz", pa.bool_(), nullable=True, metadata={"field_id": "3"}),
        ]
    )
    assert visit(table_schema_simple.as_struct(), _ConvertToArrowSchema()) == expected


def test_map_type_to_pyarrow() -> None:
    iceberg_map = MapType(
        key_id=1,
        key_type=IntegerType(),
        value_id=2,
        value_type=StringType(),
        value_required=True,
    )
    assert visit(iceberg_map, _ConvertToArrowSchema()) == pa.map_(
        pa.field("key", pa.int32(), nullable=False, metadata={"field_id": "1"}),
        pa.field("value", pa.large_string(), nullable=False, metadata={"field_id": "2"}),
    )


def test_list_type_to_pyarrow() -> None:
    iceberg_map = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit(iceberg_map, _ConvertToArrowSchema()) == pa.large_list(
        pa.field("element", pa.int32(), nullable=False, metadata={"field_id": "1"})
    )


@pytest.fixture
def bound_reference(table_schema_simple: Schema) -> BoundReference[str]:
    return BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))


@pytest.fixture
def bound_double_reference() -> BoundReference[float]:
    schema = Schema(
        NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
        schema_id=1,
        identifier_field_ids=[],
    )
    return BoundReference(schema.find_field(1), schema.accessor_for_field(1))


def test_expr_is_null_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundIsNull(bound_reference)))
        == "<pyarrow.compute.Expression is_null(foo, {nan_is_null=false})>"
    )


def test_expr_not_null_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotNull(bound_reference))) == "<pyarrow.compute.Expression is_valid(foo)>"


def test_expr_is_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundIsNaN(bound_double_reference))) == "<pyarrow.compute.Expression is_nan(foo)>"


def test_expr_not_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotNaN(bound_double_reference))) == "<pyarrow.compute.Expression invert(is_nan(foo))>"


def test_expr_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundEqualTo(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo == "hello")>'
    )


def test_expr_not_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotEqualTo(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo != "hello")>'
    )


def test_expr_greater_than_or_equal_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundGreaterThanOrEqual(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo >= "hello")>'
    )


def test_expr_greater_than_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundGreaterThan(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo > "hello")>'
    )


def test_expr_less_than_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundLessThan(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo < "hello")>'
    )


def test_expr_less_than_or_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundLessThanOrEqual(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo <= "hello")>'
    )


def test_expr_in_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundIn(bound_reference, {literal("hello"), literal("world")}))) in (
        """<pyarrow.compute.Expression is_in(foo, {value_set=large_string:[
  "hello",
  "world"
], null_matching_behavior=MATCH})>""",
        """<pyarrow.compute.Expression is_in(foo, {value_set=large_string:[
  "world",
  "hello"
], null_matching_behavior=MATCH})>""",
    )


def test_expr_not_in_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotIn(bound_reference, {literal("hello"), literal("world")}))) in (
        """<pyarrow.compute.Expression invert(is_in(foo, {value_set=large_string:[
  "hello",
  "world"
], null_matching_behavior=MATCH}))>""",
        """<pyarrow.compute.Expression invert(is_in(foo, {value_set=large_string:[
  "world",
  "hello"
], null_matching_behavior=MATCH}))>""",
    )


def test_expr_starts_with_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundStartsWith(bound_reference, literal("he"))))
        == '<pyarrow.compute.Expression starts_with(foo, {pattern="he", ignore_case=false})>'
    )


def test_expr_not_starts_with_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotStartsWith(bound_reference, literal("he"))))
        == '<pyarrow.compute.Expression invert(starts_with(foo, {pattern="he", ignore_case=false}))>'
    )


def test_and_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(And(BoundEqualTo(bound_reference, literal("hello")), BoundIsNull(bound_reference))))
        == '<pyarrow.compute.Expression ((foo == "hello") and is_null(foo, {nan_is_null=false}))>'
    )


def test_or_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(Or(BoundEqualTo(bound_reference, literal("hello")), BoundIsNull(bound_reference))))
        == '<pyarrow.compute.Expression ((foo == "hello") or is_null(foo, {nan_is_null=false}))>'
    )


def test_not_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(Not(BoundEqualTo(bound_reference, literal("hello")))))
        == '<pyarrow.compute.Expression invert((foo == "hello"))>'
    )


def test_always_true_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(AlwaysTrue())) == "<pyarrow.compute.Expression true>"


def test_always_false_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(AlwaysFalse())) == "<pyarrow.compute.Expression false>"


@pytest.fixture
def schema_int() -> Schema:
    return Schema(NestedField(1, "id", IntegerType(), required=False))


@pytest.fixture
def schema_int_str() -> Schema:
    return Schema(NestedField(1, "id", IntegerType(), required=False), NestedField(2, "data", StringType(), required=False))


@pytest.fixture
def schema_str() -> Schema:
    return Schema(NestedField(2, "data", StringType(), required=False))


@pytest.fixture
def schema_long() -> Schema:
    return Schema(NestedField(3, "id", LongType(), required=False))


@pytest.fixture
def schema_struct() -> Schema:
    return Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType()),
                NestedField(42, "long", DoubleType()),
            ),
        )
    )


@pytest.fixture
def schema_list() -> Schema:
    return Schema(
        NestedField(5, "ids", ListType(51, IntegerType(), element_required=False), required=False),
    )


@pytest.fixture
def schema_list_of_structs() -> Schema:
    return Schema(
        NestedField(
            5,
            "locations",
            ListType(
                51,
                StructType(NestedField(511, "lat", DoubleType()), NestedField(512, "long", DoubleType())),
                element_required=False,
            ),
            required=False,
        ),
    )


@pytest.fixture
def schema_map_of_structs() -> Schema:
    return Schema(
        NestedField(
            5,
            "locations",
            MapType(
                key_id=51,
                value_id=52,
                key_type=StringType(),
                value_type=StructType(
                    NestedField(511, "lat", DoubleType(), required=True), NestedField(512, "long", DoubleType(), required=True)
                ),
                element_required=False,
            ),
            required=False,
        ),
    )


@pytest.fixture
def schema_map() -> Schema:
    return Schema(
        NestedField(
            5,
            "properties",
            MapType(
                key_id=51,
                key_type=StringType(),
                value_id=52,
                value_type=StringType(),
                value_required=True,
            ),
            required=False,
        ),
    )


def _write_table_to_file(filepath: str, schema: pa.Schema, table: pa.Table) -> str:
    with pq.ParquetWriter(filepath, schema) as writer:
        writer.write_table(table)
    return filepath


def _write_table_to_data_file(filepath: str, schema: pa.Schema, table: pa.Table) -> DataFile:
    filepath = _write_table_to_file(filepath, schema, table)
    return DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=filepath,
        file_format=FileFormat.PARQUET,
        partition={},
        record_count=len(table),
        file_size_in_bytes=22,  # This is not relevant for now
    )


@pytest.fixture
def file_int(schema_int: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_int, metadata={ICEBERG_SCHEMA: bytes(schema_int.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/a.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array([0, 1, 2])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_int_str(schema_int_str: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_int_str, metadata={ICEBERG_SCHEMA: bytes(schema_int_str.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/a.parquet",
        pyarrow_schema,
        pa.Table.from_arrays([pa.array([0, 1, 2]), pa.array(["0", "1", "2"])], schema=pyarrow_schema),
    )


@pytest.fixture
def file_string(schema_str: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_str, metadata={ICEBERG_SCHEMA: bytes(schema_str.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/b.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array(["0", "1", "2"])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_long(schema_long: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_long, metadata={ICEBERG_SCHEMA: bytes(schema_long.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/c.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array([0, 1, 2])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_struct(schema_struct: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_struct, metadata={ICEBERG_SCHEMA: bytes(schema_struct.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/d.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"location": {"lat": 52.371807, "long": 4.896029}},
                {"location": {"lat": 52.387386, "long": 4.646219}},
                {"location": {"lat": 52.078663, "long": 4.288788}},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_list(schema_list: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_list, metadata={ICEBERG_SCHEMA: bytes(schema_list.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"ids": list(range(1, 10))},
                {"ids": list(range(2, 20))},
                {"ids": list(range(3, 30))},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_list_of_structs(schema_list_of_structs: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(
        schema_list_of_structs, metadata={ICEBERG_SCHEMA: bytes(schema_list_of_structs.model_dump_json(), UTF8)}
    )
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"locations": [{"lat": 52.371807, "long": 4.896029}, {"lat": 52.387386, "long": 4.646219}]},
                {"locations": []},
                {"locations": [{"lat": 52.078663, "long": 4.288788}, {"lat": 52.387386, "long": 4.646219}]},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_map_of_structs(schema_map_of_structs: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(
        schema_map_of_structs, metadata={ICEBERG_SCHEMA: bytes(schema_map_of_structs.model_dump_json(), UTF8)}
    )
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"locations": {"1": {"lat": 52.371807, "long": 4.896029}, "2": {"lat": 52.387386, "long": 4.646219}}},
                {"locations": {}},
                {"locations": {"3": {"lat": 52.078663, "long": 4.288788}, "4": {"lat": 52.387386, "long": 4.646219}}},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_map(schema_map: Schema, tmpdir: str) -> str:
    pyarrow_schema = schema_to_pyarrow(schema_map, metadata={ICEBERG_SCHEMA: bytes(schema_map.model_dump_json(), UTF8)})
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"properties": [("a", "b")]},
                {"properties": [("c", "d")]},
                {"properties": [("e", "f"), ("g", "h")]},
            ],
            schema=pyarrow_schema,
        ),
    )


def project(
    schema: Schema, files: List[str], expr: Optional[BooleanExpression] = None, table_schema: Optional[Schema] = None
) -> pa.Table:
    def _set_spec_id(datafile: DataFile) -> DataFile:
        datafile.spec_id = 0
        return datafile

    return ArrowScan(
        table_metadata=TableMetadataV2(
            location="file://a/b/",
            last_column_id=1,
            format_version=2,
            schemas=[table_schema or schema],
            partition_specs=[PartitionSpec()],
        ),
        io=PyArrowFileIO(),
        projected_schema=schema,
        row_filter=expr or AlwaysTrue(),
        case_sensitive=True,
    ).to_table(
        tasks=[
            FileScanTask(
                _set_spec_id(
                    DataFile.from_args(
                        content=DataFileContent.DATA,
                        file_path=file,
                        file_format=FileFormat.PARQUET,
                        partition={},
                        record_count=3,
                        file_size_in_bytes=3,
                    )
                )
            )
            for file in files
        ]
    )


def test_projection_add_column(file_int: str) -> None:
    schema = Schema(
        # All new IDs
        NestedField(10, "id", IntegerType(), required=False),
        NestedField(20, "list", ListType(21, IntegerType(), element_required=False), required=False),
        NestedField(
            30,
            "map",
            MapType(key_id=31, key_type=IntegerType(), value_id=32, value_type=StringType(), value_required=False),
            required=False,
        ),
        NestedField(
            40,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False), NestedField(42, "lon", DoubleType(), required=False)
            ),
            required=False,
        ),
    )
    result_table = project(schema, [file_int])

    for col in result_table.columns:
        assert len(col) == 3

    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[1], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[2], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[3], [None, None, None]):
        assert actual.as_py() == expected
    assert (
        repr(result_table.schema)
        == """id: int32
list: large_list<element: int32>
  child 0, element: int32
map: map<int32, large_string>
  child 0, entries: struct<key: int32 not null, value: large_string> not null
      child 0, key: int32 not null
      child 1, value: large_string
location: struct<lat: double, lon: double>
  child 0, lat: double
  child 1, lon: double"""
    )


def test_read_list(schema_list: Schema, file_list: str) -> None:
    result_table = project(schema_list, [file_list])

    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [list(range(1, 10)), list(range(2, 20)), list(range(3, 30))]):
        assert actual.as_py() == expected

    assert (
        repr(result_table.schema)
        == """ids: large_list<element: int32>
  child 0, element: int32"""
    )


def test_read_map(schema_map: Schema, file_map: str) -> None:
    result_table = project(schema_map, [file_map])

    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [[("a", "b")], [("c", "d")], [("e", "f"), ("g", "h")]]):
        assert actual.as_py() == expected

    assert (
        repr(result_table.schema)
        == """properties: map<string, string>
  child 0, entries: struct<key: string not null, value: string not null> not null
      child 0, key: string not null
      child 1, value: string not null"""
    )


def test_projection_add_column_struct(schema_int: Schema, file_int: str) -> None:
    schema = Schema(
        # A new ID
        NestedField(
            2,
            "id",
            MapType(key_id=3, key_type=IntegerType(), value_id=4, value_type=StringType(), value_required=False),
            required=False,
        )
    )
    result_table = project(schema, [file_int])
    # Everything should be None
    for r in result_table.columns[0]:
        assert r.as_py() is None
    assert (
        repr(result_table.schema)
        == """id: map<int32, large_string>
  child 0, entries: struct<key: int32 not null, value: large_string> not null
      child 0, key: int32 not null
      child 1, value: large_string"""
    )


def test_projection_add_column_struct_required(file_int: str) -> None:
    schema = Schema(
        # A new ID
        NestedField(
            2,
            "other_id",
            IntegerType(),
            required=True,
        )
    )
    with pytest.raises(ResolveError) as exc_info:
        _ = project(schema, [file_int])
    assert "Field is required, and could not be found in the file: 2: other_id: required int" in str(exc_info.value)


def test_projection_rename_column(schema_int: Schema, file_int: str) -> None:
    schema = Schema(
        # Reuses the id 1
        NestedField(1, "other_name", IntegerType(), required=True)
    )
    result_table = project(schema, [file_int])
    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [0, 1, 2]):
        assert actual.as_py() == expected

    assert repr(result_table.schema) == "other_name: int32 not null"


def test_projection_concat_files(schema_int: Schema, file_int: str) -> None:
    result_table = project(schema_int, [file_int, file_int])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2, 0, 1, 2]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 6
    assert repr(result_table.schema) == "id: int32"


def test_identity_transform_column_projection(tmp_path: str, catalog: InMemoryCatalog) -> None:
    # Test by adding a non-partitioned data file to a partitioned table, verifying partition value projection from manifest metadata.
    # TODO: Update to use a data file created by writing data to an unpartitioned table once add_files supports field IDs.
    # (context: https://github.com/apache/iceberg-python/pull/1443#discussion_r1901374875)

    schema = Schema(
        NestedField(1, "other_field", StringType(), required=False), NestedField(2, "partition_id", IntegerType(), required=False)
    )

    partition_spec = PartitionSpec(
        PartitionField(2, 1000, IdentityTransform(), "partition_id"),
    )

    catalog.create_namespace("default")
    table = catalog.create_table(
        "default.test_projection_partition",
        schema=schema,
        partition_spec=partition_spec,
        properties={TableProperties.DEFAULT_NAME_MAPPING: create_mapping_from_schema(schema).model_dump_json()},
    )

    file_data = pa.array(["foo", "bar", "baz"], type=pa.string())
    file_loc = f"{tmp_path}/test.parquet"
    pq.write_table(pa.table([file_data], names=["other_field"]), file_loc)

    statistics = data_file_statistics_from_parquet_metadata(
        parquet_metadata=pq.read_metadata(file_loc),
        stats_columns=compute_statistics_plan(table.schema(), table.metadata.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(table.schema()),
    )

    unpartitioned_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_loc,
        file_format=FileFormat.PARQUET,
        # projected value
        partition=Record(1),
        file_size_in_bytes=os.path.getsize(file_loc),
        sort_order_id=None,
        spec_id=table.metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )

    with table.transaction() as transaction:
        with transaction.update_snapshot().overwrite() as update:
            update.append_data_file(unpartitioned_file)

    schema = pa.schema([("other_field", pa.string()), ("partition_id", pa.int32())])
    assert table.scan().to_arrow() == pa.table(
        {
            "other_field": ["foo", "bar", "baz"],
            "partition_id": [1, 1, 1],
        },
        schema=schema,
    )
    # Test that row filter works with partition value projection
    assert table.scan(row_filter="partition_id = 1").to_arrow() == pa.table(
        {
            "other_field": ["foo", "bar", "baz"],
            "partition_id": [1, 1, 1],
        },
        schema=schema,
    )
    # Test that row filter does not return any rows for a non-existing partition value
    assert len(table.scan(row_filter="partition_id = -1").to_arrow()) == 0


def test_identity_transform_columns_projection(tmp_path: str, catalog: InMemoryCatalog) -> None:
    # Test by adding a non-partitioned data file to a multi-partitioned table, verifying partition value projection from manifest metadata.
    # TODO: Update to use a data file created by writing data to an unpartitioned table once add_files supports field IDs.
    # (context: https://github.com/apache/iceberg-python/pull/1443#discussion_r1901374875)
    schema = Schema(
        NestedField(1, "field_1", StringType(), required=False),
        NestedField(2, "field_2", IntegerType(), required=False),
        NestedField(3, "field_3", IntegerType(), required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(2, 1000, IdentityTransform(), "field_2"),
        PartitionField(3, 1001, IdentityTransform(), "field_3"),
    )

    catalog.create_namespace("default")
    table = catalog.create_table(
        "default.test_projection_partitions",
        schema=schema,
        partition_spec=partition_spec,
        properties={TableProperties.DEFAULT_NAME_MAPPING: create_mapping_from_schema(schema).model_dump_json()},
    )

    file_data = pa.array(["foo"], type=pa.string())
    file_loc = f"{tmp_path}/test.parquet"
    pq.write_table(pa.table([file_data], names=["field_1"]), file_loc)

    statistics = data_file_statistics_from_parquet_metadata(
        parquet_metadata=pq.read_metadata(file_loc),
        stats_columns=compute_statistics_plan(table.schema(), table.metadata.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(table.schema()),
    )

    unpartitioned_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_loc,
        file_format=FileFormat.PARQUET,
        # projected value
        partition=Record(2, 3),
        file_size_in_bytes=os.path.getsize(file_loc),
        sort_order_id=None,
        spec_id=table.metadata.default_spec_id,
        equality_ids=None,
        key_metadata=None,
        **statistics.to_serialized_dict(),
    )

    with table.transaction() as transaction:
        with transaction.update_snapshot().overwrite() as update:
            update.append_data_file(unpartitioned_file)

    assert (
        str(table.scan().to_arrow())
        == """pyarrow.Table
field_1: string
field_2: int32
field_3: int32
----
field_1: [["foo"]]
field_2: [[2]]
field_3: [[3]]"""
    )


@pytest.fixture
def catalog() -> InMemoryCatalog:
    return InMemoryCatalog("test.in_memory.catalog", **{"test.key": "test.value"})


def test_projection_filter(schema_int: Schema, file_int: str) -> None:
    result_table = project(schema_int, [file_int], GreaterThan("id", 4))
    assert len(result_table.columns[0]) == 0
    assert repr(result_table.schema) == """id: int32"""


def test_projection_filter_renamed_column(file_int: str) -> None:
    schema = Schema(
        # Reuses the id 1
        NestedField(1, "other_id", IntegerType(), required=True)
    )
    result_table = project(schema, [file_int], GreaterThan("other_id", 1))
    assert len(result_table.columns[0]) == 1
    assert repr(result_table.schema) == "other_id: int32 not null"


def test_projection_filter_add_column(schema_int: Schema, file_int: str, file_string: str) -> None:
    """We have one file that has the column, and the other one doesn't"""
    result_table = project(schema_int, [file_int, file_string])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2, None, None, None]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 6
    assert repr(result_table.schema) == "id: int32"


def test_projection_filter_add_column_promote(file_int: str) -> None:
    schema_long = Schema(NestedField(1, "id", LongType(), required=True))
    result_table = project(schema_long, [file_int])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "id: int64 not null"


def test_projection_filter_add_column_demote(file_long: str) -> None:
    schema_int = Schema(NestedField(3, "id", IntegerType()))
    with pytest.raises(ResolveError) as exc_info:
        _ = project(schema_int, [file_long])
    assert "Cannot promote long to int" in str(exc_info.value)


def test_projection_nested_struct_subset(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=True),
                # long is missing!
            ),
            required=True,
        )
    )

    result_table = project(schema, [file_struct])

    for actual, expected in zip(result_table.columns[0], [52.371807, 52.387386, 52.078663]):
        assert actual.as_py() == {"lat": expected}

    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == """location: struct<lat: double not null> not null
  child 0, lat: double not null"""
    )


def test_projection_nested_new_field(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(43, "null", DoubleType(), required=False),  # Whoa, this column doesn't exist in the file
            ),
            required=True,
        )
    )

    result_table = project(schema, [file_struct])

    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == {"null": expected}
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == """location: struct<null: double> not null
  child 0, null: double"""
    )


def test_projection_nested_struct(schema_struct: Schema, file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False),
                NestedField(43, "null", DoubleType(), required=False),
                NestedField(42, "long", DoubleType(), required=False),
            ),
            required=True,
        )
    )

    result_table = project(schema, [file_struct])
    for actual, expected in zip(
        result_table.columns[0],
        [
            {"lat": 52.371807, "long": 4.896029, "null": None},
            {"lat": 52.387386, "long": 4.646219, "null": None},
            {"lat": 52.078663, "long": 4.288788, "null": None},
        ],
    ):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == """location: struct<lat: double, null: double, long: double> not null
  child 0, lat: double
  child 1, null: double
  child 2, long: double"""
    )


def test_projection_list_of_structs(schema_list_of_structs: Schema, file_list_of_structs: str) -> None:
    schema = Schema(
        NestedField(
            5,
            "locations",
            ListType(
                51,
                StructType(
                    NestedField(511, "latitude", DoubleType(), required=True),
                    NestedField(512, "longitude", DoubleType(), required=True),
                    NestedField(513, "altitude", DoubleType(), required=False),
                ),
                element_required=False,
            ),
            required=False,
        ),
    )

    result_table = project(schema, [file_list_of_structs])
    assert len(result_table.columns) == 1
    assert len(result_table.columns[0]) == 3
    results = [row.as_py() for row in result_table.columns[0]]
    assert results == [
        [
            {"latitude": 52.371807, "longitude": 4.896029, "altitude": None},
            {"latitude": 52.387386, "longitude": 4.646219, "altitude": None},
        ],
        [],
        [
            {"latitude": 52.078663, "longitude": 4.288788, "altitude": None},
            {"latitude": 52.387386, "longitude": 4.646219, "altitude": None},
        ],
    ]
    assert (
        repr(result_table.schema)
        == """locations: large_list<element: struct<latitude: double not null, longitude: double not null, altitude: double>>
  child 0, element: struct<latitude: double not null, longitude: double not null, altitude: double>
      child 0, latitude: double not null
      child 1, longitude: double not null
      child 2, altitude: double"""
    )


def test_projection_maps_of_structs(schema_map_of_structs: Schema, file_map_of_structs: str) -> None:
    schema = Schema(
        NestedField(
            5,
            "locations",
            MapType(
                key_id=51,
                value_id=52,
                key_type=StringType(),
                value_type=StructType(
                    NestedField(511, "latitude", DoubleType(), required=True),
                    NestedField(512, "longitude", DoubleType(), required=True),
                    NestedField(513, "altitude", DoubleType()),
                ),
                element_required=False,
            ),
            required=False,
        ),
    )

    result_table = project(schema, [file_map_of_structs])
    assert len(result_table.columns) == 1
    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(
        result_table.columns[0],
        [
            [
                ("1", {"latitude": 52.371807, "longitude": 4.896029, "altitude": None}),
                ("2", {"latitude": 52.387386, "longitude": 4.646219, "altitude": None}),
            ],
            [],
            [
                ("3", {"latitude": 52.078663, "longitude": 4.288788, "altitude": None}),
                ("4", {"latitude": 52.387386, "longitude": 4.646219, "altitude": None}),
            ],
        ],
    ):
        assert actual.as_py() == expected
    assert (
        repr(result_table.schema)
        == """locations: map<string, struct<latitude: double not null, longitude: double not null, altitude: double>>
  child 0, entries: struct<key: string not null, value: struct<latitude: double not null, longitude: double not null, altitude: double> not null> not null
      child 0, key: string not null
      child 1, value: struct<latitude: double not null, longitude: double not null, altitude: double> not null
          child 0, latitude: double not null
          child 1, longitude: double not null
          child 2, altitude: double"""
    )


def test_projection_nested_struct_different_parent_id(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            5,  # 😱 this is 4 in the file, this will be fixed when projecting the file schema
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False), NestedField(42, "long", DoubleType(), required=False)
            ),
            required=False,
        )
    )

    result_table = project(schema, [file_struct])
    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == """location: struct<lat: double, long: double>
  child 0, lat: double
  child 1, long: double"""
    )


def test_projection_filter_on_unprojected_field(schema_int_str: Schema, file_int_str: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))

    result_table = project(schema, [file_int_str], GreaterThan("data", "1"), schema_int_str)

    for actual, expected in zip(
        result_table.columns[0],
        [2],
    ):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 1
    assert repr(result_table.schema) == "id: int32 not null"


def test_projection_filter_on_unknown_field(schema_int_str: Schema, file_int_str: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType()))

    with pytest.raises(ValueError) as exc_info:
        _ = project(schema, [file_int_str], GreaterThan("unknown_field", "1"), schema_int_str)

    assert "Could not find field with name unknown_field, case_sensitive=True" in str(exc_info.value)


@pytest.fixture
def deletes_file(tmp_path: str, example_task: FileScanTask) -> str:
    path = example_task.file.file_path
    table = pa.table({"file_path": [path, path, path], "pos": [1, 3, 5]})

    deletes_file_path = f"{tmp_path}/deletes.parquet"
    pq.write_table(table, deletes_file_path)

    return deletes_file_path


def test_read_deletes(deletes_file: str, example_task: FileScanTask) -> None:
    deletes = _read_deletes(PyArrowFileIO(), DataFile.from_args(file_path=deletes_file, file_format=FileFormat.PARQUET))
    assert set(deletes.keys()) == {example_task.file.file_path}
    assert list(deletes.values())[0] == pa.chunked_array([[1, 3, 5]])


def test_delete(deletes_file: str, example_task: FileScanTask, table_schema_simple: Schema) -> None:
    metadata_location = "file://a/b/c.json"
    example_task_with_delete = FileScanTask(
        data_file=example_task.file,
        delete_files={
            DataFile.from_args(content=DataFileContent.POSITION_DELETES, file_path=deletes_file, file_format=FileFormat.PARQUET)
        },
    )
    with_deletes = ArrowScan(
        table_metadata=TableMetadataV2(
            location=metadata_location,
            last_column_id=1,
            format_version=2,
            current_schema_id=1,
            schemas=[table_schema_simple],
            partition_specs=[PartitionSpec()],
        ),
        io=load_file_io(),
        projected_schema=table_schema_simple,
        row_filter=AlwaysTrue(),
    ).to_table(tasks=[example_task_with_delete])

    assert (
        str(with_deletes)
        == """pyarrow.Table
foo: large_string
bar: int32 not null
baz: bool
----
foo: [["a","c"]]
bar: [[1,3]]
baz: [[true,null]]"""
    )


def test_delete_duplicates(deletes_file: str, example_task: FileScanTask, table_schema_simple: Schema) -> None:
    metadata_location = "file://a/b/c.json"
    example_task_with_delete = FileScanTask(
        data_file=example_task.file,
        delete_files={
            DataFile.from_args(content=DataFileContent.POSITION_DELETES, file_path=deletes_file, file_format=FileFormat.PARQUET),
            DataFile.from_args(content=DataFileContent.POSITION_DELETES, file_path=deletes_file, file_format=FileFormat.PARQUET),
        },
    )

    with_deletes = ArrowScan(
        table_metadata=TableMetadataV2(
            location=metadata_location,
            last_column_id=1,
            format_version=2,
            current_schema_id=1,
            schemas=[table_schema_simple],
            partition_specs=[PartitionSpec()],
        ),
        io=load_file_io(),
        projected_schema=table_schema_simple,
        row_filter=AlwaysTrue(),
    ).to_table(tasks=[example_task_with_delete])

    assert (
        str(with_deletes)
        == """pyarrow.Table
foo: large_string
bar: int32 not null
baz: bool
----
foo: [["a","c"]]
bar: [[1,3]]
baz: [[true,null]]"""
    )


def test_pyarrow_wrap_fsspec(example_task: FileScanTask, table_schema_simple: Schema) -> None:
    metadata_location = "file://a/b/c.json"

    projection = ArrowScan(
        table_metadata=TableMetadataV2(
            location=metadata_location,
            last_column_id=1,
            format_version=2,
            current_schema_id=1,
            schemas=[table_schema_simple],
            partition_specs=[PartitionSpec()],
        ),
        io=load_file_io(properties={"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}, location=metadata_location),
        case_sensitive=True,
        projected_schema=table_schema_simple,
        row_filter=AlwaysTrue(),
    ).to_table(tasks=[example_task])

    assert (
        str(projection)
        == """pyarrow.Table
foo: large_string
bar: int32 not null
baz: bool
----
foo: [["a","b","c"]]
bar: [[1,2,3]]
baz: [[true,false,null]]"""
    )


@pytest.mark.gcs
def test_new_input_file_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test creating a new input file from a fsspec file-io"""
    filename = str(uuid4())

    input_file = pyarrow_fileio_gcs.new_input(f"gs://warehouse/{filename}")

    assert isinstance(input_file, PyArrowFile)
    assert input_file.location == f"gs://warehouse/{filename}"


@pytest.mark.gcs
def test_new_output_file_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test creating a new output file from an fsspec file-io"""
    filename = str(uuid4())

    output_file = pyarrow_fileio_gcs.new_output(f"gs://warehouse/{filename}")

    assert isinstance(output_file, PyArrowFile)
    assert output_file.location == f"gs://warehouse/{filename}"


@pytest.mark.gcs
@pytest.mark.skip(reason="Open issue on Arrow: https://github.com/apache/arrow/issues/36993")
def test_write_and_read_file_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test writing and reading a file using PyArrowFile"""
    location = f"gs://warehouse/{uuid4()}.txt"
    output_file = pyarrow_fileio_gcs.new_output(location=location)
    with output_file.create() as f:
        assert f.write(b"foo") == 3

    assert output_file.exists()

    input_file = pyarrow_fileio_gcs.new_input(location=location)
    with input_file.open() as f:
        assert f.read() == b"foo"

    pyarrow_fileio_gcs.delete(input_file)


@pytest.mark.gcs
def test_getting_length_of_file_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test getting the length of PyArrowFile"""
    filename = str(uuid4())

    output_file = pyarrow_fileio_gcs.new_output(location=f"gs://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = pyarrow_fileio_gcs.new_input(location=f"gs://warehouse/{filename}")
    assert len(input_file) == 6

    pyarrow_fileio_gcs.delete(output_file)


@pytest.mark.gcs
@pytest.mark.skip(reason="Open issue on Arrow: https://github.com/apache/arrow/issues/36993")
def test_file_tell_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    location = f"gs://warehouse/{uuid4()}"

    output_file = pyarrow_fileio_gcs.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foobar")

    input_file = pyarrow_fileio_gcs.new_input(location=location)
    with input_file.open() as f:
        f.seek(0)
        assert f.tell() == 0
        f.seek(1)
        assert f.tell() == 1
        f.seek(3)
        assert f.tell() == 3
        f.seek(0)
        assert f.tell() == 0


@pytest.mark.gcs
@pytest.mark.skip(reason="Open issue on Arrow: https://github.com/apache/arrow/issues/36993")
def test_read_specified_bytes_for_file_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    location = f"gs://warehouse/{uuid4()}"

    output_file = pyarrow_fileio_gcs.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foo")

    input_file = pyarrow_fileio_gcs.new_input(location=location)
    with input_file.open() as f:
        f.seek(0)
        assert b"f" == f.read(1)
        f.seek(0)
        assert b"fo" == f.read(2)
        f.seek(1)
        assert b"o" == f.read(1)
        f.seek(1)
        assert b"oo" == f.read(2)
        f.seek(0)
        assert b"foo" == f.read(999)  # test reading amount larger than entire content length

    pyarrow_fileio_gcs.delete(input_file)


@pytest.mark.gcs
@pytest.mark.skip(reason="Open issue on Arrow: https://github.com/apache/arrow/issues/36993")
def test_raise_on_opening_file_not_found_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test that PyArrowFile raises appropriately when the gcs file is not found"""

    filename = str(uuid4())
    input_file = pyarrow_fileio_gcs.new_input(location=f"gs://warehouse/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.gcs
def test_checking_if_a_file_exists_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test checking if a file exists"""
    non_existent_file = pyarrow_fileio_gcs.new_input(location="gs://warehouse/does-not-exist.txt")
    assert not non_existent_file.exists()

    location = f"gs://warehouse/{uuid4()}"
    output_file = pyarrow_fileio_gcs.new_output(location=location)
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = pyarrow_fileio_gcs.new_input(location=location)
    assert existing_input_file.exists()

    existing_output_file = pyarrow_fileio_gcs.new_output(location=location)
    assert existing_output_file.exists()

    pyarrow_fileio_gcs.delete(existing_output_file)


@pytest.mark.gcs
@pytest.mark.skip(reason="Open issue on Arrow: https://github.com/apache/arrow/issues/36993")
def test_closing_a_file_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test closing an output file and input file"""
    filename = str(uuid4())
    output_file = pyarrow_fileio_gcs.new_output(location=f"gs://warehouse/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")
        assert not write_file.closed  # type: ignore
    assert write_file.closed  # type: ignore

    input_file = pyarrow_fileio_gcs.new_input(location=f"gs://warehouse/{filename}")
    with input_file.open() as f:
        assert not f.closed  # type: ignore
    assert f.closed  # type: ignore

    pyarrow_fileio_gcs.delete(f"gs://warehouse/{filename}")


@pytest.mark.gcs
def test_converting_an_outputfile_to_an_inputfile_gcs(pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test converting an output file to an input file"""
    filename = str(uuid4())
    output_file = pyarrow_fileio_gcs.new_output(location=f"gs://warehouse/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location


@pytest.mark.gcs
@pytest.mark.skip(reason="Open issue on Arrow: https://github.com/apache/arrow/issues/36993")
def test_writing_avro_file_gcs(generated_manifest_entry_file: str, pyarrow_fileio_gcs: PyArrowFileIO) -> None:
    """Test that bytes match when reading a local avro file, writing it using pyarrow file-io, and then reading it again"""
    filename = str(uuid4())
    with PyArrowFileIO().new_input(location=generated_manifest_entry_file).open() as f:
        b1 = f.read()
        with pyarrow_fileio_gcs.new_output(location=f"gs://warehouse/{filename}").create() as out_f:
            out_f.write(b1)
        with pyarrow_fileio_gcs.new_input(location=f"gs://warehouse/{filename}").open() as in_f:
            b2 = in_f.read()
            assert b1 == b2  # Check that bytes of read from local avro file match bytes written to s3

    pyarrow_fileio_gcs.delete(f"gs://warehouse/{filename}")


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_new_input_file_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test creating a new input file from pyarrow file-io"""
    filename = str(uuid4())

    input_file = pyarrow_fileio_adls.new_input(f"{adls_scheme}://warehouse/{filename}")

    assert isinstance(input_file, PyArrowFile)
    assert input_file.location == f"{adls_scheme}://warehouse/{filename}"


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_new_output_file_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test creating a new output file from pyarrow file-io"""
    filename = str(uuid4())

    output_file = pyarrow_fileio_adls.new_output(f"{adls_scheme}://warehouse/{filename}")

    assert isinstance(output_file, PyArrowFile)
    assert output_file.location == f"{adls_scheme}://warehouse/{filename}"


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_write_and_read_file_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test writing and reading a file using PyArrowFile"""
    location = f"{adls_scheme}://warehouse/{uuid4()}.txt"
    output_file = pyarrow_fileio_adls.new_output(location=location)
    with output_file.create() as f:
        assert f.write(b"foo") == 3

    assert output_file.exists()

    input_file = pyarrow_fileio_adls.new_input(location=location)
    with input_file.open() as f:
        assert f.read() == b"foo"

    pyarrow_fileio_adls.delete(input_file)


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_getting_length_of_file_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test getting the length of PyArrowFile"""
    filename = str(uuid4())

    output_file = pyarrow_fileio_adls.new_output(location=f"{adls_scheme}://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = pyarrow_fileio_adls.new_input(location=f"{adls_scheme}://warehouse/{filename}")
    assert len(input_file) == 6

    pyarrow_fileio_adls.delete(output_file)


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_file_tell_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    location = f"{adls_scheme}://warehouse/{uuid4()}"

    output_file = pyarrow_fileio_adls.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foobar")

    input_file = pyarrow_fileio_adls.new_input(location=location)
    with input_file.open() as f:
        f.seek(0)
        assert f.tell() == 0
        f.seek(1)
        assert f.tell() == 1
        f.seek(3)
        assert f.tell() == 3
        f.seek(0)
        assert f.tell() == 0


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_read_specified_bytes_for_file_adls(pyarrow_fileio_adls: PyArrowFileIO) -> None:
    location = f"abfss://warehouse/{uuid4()}"

    output_file = pyarrow_fileio_adls.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foo")

    input_file = pyarrow_fileio_adls.new_input(location=location)
    with input_file.open() as f:
        f.seek(0)
        assert b"f" == f.read(1)
        f.seek(0)
        assert b"fo" == f.read(2)
        f.seek(1)
        assert b"o" == f.read(1)
        f.seek(1)
        assert b"oo" == f.read(2)
        f.seek(0)
        assert b"foo" == f.read(999)  # test reading amount larger than entire content length

    pyarrow_fileio_adls.delete(input_file)


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_raise_on_opening_file_not_found_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test that PyArrowFile raises appropriately when the adls file is not found"""

    filename = str(uuid4())
    input_file = pyarrow_fileio_adls.new_input(location=f"{adls_scheme}://warehouse/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_checking_if_a_file_exists_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test checking if a file exists"""
    non_existent_file = pyarrow_fileio_adls.new_input(location=f"{adls_scheme}://warehouse/does-not-exist.txt")
    assert not non_existent_file.exists()

    location = f"{adls_scheme}://warehouse/{uuid4()}"
    output_file = pyarrow_fileio_adls.new_output(location=location)
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = pyarrow_fileio_adls.new_input(location=location)
    assert existing_input_file.exists()

    existing_output_file = pyarrow_fileio_adls.new_output(location=location)
    assert existing_output_file.exists()

    pyarrow_fileio_adls.delete(existing_output_file)


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_closing_a_file_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test closing an output file and input file"""
    filename = str(uuid4())
    output_file = pyarrow_fileio_adls.new_output(location=f"{adls_scheme}://warehouse/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")
        assert not write_file.closed  # type: ignore
    assert write_file.closed  # type: ignore

    input_file = pyarrow_fileio_adls.new_input(location=f"{adls_scheme}://warehouse/{filename}")
    with input_file.open() as f:
        assert not f.closed  # type: ignore
    assert f.closed  # type: ignore

    pyarrow_fileio_adls.delete(f"{adls_scheme}://warehouse/{filename}")


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_converting_an_outputfile_to_an_inputfile_adls(pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test converting an output file to an input file"""
    filename = str(uuid4())
    output_file = pyarrow_fileio_adls.new_output(location=f"{adls_scheme}://warehouse/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location


@pytest.mark.adls
@skip_if_pyarrow_too_old
def test_writing_avro_file_adls(generated_manifest_entry_file: str, pyarrow_fileio_adls: PyArrowFileIO, adls_scheme: str) -> None:
    """Test that bytes match when reading a local avro file, writing it using pyarrow file-io, and then reading it again"""
    filename = str(uuid4())
    with PyArrowFileIO().new_input(location=generated_manifest_entry_file).open() as f:
        b1 = f.read()
        with pyarrow_fileio_adls.new_output(location=f"{adls_scheme}://warehouse/{filename}").create() as out_f:
            out_f.write(b1)
        with pyarrow_fileio_adls.new_input(location=f"{adls_scheme}://warehouse/{filename}").open() as in_f:
            b2 = in_f.read()
            assert b1 == b2  # Check that bytes of read from local avro file match bytes written to s3

    pyarrow_fileio_adls.delete(f"{adls_scheme}://warehouse/{filename}")


def test_parse_location() -> None:
    def check_results(location: str, expected_schema: str, expected_netloc: str, expected_uri: str) -> None:
        schema, netloc, uri = PyArrowFileIO.parse_location(location)
        assert schema == expected_schema
        assert netloc == expected_netloc
        assert uri == expected_uri

    check_results("hdfs://127.0.0.1:9000/root/foo.txt", "hdfs", "127.0.0.1:9000", "/root/foo.txt")
    check_results("hdfs://127.0.0.1/root/foo.txt", "hdfs", "127.0.0.1", "/root/foo.txt")
    check_results("hdfs://clusterA/root/foo.txt", "hdfs", "clusterA", "/root/foo.txt")

    check_results("/root/foo.txt", "file", "", "/root/foo.txt")
    check_results("/root/tmp/foo.txt", "file", "", "/root/tmp/foo.txt")


def test_make_compatible_name() -> None:
    assert make_compatible_name("label/abc") == "label_x2Fabc"
    assert make_compatible_name("label?abc") == "label_x3Fabc"


@pytest.mark.parametrize(
    "vals, primitive_type, expected_result",
    [
        ([None, 2, 1], IntegerType(), 1),
        ([1, None, 2], IntegerType(), 1),
        ([None, None, None], IntegerType(), None),
        ([None, date(2024, 2, 4), date(2024, 1, 2)], DateType(), date(2024, 1, 2)),
        ([date(2024, 1, 2), None, date(2024, 2, 4)], DateType(), date(2024, 1, 2)),
        ([None, None, None], DateType(), None),
    ],
)
def test_stats_aggregator_update_min(vals: List[Any], primitive_type: PrimitiveType, expected_result: Any) -> None:
    stats = StatsAggregator(primitive_type, _primitive_to_physical(primitive_type))

    for val in vals:
        stats.update_min(val)

    assert stats.current_min == expected_result


@pytest.mark.parametrize(
    "vals, primitive_type, expected_result",
    [
        ([None, 2, 1], IntegerType(), 2),
        ([1, None, 2], IntegerType(), 2),
        ([None, None, None], IntegerType(), None),
        ([None, date(2024, 2, 4), date(2024, 1, 2)], DateType(), date(2024, 2, 4)),
        ([date(2024, 1, 2), None, date(2024, 2, 4)], DateType(), date(2024, 2, 4)),
        ([None, None, None], DateType(), None),
    ],
)
def test_stats_aggregator_update_max(vals: List[Any], primitive_type: PrimitiveType, expected_result: Any) -> None:
    stats = StatsAggregator(primitive_type, _primitive_to_physical(primitive_type))

    for val in vals:
        stats.update_max(val)

    assert stats.current_max == expected_result


def test_bin_pack_arrow_table(arrow_table_with_null: pa.Table) -> None:
    # default packs to 1 bin since the table is small
    bin_packed = bin_pack_arrow_table(
        arrow_table_with_null, target_file_size=TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
    )
    assert len(list(bin_packed)) == 1

    # as long as table is smaller than default target size, it should pack to 1 bin
    bigger_arrow_tbl = pa.concat_tables([arrow_table_with_null] * 10)
    assert bigger_arrow_tbl.nbytes < TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
    bin_packed = bin_pack_arrow_table(bigger_arrow_tbl, target_file_size=TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
    assert len(list(bin_packed)) == 1

    # unless we override the target size to be smaller
    bin_packed = bin_pack_arrow_table(bigger_arrow_tbl, target_file_size=arrow_table_with_null.nbytes)
    assert len(list(bin_packed)) == 10

    # and will produce half the number of files if we double the target size
    bin_packed = bin_pack_arrow_table(bigger_arrow_tbl, target_file_size=arrow_table_with_null.nbytes * 2)
    assert len(list(bin_packed)) == 5


def test_schema_mismatch_type(table_schema_simple: Schema) -> None:
    other_schema = pa.schema(
        (
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.decimal128(18, 6), nullable=False),
            pa.field("baz", pa.bool_(), nullable=True),
        )
    )

    expected = r"""Mismatch in fields:
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃    ┃ Table field              ┃ Dataframe field                 ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ │ 1: foo: optional string  │ 1: foo: optional string         │
│ ❌ │ 2: bar: required int     │ 2: bar: required decimal\(18, 6\) │
│ ✅ │ 3: baz: optional boolean │ 3: baz: optional boolean        │
└────┴──────────────────────────┴─────────────────────────────────┘
"""

    with pytest.raises(ValueError, match=expected):
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)


def test_schema_mismatch_nullability(table_schema_simple: Schema) -> None:
    other_schema = pa.schema(
        (
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=True),
            pa.field("baz", pa.bool_(), nullable=True),
        )
    )

    expected = """Mismatch in fields:
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃    ┃ Table field              ┃ Dataframe field          ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ │ 1: foo: optional string  │ 1: foo: optional string  │
│ ❌ │ 2: bar: required int     │ 2: bar: optional int     │
│ ✅ │ 3: baz: optional boolean │ 3: baz: optional boolean │
└────┴──────────────────────────┴──────────────────────────┘
"""

    with pytest.raises(ValueError, match=expected):
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)


def test_schema_compatible_nullability_diff(table_schema_simple: Schema) -> None:
    other_schema = pa.schema(
        (
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=False),
            pa.field("baz", pa.bool_(), nullable=False),
        )
    )

    try:
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)
    except Exception:
        pytest.fail("Unexpected Exception raised when calling `_check_pyarrow_schema_compatible`")


def test_schema_mismatch_missing_field(table_schema_simple: Schema) -> None:
    other_schema = pa.schema(
        (
            pa.field("foo", pa.string(), nullable=True),
            pa.field("baz", pa.bool_(), nullable=True),
        )
    )

    expected = """Mismatch in fields:
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃    ┃ Table field              ┃ Dataframe field          ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ │ 1: foo: optional string  │ 1: foo: optional string  │
│ ❌ │ 2: bar: required int     │ Missing                  │
│ ✅ │ 3: baz: optional boolean │ 3: baz: optional boolean │
└────┴──────────────────────────┴──────────────────────────┘
"""

    with pytest.raises(ValueError, match=expected):
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)


def test_schema_compatible_missing_nullable_field_nested(table_schema_nested: Schema) -> None:
    schema = table_schema_nested.as_arrow()
    schema = schema.remove(6).insert(
        6,
        pa.field(
            "person",
            pa.struct(
                [
                    pa.field("age", pa.int32(), nullable=False),
                ]
            ),
            nullable=True,
        ),
    )
    try:
        _check_pyarrow_schema_compatible(table_schema_nested, schema)
    except Exception:
        pytest.fail("Unexpected Exception raised when calling `_check_pyarrow_schema_compatible`")


def test_schema_mismatch_missing_required_field_nested(table_schema_nested: Schema) -> None:
    other_schema = table_schema_nested.as_arrow()
    other_schema = other_schema.remove(6).insert(
        6,
        pa.field(
            "person",
            pa.struct(
                [
                    pa.field("name", pa.string(), nullable=True),
                ]
            ),
            nullable=True,
        ),
    )
    expected = """Mismatch in fields:
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃    ┃ Table field                        ┃ Dataframe field                    ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ │ 1: foo: optional string            │ 1: foo: optional string            │
│ ✅ │ 2: bar: required int               │ 2: bar: required int               │
│ ✅ │ 3: baz: optional boolean           │ 3: baz: optional boolean           │
│ ✅ │ 4: qux: required list<string>      │ 4: qux: required list<string>      │
│ ✅ │ 5: element: required string        │ 5: element: required string        │
│ ✅ │ 6: quux: required map<string,      │ 6: quux: required map<string,      │
│    │ map<string, int>>                  │ map<string, int>>                  │
│ ✅ │ 7: key: required string            │ 7: key: required string            │
│ ✅ │ 8: value: required map<string,     │ 8: value: required map<string,     │
│    │ int>                               │ int>                               │
│ ✅ │ 9: key: required string            │ 9: key: required string            │
│ ✅ │ 10: value: required int            │ 10: value: required int            │
│ ✅ │ 11: location: required             │ 11: location: required             │
│    │ list<struct<13: latitude: optional │ list<struct<13: latitude: optional │
│    │ float, 14: longitude: optional     │ float, 14: longitude: optional     │
│    │ float>>                            │ float>>                            │
│ ✅ │ 12: element: required struct<13:   │ 12: element: required struct<13:   │
│    │ latitude: optional float, 14:      │ latitude: optional float, 14:      │
│    │ longitude: optional float>         │ longitude: optional float>         │
│ ✅ │ 13: latitude: optional float       │ 13: latitude: optional float       │
│ ✅ │ 14: longitude: optional float      │ 14: longitude: optional float      │
│ ✅ │ 15: person: optional struct<16:    │ 15: person: optional struct<16:    │
│    │ name: optional string, 17: age:    │ name: optional string>             │
│    │ required int>                      │                                    │
│ ✅ │ 16: name: optional string          │ 16: name: optional string          │
│ ❌ │ 17: age: required int              │ Missing                            │
└────┴────────────────────────────────────┴────────────────────────────────────┘
"""

    with pytest.raises(ValueError, match=expected):
        _check_pyarrow_schema_compatible(table_schema_nested, other_schema)


def test_schema_compatible_nested(table_schema_nested: Schema) -> None:
    try:
        _check_pyarrow_schema_compatible(table_schema_nested, table_schema_nested.as_arrow())
    except Exception:
        pytest.fail("Unexpected Exception raised when calling `_check_pyarrow_schema_compatible`")


def test_schema_mismatch_additional_field(table_schema_simple: Schema) -> None:
    other_schema = pa.schema(
        (
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=False),
            pa.field("baz", pa.bool_(), nullable=True),
            pa.field("new_field", pa.date32(), nullable=True),
        )
    )

    with pytest.raises(
        ValueError, match=r"PyArrow table contains more columns: new_field. Update the schema first \(hint, use union_by_name\)."
    ):
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)


def test_schema_compatible(table_schema_simple: Schema) -> None:
    try:
        _check_pyarrow_schema_compatible(table_schema_simple, table_schema_simple.as_arrow())
    except Exception:
        pytest.fail("Unexpected Exception raised when calling `_check_pyarrow_schema_compatible`")


def test_schema_projection(table_schema_simple: Schema) -> None:
    # remove optional `baz` field from `table_schema_simple`
    other_schema = pa.schema(
        (
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=False),
        )
    )
    try:
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)
    except Exception:
        pytest.fail("Unexpected Exception raised when calling `_check_pyarrow_schema_compatible`")


def test_schema_downcast(table_schema_simple: Schema) -> None:
    # large_string type is compatible with string type
    other_schema = pa.schema(
        (
            pa.field("foo", pa.large_string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=False),
            pa.field("baz", pa.bool_(), nullable=True),
        )
    )

    try:
        _check_pyarrow_schema_compatible(table_schema_simple, other_schema)
    except Exception:
        pytest.fail("Unexpected Exception raised when calling `_check_pyarrow_schema_compatible`")


def test_partition_for_demo() -> None:
    test_pa_schema = pa.schema([("year", pa.int64()), ("n_legs", pa.int64()), ("animal", pa.string())])
    test_schema = Schema(
        NestedField(field_id=1, name="year", field_type=StringType(), required=False),
        NestedField(field_id=2, name="n_legs", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="animal", field_type=StringType(), required=False),
        schema_id=1,
    )
    test_data = {
        "year": [2020, 2022, 2022, 2022, 2021, 2022, 2022, 2019, 2021],
        "n_legs": [2, 2, 2, 4, 4, 4, 4, 5, 100],
        "animal": ["Flamingo", "Parrot", "Parrot", "Horse", "Dog", "Horse", "Horse", "Brittle stars", "Centipede"],
    }
    arrow_table = pa.Table.from_pydict(test_data, schema=test_pa_schema)
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1002, transform=IdentityTransform(), name="n_legs_identity"),
        PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="year_identity"),
    )
    result = _determine_partitions(partition_spec, test_schema, arrow_table)
    assert {table_partition.partition_key.partition for table_partition in result} == {
        Record(2, 2020),
        Record(100, 2021),
        Record(4, 2021),
        Record(4, 2022),
        Record(2, 2022),
        Record(5, 2019),
    }
    assert (
        pa.concat_tables([table_partition.arrow_table_partition for table_partition in result]).num_rows == arrow_table.num_rows
    )


def test_partition_for_nested_field() -> None:
    schema = Schema(
        NestedField(id=1, name="foo", field_type=StringType(), required=True),
        NestedField(
            id=2,
            name="bar",
            field_type=StructType(
                NestedField(id=3, name="baz", field_type=TimestampType(), required=False),
                NestedField(id=4, name="qux", field_type=IntegerType(), required=False),
            ),
            required=True,
        ),
    )

    spec = PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=HourTransform(), name="ts"))

    t1 = datetime(2025, 7, 11, 9, 30, 0)
    t2 = datetime(2025, 7, 11, 10, 30, 0)

    test_data = [
        {"foo": "a", "bar": {"baz": t1, "qux": 1}},
        {"foo": "b", "bar": {"baz": t2, "qux": 2}},
    ]

    arrow_table = pa.Table.from_pylist(test_data, schema=schema.as_arrow())
    partitions = _determine_partitions(spec, schema, arrow_table)
    partition_values = {p.partition_key.partition[0] for p in partitions}

    assert partition_values == {486729, 486730}


def test_partition_for_deep_nested_field() -> None:
    schema = Schema(
        NestedField(
            id=1,
            name="foo",
            field_type=StructType(
                NestedField(
                    id=2,
                    name="bar",
                    field_type=StructType(NestedField(id=3, name="baz", field_type=StringType(), required=False)),
                    required=True,
                )
            ),
            required=True,
        )
    )

    spec = PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="qux"))

    test_data = [
        {"foo": {"bar": {"baz": "data-1"}}},
        {"foo": {"bar": {"baz": "data-2"}}},
        {"foo": {"bar": {"baz": "data-1"}}},
    ]

    arrow_table = pa.Table.from_pylist(test_data, schema=schema.as_arrow())
    partitions = _determine_partitions(spec, schema, arrow_table)

    assert len(partitions) == 2  # 2 unique partitions
    partition_values = {p.partition_key.partition[0] for p in partitions}
    assert partition_values == {"data-1", "data-2"}


def test_inspect_partition_for_nested_field(catalog: InMemoryCatalog) -> None:
    schema = Schema(
        NestedField(id=1, name="foo", field_type=StringType(), required=True),
        NestedField(
            id=2,
            name="bar",
            field_type=StructType(
                NestedField(id=3, name="baz", field_type=StringType(), required=False),
                NestedField(id=4, name="qux", field_type=IntegerType(), required=False),
            ),
            required=True,
        ),
    )
    spec = PartitionSpec(PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="part"))
    catalog.create_namespace("default")
    table = catalog.create_table("default.test_partition_in_struct", schema=schema, partition_spec=spec)
    test_data = [
        {"foo": "a", "bar": {"baz": "data-a", "qux": 1}},
        {"foo": "b", "bar": {"baz": "data-b", "qux": 2}},
    ]

    arrow_table = pa.Table.from_pylist(test_data, schema=table.schema().as_arrow())
    table.append(arrow_table)
    partitions_table = table.inspect.partitions()
    partitions = partitions_table["partition"].to_pylist()

    assert len(partitions) == 2
    assert {part["part"] for part in partitions} == {"data-a", "data-b"}


def test_identity_partition_on_multi_columns() -> None:
    test_pa_schema = pa.schema([("born_year", pa.int64()), ("n_legs", pa.int64()), ("animal", pa.string())])
    test_schema = Schema(
        NestedField(field_id=1, name="born_year", field_type=StringType(), required=False),
        NestedField(field_id=2, name="n_legs", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="animal", field_type=StringType(), required=False),
        schema_id=1,
    )
    # 5 partitions, 6 unique row values, 12 rows
    test_rows = [
        (2021, 4, "Dog"),
        (2022, 4, "Horse"),
        (2022, 4, "Another Horse"),
        (2021, 100, "Centipede"),
        (None, 4, "Kirin"),
        (2021, None, "Fish"),
    ] * 2
    expected = {Record(test_rows[i][1], test_rows[i][0]) for i in range(len(test_rows))}
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1002, transform=IdentityTransform(), name="n_legs_identity"),
        PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="year_identity"),
    )
    import random

    # there are 12! / ((2!)^6) = 7,484,400 permutations, too many to pick all
    for _ in range(1000):
        random.shuffle(test_rows)
        test_data = {
            "born_year": [row[0] for row in test_rows],
            "n_legs": [row[1] for row in test_rows],
            "animal": [row[2] for row in test_rows],
        }
        arrow_table = pa.Table.from_pydict(test_data, schema=test_pa_schema)

        result = _determine_partitions(partition_spec, test_schema, arrow_table)

        assert {table_partition.partition_key.partition for table_partition in result} == expected
        concatenated_arrow_table = pa.concat_tables([table_partition.arrow_table_partition for table_partition in result])
        assert concatenated_arrow_table.num_rows == arrow_table.num_rows
        assert concatenated_arrow_table.sort_by(
            [
                ("born_year", "ascending"),
                ("n_legs", "ascending"),
                ("animal", "ascending"),
            ]
        ) == arrow_table.sort_by([("born_year", "ascending"), ("n_legs", "ascending"), ("animal", "ascending")])


def test_initial_value() -> None:
    # Have some fake data, otherwise it will generate a table without records
    data = pa.record_batch([pa.nulls(10, pa.int64())], names=["some_field"])
    result = _to_requested_schema(
        Schema(NestedField(1, "we-love-22", LongType(), required=True, initial_default=22)), Schema(), data
    )
    assert result.column_names == ["we-love-22"]
    for val in result[0]:
        assert val.as_py() == 22


def test__to_requested_schema_timestamp_to_timestamptz_projection() -> None:
    # file is written with timestamp without timezone
    file_schema = Schema(NestedField(1, "ts_field", TimestampType(), required=False))
    batch = pa.record_batch(
        [
            pa.array(
                [
                    datetime(2025, 8, 14, 12, 0, 0),
                    datetime(2025, 8, 14, 13, 0, 0),
                ],
                type=pa.timestamp("us"),
            )
        ],
        names=["ts_field"],
    )

    # table is written with timestamp with timezone
    table_schema = Schema(NestedField(1, "ts_field", TimestamptzType(), required=False))

    actual_result = _to_requested_schema(table_schema, file_schema, batch, downcast_ns_timestamp_to_us=True)
    expected = pa.record_batch(
        [
            pa.array(
                [
                    datetime(2025, 8, 14, 12, 0, 0),
                    datetime(2025, 8, 14, 13, 0, 0),
                ],
                type=pa.timestamp("us", tz=timezone.utc),
            )
        ],
        names=["ts_field"],
    )

    # expect actual_result to have timezone
    assert expected.equals(actual_result)


def test__to_requested_schema_timestamps(
    arrow_table_schema_with_all_timestamp_precisions: pa.Schema,
    arrow_table_with_all_timestamp_precisions: pa.Table,
    arrow_table_schema_with_all_microseconds_timestamp_precisions: pa.Schema,
    table_schema_with_all_microseconds_timestamp_precision: Schema,
) -> None:
    requested_schema = table_schema_with_all_microseconds_timestamp_precision
    file_schema = requested_schema
    batch = arrow_table_with_all_timestamp_precisions.to_batches()[0]
    result = _to_requested_schema(requested_schema, file_schema, batch, downcast_ns_timestamp_to_us=True, include_field_ids=False)

    expected = arrow_table_with_all_timestamp_precisions.cast(
        arrow_table_schema_with_all_microseconds_timestamp_precisions, safe=False
    ).to_batches()[0]
    assert result == expected


def test__to_requested_schema_timestamps_without_downcast_raises_exception(
    arrow_table_schema_with_all_timestamp_precisions: pa.Schema,
    arrow_table_with_all_timestamp_precisions: pa.Table,
    arrow_table_schema_with_all_microseconds_timestamp_precisions: pa.Schema,
    table_schema_with_all_microseconds_timestamp_precision: Schema,
) -> None:
    requested_schema = table_schema_with_all_microseconds_timestamp_precision
    file_schema = requested_schema
    batch = arrow_table_with_all_timestamp_precisions.to_batches()[0]
    with pytest.raises(ValueError) as exc_info:
        _to_requested_schema(requested_schema, file_schema, batch, downcast_ns_timestamp_to_us=False, include_field_ids=False)

    assert "Unsupported schema projection from timestamp[ns] to timestamp[us]" in str(exc_info.value)


def test_pyarrow_file_io_fs_by_scheme_cache() -> None:
    # It's better to set up multi-region minio servers for an integration test once `endpoint_url` argument becomes available for `resolve_s3_region`
    # Refer to: https://github.com/apache/arrow/issues/43713

    pyarrow_file_io = PyArrowFileIO()
    us_east_1_region = "us-east-1"
    ap_southeast_2_region = "ap-southeast-2"

    with patch("pyarrow.fs.resolve_s3_region") as mock_s3_region_resolver:
        # Call with new argument resolves region automatically
        mock_s3_region_resolver.return_value = us_east_1_region
        filesystem_us = pyarrow_file_io.fs_by_scheme("s3", "us-east-1-bucket")
        assert filesystem_us.region == us_east_1_region
        assert pyarrow_file_io.fs_by_scheme.cache_info().misses == 1  # type: ignore
        assert pyarrow_file_io.fs_by_scheme.cache_info().currsize == 1  # type: ignore

        # Call with different argument also resolves region automatically
        mock_s3_region_resolver.return_value = ap_southeast_2_region
        filesystem_ap_southeast_2 = pyarrow_file_io.fs_by_scheme("s3", "ap-southeast-2-bucket")
        assert filesystem_ap_southeast_2.region == ap_southeast_2_region
        assert pyarrow_file_io.fs_by_scheme.cache_info().misses == 2  # type: ignore
        assert pyarrow_file_io.fs_by_scheme.cache_info().currsize == 2  # type: ignore

        # Call with same argument hits cache
        filesystem_us_cached = pyarrow_file_io.fs_by_scheme("s3", "us-east-1-bucket")
        assert filesystem_us_cached.region == us_east_1_region
        assert pyarrow_file_io.fs_by_scheme.cache_info().hits == 1  # type: ignore

        # Call with same argument hits cache
        filesystem_ap_southeast_2_cached = pyarrow_file_io.fs_by_scheme("s3", "ap-southeast-2-bucket")
        assert filesystem_ap_southeast_2_cached.region == ap_southeast_2_region
        assert pyarrow_file_io.fs_by_scheme.cache_info().hits == 2  # type: ignore


def test_pyarrow_io_new_input_multi_region(caplog: Any) -> None:
    # It's better to set up multi-region minio servers for an integration test once `endpoint_url` argument becomes available for `resolve_s3_region`
    # Refer to: https://github.com/apache/arrow/issues/43713
    user_provided_region = "ap-southeast-1"
    bucket_regions = [
        ("us-east-2-bucket", "us-east-2"),
        ("ap-southeast-2-bucket", "ap-southeast-2"),
    ]

    def _s3_region_map(bucket: str) -> str:
        for bucket_region in bucket_regions:
            if bucket_region[0] == bucket:
                return bucket_region[1]
        raise OSError("Unknown bucket")

    # For a pyarrow io instance with configured default s3 region
    pyarrow_file_io = PyArrowFileIO({"s3.region": user_provided_region, "s3.resolve-region": "true"})
    with patch("pyarrow.fs.resolve_s3_region") as mock_s3_region_resolver:
        mock_s3_region_resolver.side_effect = _s3_region_map

        # The region is set to provided region if bucket region cannot be resolved
        with caplog.at_level(logging.WARNING):
            assert pyarrow_file_io.new_input("s3://non-exist-bucket/path/to/file")._filesystem.region == user_provided_region
        assert "Unable to resolve region for bucket non-exist-bucket" in caplog.text

        for bucket_region in bucket_regions:
            # For s3 scheme, region is overwritten by resolved bucket region if different from user provided region
            with caplog.at_level(logging.WARNING):
                assert pyarrow_file_io.new_input(f"s3://{bucket_region[0]}/path/to/file")._filesystem.region == bucket_region[1]
            assert (
                f"PyArrow FileIO overriding S3 bucket region for bucket {bucket_region[0]}: "
                f"provided region {user_provided_region}, actual region {bucket_region[1]}" in caplog.text
            )

            # For oss scheme, user provided region is used instead
            assert pyarrow_file_io.new_input(f"oss://{bucket_region[0]}/path/to/file")._filesystem.region == user_provided_region


def test_pyarrow_io_multi_fs() -> None:
    pyarrow_file_io = PyArrowFileIO({"s3.region": "ap-southeast-1"})

    with patch("pyarrow.fs.resolve_s3_region") as mock_s3_region_resolver:
        mock_s3_region_resolver.return_value = None

        # The PyArrowFileIO instance resolves s3 file input to S3FileSystem
        assert isinstance(pyarrow_file_io.new_input("s3://bucket/path/to/file")._filesystem, S3FileSystem)

        # Same PyArrowFileIO instance resolves local file input to LocalFileSystem
        assert isinstance(pyarrow_file_io.new_input("file:///path/to/file")._filesystem, LocalFileSystem)


class SomeRetryStrategy(AwsDefaultS3RetryStrategy):
    def __init__(self) -> None:
        super().__init__()
        warnings.warn("Initialized SomeRetryStrategy 👍")


def test_retry_strategy() -> None:
    io = PyArrowFileIO(properties={S3_RETRY_STRATEGY_IMPL: "tests.io.test_pyarrow.SomeRetryStrategy"})
    with pytest.warns(UserWarning, match="Initialized SomeRetryStrategy.*"):
        io.new_input("s3://bucket/path/to/file")


def test_retry_strategy_not_found() -> None:
    io = PyArrowFileIO(properties={S3_RETRY_STRATEGY_IMPL: "pyiceberg.DoesNotExist"})
    with pytest.warns(UserWarning, match="Could not initialize S3 retry strategy: pyiceberg.DoesNotExist"):
        io.new_input("s3://bucket/path/to/file")


@pytest.mark.parametrize("format_version", [1, 2, 3])
def test_task_to_record_batches_nanos(format_version: TableVersion, tmpdir: str) -> None:
    arrow_table = pa.table(
        [
            pa.array(
                [
                    datetime(2025, 8, 14, 12, 0, 0),
                    datetime(2025, 8, 14, 13, 0, 0),
                ],
                type=pa.timestamp("ns"),
            )
        ],
        pa.schema((pa.field("ts_field", pa.timestamp("ns"), nullable=True, metadata={PYARROW_PARQUET_FIELD_ID_KEY: "1"}),)),
    )

    data_file = _write_table_to_data_file(f"{tmpdir}/test_task_to_record_batches_nanos.parquet", arrow_table.schema, arrow_table)

    if format_version <= 2:
        table_schema = Schema(NestedField(1, "ts_field", TimestampType(), required=False))
    else:
        table_schema = Schema(NestedField(1, "ts_field", TimestampNanoType(), required=False))

    actual_result = list(
        _task_to_record_batches(
            PyArrowFileIO(),
            FileScanTask(data_file),
            bound_row_filter=AlwaysTrue(),
            projected_schema=table_schema,
            projected_field_ids={1},
            positional_deletes=None,
            case_sensitive=True,
            format_version=format_version,
        )
    )[0]

    def _expected_batch(unit: str) -> pa.RecordBatch:
        return pa.record_batch(
            [
                pa.array(
                    [
                        datetime(2025, 8, 14, 12, 0, 0),
                        datetime(2025, 8, 14, 13, 0, 0),
                    ],
                    type=pa.timestamp(unit),
                )
            ],
            names=["ts_field"],
        )

    assert _expected_batch("ns" if format_version > 2 else "us").equals(actual_result)


def test_parse_location_defaults() -> None:
    """Test that parse_location uses defaults."""

    from pyiceberg.io.pyarrow import PyArrowFileIO

    # if no default scheme or netloc is provided, use file scheme and empty netloc
    scheme, netloc, path = PyArrowFileIO.parse_location("/foo/bar")
    assert scheme == "file"
    assert netloc == ""
    assert path == "/foo/bar"

    scheme, netloc, path = PyArrowFileIO.parse_location(
        "/foo/bar", properties={"DEFAULT_SCHEME": "scheme", "DEFAULT_NETLOC": "netloc:8000"}
    )
    assert scheme == "scheme"
    assert netloc == "netloc:8000"
    assert path == "/foo/bar"

    scheme, netloc, path = PyArrowFileIO.parse_location(
        "/foo/bar", properties={"DEFAULT_SCHEME": "hdfs", "DEFAULT_NETLOC": "netloc:8000"}
    )
    assert scheme == "hdfs"
    assert netloc == "netloc:8000"
    assert path == "/foo/bar"
