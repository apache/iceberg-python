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
# pylint: disable=redefined-outer-name,arguments-renamed,fixme
"""FileIO implementation for reading and writing table files that uses pyarrow.fs.

This file contains a FileIO implementation that relies on the filesystem interface provided
by PyArrow. It relies on PyArrow's `from_uri` method that infers the correct filesystem
type to use. Theoretically, this allows the supported storage types to grow naturally
with the pyarrow library.
"""

from __future__ import annotations

import concurrent.futures
import fnmatch
import itertools
import logging
import os
import re
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache, singledispatch
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import urlparse

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.lib
import pyarrow.parquet as pq
from pyarrow import ChunkedArray
from pyarrow.fs import (
    FileInfo,
    FileSystem,
    FileType,
    FSSpecHandler,
)
from sortedcontainers import SortedList

from pyiceberg.avro.resolver import ResolveError
from pyiceberg.conversions import to_bytes
from pyiceberg.expressions import (
    AlwaysTrue,
    BooleanExpression,
    BoundTerm,
    Literal,
)
from pyiceberg.expressions.visitors import (
    BoundBooleanExpressionVisitor,
    bind,
    extract_field_ids,
    translate_column_names,
)
from pyiceberg.expressions.visitors import visit as boolean_expression_visit
from pyiceberg.io import (
    GCS_DEFAULT_LOCATION,
    GCS_ENDPOINT,
    GCS_TOKEN,
    GCS_TOKEN_EXPIRES_AT_MS,
    HDFS_HOST,
    HDFS_KERB_TICKET,
    HDFS_PORT,
    HDFS_USER,
    S3_ACCESS_KEY_ID,
    S3_CONNECT_TIMEOUT,
    S3_ENDPOINT,
    S3_PROXY_URI,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
)
from pyiceberg.schema import (
    PartnerAccessor,
    PreOrderSchemaVisitor,
    Schema,
    SchemaVisitorPerPrimitiveType,
    SchemaWithPartnerVisitor,
    pre_order_visit,
    promote,
    prune_columns,
    sanitize_column_names,
    visit,
    visit_with_partner,
)
from pyiceberg.table import PropertyUtil, TableProperties, WriteTask
from pyiceberg.table.name_mapping import NameMapping
from pyiceberg.transforms import TruncateTransform
from pyiceberg.typedef import EMPTY_DICT, Properties, Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.datetime import millis_to_datetime
from pyiceberg.utils.singleton import Singleton
from pyiceberg.utils.truncate import truncate_upper_bound_binary_string, truncate_upper_bound_text_string

if TYPE_CHECKING:
    from pyiceberg.table import FileScanTask, Table

logger = logging.getLogger(__name__)

ONE_MEGABYTE = 1024 * 1024
BUFFER_SIZE = "buffer-size"
ICEBERG_SCHEMA = b"iceberg.schema"
# The PARQUET: in front means that it is Parquet specific, in this case the field_id
PYARROW_PARQUET_FIELD_ID_KEY = b"PARQUET:field_id"
PYARROW_FIELD_DOC_KEY = b"doc"
LIST_ELEMENT_NAME = "element"
MAP_KEY_NAME = "key"
MAP_VALUE_NAME = "value"
DOC = "doc"

T = TypeVar("T")


class PyArrowLocalFileSystem(pyarrow.fs.LocalFileSystem):
    def open_output_stream(self, path: str, *args: Any, **kwargs: Any) -> pyarrow.NativeFile:
        # In LocalFileSystem, parent directories must be first created before opening an output stream
        self.create_dir(os.path.dirname(path), recursive=True)
        return super().open_output_stream(path, *args, **kwargs)


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation that uses a pyarrow filesystem to generate pyarrow.lib.NativeFile instances.

    Args:
        location (str): A URI or a path to a local file.

    Attributes:
        location(str): The URI or path to a local file for a PyArrowFile instance.

    Examples:
        >>> from pyiceberg.io.pyarrow import PyArrowFile
        >>> # input_file = PyArrowFile("s3://foo/bar.txt")
        >>> # Read the contents of the PyArrowFile instance
        >>> # Make sure that you have permissions to read/write
        >>> # file_content = input_file.open().read()

        >>> # output_file = PyArrowFile("s3://baz/qux.txt")
        >>> # Write bytes to a file
        >>> # Make sure that you have permissions to read/write
        >>> # output_file.create().write(b'foobytes')
    """

    _fs: FileSystem
    _path: str
    _buffer_size: int

    def __init__(self, location: str, path: str, fs: FileSystem, buffer_size: int = ONE_MEGABYTE):
        self._filesystem = fs
        self._path = path
        self._buffer_size = buffer_size
        super().__init__(location=location)

    def _file_info(self) -> FileInfo:
        """Retrieve a pyarrow.fs.FileInfo object for the location.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15.
        """
        try:
            file_info = self._filesystem.get_file_info(self._path)
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot get file info, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error

        if file_info.type == FileType.NotFound:
            raise FileNotFoundError(f"Cannot get file info, file not found: {self.location}")
        return file_info

    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        file_info = self._file_info()
        return file_info.size

    def exists(self) -> bool:
        """Check whether the location exists."""
        try:
            self._file_info()  # raises FileNotFoundError if it does not exist
            return True
        except FileNotFoundError:
            return False

    def open(self, seekable: bool = True) -> InputStream:
        """Open the location using a PyArrow FileSystem inferred from the location.

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential.

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at `self.location`.

        Raises:
            FileNotFoundError: If the file at self.location does not exist.
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15.
        """
        try:
            if seekable:
                input_file = self._filesystem.open_input_file(self._path)
            else:
                input_file = self._filesystem.open_input_stream(self._path, buffer_size=self._buffer_size)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot open file, does not exist: {self.location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot open file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return input_file

    def create(self, overwrite: bool = False) -> OutputStream:
        """Create a writable pyarrow.lib.NativeFile for this PyArrowFile's location.

        Args:
            overwrite (bool): Whether to overwrite the file if it already exists.

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at self.location.

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False.

        Note:
            This retrieves a pyarrow NativeFile by opening an output stream. If overwrite is set to False,
            a check is first performed to verify that the file does not exist. This is not thread-safe and
            a possibility does exist that the file can be created by a concurrent process after the existence
            check yet before the output stream is created. In such a case, the default pyarrow behavior will
            truncate the contents of the existing file when opening the output stream.
        """
        try:
            if not overwrite and self.exists() is True:
                raise FileExistsError(f"Cannot create file, already exists: {self.location}")
            output_file = self._filesystem.open_output_stream(self._path, buffer_size=self._buffer_size)
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot create file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return output_file

    def to_input_file(self) -> PyArrowFile:
        """Return a new PyArrowFile for the location of an existing PyArrowFile instance.

        This method is included to abide by the OutputFile abstract base class. Since this implementation uses a single
        PyArrowFile class (as opposed to separate InputFile and OutputFile implementations), this method effectively returns
        a copy of the same instance.
        """
        return self


class PyArrowFileIO(FileIO):
    fs_by_scheme: Callable[[str, Optional[str]], FileSystem]

    def __init__(self, properties: Properties = EMPTY_DICT):
        self.fs_by_scheme: Callable[[str, Optional[str]], FileSystem] = lru_cache(self._initialize_fs)
        super().__init__(properties=properties)

    @staticmethod
    def parse_location(location: str) -> Tuple[str, str, str]:
        """Return the path without the scheme."""
        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        elif uri.scheme == "hdfs":
            return uri.scheme, uri.netloc, location
        else:
            return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"

    def _initialize_fs(self, scheme: str, netloc: Optional[str] = None) -> FileSystem:
        if scheme in {"s3", "s3a", "s3n"}:
            from pyarrow.fs import S3FileSystem

            client_kwargs: Dict[str, Any] = {
                "endpoint_override": self.properties.get(S3_ENDPOINT),
                "access_key": self.properties.get(S3_ACCESS_KEY_ID),
                "secret_key": self.properties.get(S3_SECRET_ACCESS_KEY),
                "session_token": self.properties.get(S3_SESSION_TOKEN),
                "region": self.properties.get(S3_REGION),
            }

            if proxy_uri := self.properties.get(S3_PROXY_URI):
                client_kwargs["proxy_options"] = proxy_uri

            if connect_timeout := self.properties.get(S3_CONNECT_TIMEOUT):
                client_kwargs["connect_timeout"] = float(connect_timeout)

            return S3FileSystem(**client_kwargs)
        elif scheme == "hdfs":
            from pyarrow.fs import HadoopFileSystem

            hdfs_kwargs: Dict[str, Any] = {}
            if netloc:
                return HadoopFileSystem.from_uri(f"hdfs://{netloc}")
            if host := self.properties.get(HDFS_HOST):
                hdfs_kwargs["host"] = host
            if port := self.properties.get(HDFS_PORT):
                # port should be an integer type
                hdfs_kwargs["port"] = int(port)
            if user := self.properties.get(HDFS_USER):
                hdfs_kwargs["user"] = user
            if kerb_ticket := self.properties.get(HDFS_KERB_TICKET):
                hdfs_kwargs["kerb_ticket"] = kerb_ticket

            return HadoopFileSystem(**hdfs_kwargs)
        elif scheme in {"gs", "gcs"}:
            from pyarrow.fs import GcsFileSystem

            gcs_kwargs: Dict[str, Any] = {}
            if access_token := self.properties.get(GCS_TOKEN):
                gcs_kwargs["access_token"] = access_token
            if expiration := self.properties.get(GCS_TOKEN_EXPIRES_AT_MS):
                gcs_kwargs["credential_token_expiration"] = millis_to_datetime(int(expiration))
            if bucket_location := self.properties.get(GCS_DEFAULT_LOCATION):
                gcs_kwargs["default_bucket_location"] = bucket_location
            if endpoint := self.properties.get(GCS_ENDPOINT):
                url_parts = urlparse(endpoint)
                gcs_kwargs["scheme"] = url_parts.scheme
                gcs_kwargs["endpoint_override"] = url_parts.netloc

            return GcsFileSystem(**gcs_kwargs)
        elif scheme == "file":
            return PyArrowLocalFileSystem()
        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    def new_input(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to read bytes from the file at the given location.

        Args:
            location (str): A URI or a path to a local file.

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location.
        """
        scheme, netloc, path = self.parse_location(location)
        return PyArrowFile(
            fs=self.fs_by_scheme(scheme, netloc),
            location=location,
            path=path,
            buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)),
        )

    def new_output(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to write bytes to the file at the given location.

        Args:
            location (str): A URI or a path to a local file.

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location.
        """
        scheme, netloc, path = self.parse_location(location)
        return PyArrowFile(
            fs=self.fs_by_scheme(scheme, netloc),
            location=location,
            path=path,
            buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)),
        )

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location.

        Args:
            location (Union[str, InputFile, OutputFile]): The URI to the file--if an InputFile instance or an OutputFile instance is provided,
                the location attribute for that instance is used as the location to delete.

        Raises:
            FileNotFoundError: When the file at the provided location does not exist.
            PermissionError: If the file at the provided location cannot be accessed due to a permission error such as
                an AWS error code 15.
        """
        str_location = location.location if isinstance(location, (InputFile, OutputFile)) else location
        scheme, netloc, path = self.parse_location(str_location)
        fs = self.fs_by_scheme(scheme, netloc)

        try:
            fs.delete_file(path)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot delete file, does not exist: {location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot delete file, access denied: {location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error


def schema_to_pyarrow(schema: Union[Schema, IcebergType], metadata: Dict[bytes, bytes] = EMPTY_DICT) -> pa.schema:
    return visit(schema, _ConvertToArrowSchema(metadata))


class _ConvertToArrowSchema(SchemaVisitorPerPrimitiveType[pa.DataType]):
    _metadata: Dict[bytes, bytes]

    def __init__(self, metadata: Dict[bytes, bytes] = EMPTY_DICT) -> None:
        self._metadata = metadata

    def schema(self, _: Schema, struct_result: pa.StructType) -> pa.schema:
        return pa.schema(list(struct_result), metadata=self._metadata)

    def struct(self, _: StructType, field_results: List[pa.DataType]) -> pa.DataType:
        return pa.struct(field_results)

    def field(self, field: NestedField, field_result: pa.DataType) -> pa.Field:
        return pa.field(
            name=field.name,
            type=field_result,
            nullable=field.optional,
            metadata={PYARROW_FIELD_DOC_KEY: field.doc, PYARROW_PARQUET_FIELD_ID_KEY: str(field.field_id)}
            if field.doc
            else {PYARROW_PARQUET_FIELD_ID_KEY: str(field.field_id)},
        )

    def list(self, list_type: ListType, element_result: pa.DataType) -> pa.DataType:
        element_field = self.field(list_type.element_field, element_result)
        return pa.list_(value_type=element_field)

    def map(self, map_type: MapType, key_result: pa.DataType, value_result: pa.DataType) -> pa.DataType:
        key_field = self.field(map_type.key_field, key_result)
        value_field = self.field(map_type.value_field, value_result)
        return pa.map_(key_type=key_field, item_type=value_field)

    def visit_fixed(self, fixed_type: FixedType) -> pa.DataType:
        return pa.binary(len(fixed_type))

    def visit_decimal(self, decimal_type: DecimalType) -> pa.DataType:
        return pa.decimal128(decimal_type.precision, decimal_type.scale)

    def visit_boolean(self, _: BooleanType) -> pa.DataType:
        return pa.bool_()

    def visit_integer(self, _: IntegerType) -> pa.DataType:
        return pa.int32()

    def visit_long(self, _: LongType) -> pa.DataType:
        return pa.int64()

    def visit_float(self, _: FloatType) -> pa.DataType:
        # 32-bit IEEE 754 floating point
        return pa.float32()

    def visit_double(self, _: DoubleType) -> pa.DataType:
        # 64-bit IEEE 754 floating point
        return pa.float64()

    def visit_date(self, _: DateType) -> pa.DataType:
        # Date encoded as an int
        return pa.date32()

    def visit_time(self, _: TimeType) -> pa.DataType:
        return pa.time64("us")

    def visit_timestamp(self, _: TimestampType) -> pa.DataType:
        return pa.timestamp(unit="us")

    def visit_timestamptz(self, _: TimestamptzType) -> pa.DataType:
        return pa.timestamp(unit="us", tz="UTC")

    def visit_string(self, _: StringType) -> pa.DataType:
        return pa.string()

    def visit_uuid(self, _: UUIDType) -> pa.DataType:
        return pa.binary(16)

    def visit_binary(self, _: BinaryType) -> pa.DataType:
        return pa.large_binary()


def _convert_scalar(value: Any, iceberg_type: IcebergType) -> pa.scalar:
    if not isinstance(iceberg_type, PrimitiveType):
        raise ValueError(f"Expected primitive type, got: {iceberg_type}")
    return pa.scalar(value=value, type=schema_to_pyarrow(iceberg_type))


class _ConvertToArrowExpression(BoundBooleanExpressionVisitor[pc.Expression]):
    def visit_in(self, term: BoundTerm[pc.Expression], literals: Set[Any]) -> pc.Expression:
        pyarrow_literals = pa.array(literals, type=schema_to_pyarrow(term.ref().field.field_type))
        return pc.field(term.ref().field.name).isin(pyarrow_literals)

    def visit_not_in(self, term: BoundTerm[pc.Expression], literals: Set[Any]) -> pc.Expression:
        pyarrow_literals = pa.array(literals, type=schema_to_pyarrow(term.ref().field.field_type))
        return ~pc.field(term.ref().field.name).isin(pyarrow_literals)

    def visit_is_nan(self, term: BoundTerm[Any]) -> pc.Expression:
        ref = pc.field(term.ref().field.name)
        return pc.is_nan(ref)

    def visit_not_nan(self, term: BoundTerm[Any]) -> pc.Expression:
        ref = pc.field(term.ref().field.name)
        return ~pc.is_nan(ref)

    def visit_is_null(self, term: BoundTerm[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name).is_null(nan_is_null=False)

    def visit_not_null(self, term: BoundTerm[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name).is_valid()

    def visit_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) == _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_not_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) != _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_greater_than_or_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) >= _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_greater_than(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) > _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_less_than(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) < _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_less_than_or_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) <= _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_starts_with(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.starts_with(pc.field(term.ref().field.name), literal.value)

    def visit_not_starts_with(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return ~pc.starts_with(pc.field(term.ref().field.name), literal.value)

    def visit_true(self) -> pc.Expression:
        return pc.scalar(True)

    def visit_false(self) -> pc.Expression:
        return pc.scalar(False)

    def visit_not(self, child_result: pc.Expression) -> pc.Expression:
        return ~child_result

    def visit_and(self, left_result: pc.Expression, right_result: pc.Expression) -> pc.Expression:
        return left_result & right_result

    def visit_or(self, left_result: pc.Expression, right_result: pc.Expression) -> pc.Expression:
        return left_result | right_result


def expression_to_pyarrow(expr: BooleanExpression) -> pc.Expression:
    return boolean_expression_visit(expr, _ConvertToArrowExpression())


@lru_cache
def _get_file_format(file_format: FileFormat, **kwargs: Dict[str, Any]) -> ds.FileFormat:
    if file_format == FileFormat.PARQUET:
        return ds.ParquetFileFormat(**kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def _construct_fragment(fs: FileSystem, data_file: DataFile, file_format_kwargs: Dict[str, Any] = EMPTY_DICT) -> ds.Fragment:
    _, _, path = PyArrowFileIO.parse_location(data_file.file_path)
    return _get_file_format(data_file.file_format, **file_format_kwargs).make_fragment(path, fs)


def _read_deletes(fs: FileSystem, data_file: DataFile) -> Dict[str, pa.ChunkedArray]:
    delete_fragment = _construct_fragment(
        fs, data_file, file_format_kwargs={"dictionary_columns": ("file_path",), "pre_buffer": True, "buffer_size": ONE_MEGABYTE}
    )
    table = ds.Scanner.from_fragment(fragment=delete_fragment).to_table()
    table = table.unify_dictionaries()
    return {
        file.as_py(): table.filter(pc.field("file_path") == file).column("pos")
        for file in table.column("file_path").chunks[0].dictionary
    }


def _combine_positional_deletes(positional_deletes: List[pa.ChunkedArray], rows: int) -> pa.Array:
    if len(positional_deletes) == 1:
        all_chunks = positional_deletes[0]
    else:
        all_chunks = pa.chunked_array(itertools.chain(*[arr.chunks for arr in positional_deletes]))
    return np.setdiff1d(np.arange(rows), all_chunks, assume_unique=False)


def pyarrow_to_schema(schema: pa.Schema, name_mapping: Optional[NameMapping] = None) -> Schema:
    has_ids = visit_pyarrow(schema, _HasIds())
    if has_ids:
        visitor = _ConvertToIceberg()
    elif name_mapping is not None:
        visitor = _ConvertToIceberg(name_mapping=name_mapping)
    else:
        raise ValueError(
            "Parquet file does not have field-ids and the Iceberg table does not have 'schema.name-mapping.default' defined"
        )
    return visit_pyarrow(schema, visitor)


@singledispatch
def visit_pyarrow(obj: Union[pa.DataType, pa.Schema], visitor: PyArrowSchemaVisitor[T]) -> T:
    """Apply a pyarrow schema visitor to any point within a schema.

    The function traverses the schema in post-order fashion.

    Args:
        obj (Union[pa.DataType, pa.Schema]): An instance of a Schema or an IcebergType.
        visitor (PyArrowSchemaVisitor[T]): An instance of an implementation of the generic PyarrowSchemaVisitor base class.

    Raises:
        NotImplementedError: If attempting to visit an unrecognized object type.
    """
    raise NotImplementedError(f"Cannot visit non-type: {obj}")


@visit_pyarrow.register(pa.Schema)
def _(obj: pa.Schema, visitor: PyArrowSchemaVisitor[T]) -> T:
    return visitor.schema(obj, visit_pyarrow(pa.struct(obj), visitor))


@visit_pyarrow.register(pa.StructType)
def _(obj: pa.StructType, visitor: PyArrowSchemaVisitor[T]) -> T:
    results = []

    for field in obj:
        visitor.before_field(field)
        result = visit_pyarrow(field.type, visitor)
        results.append(visitor.field(field, result))
        visitor.after_field(field)

    return visitor.struct(obj, results)


@visit_pyarrow.register(pa.ListType)
def _(obj: pa.ListType, visitor: PyArrowSchemaVisitor[T]) -> T:
    visitor.before_list_element(obj.value_field)
    result = visit_pyarrow(obj.value_type, visitor)
    visitor.after_list_element(obj.value_field)

    return visitor.list(obj, result)


@visit_pyarrow.register(pa.MapType)
def _(obj: pa.MapType, visitor: PyArrowSchemaVisitor[T]) -> T:
    visitor.before_map_key(obj.key_field)
    key_result = visit_pyarrow(obj.key_type, visitor)
    visitor.after_map_key(obj.key_field)

    visitor.before_map_value(obj.item_field)
    value_result = visit_pyarrow(obj.item_type, visitor)
    visitor.after_map_value(obj.item_field)

    return visitor.map(obj, key_result, value_result)


@visit_pyarrow.register(pa.DataType)
def _(obj: pa.DataType, visitor: PyArrowSchemaVisitor[T]) -> T:
    if pa.types.is_nested(obj):
        raise TypeError(f"Expected primitive type, got: {type(obj)}")
    return visitor.primitive(obj)


class PyArrowSchemaVisitor(Generic[T], ABC):
    def before_field(self, field: pa.Field) -> None:
        """Override this method to perform an action immediately before visiting a field."""

    def after_field(self, field: pa.Field) -> None:
        """Override this method to perform an action immediately after visiting a field."""

    def before_list_element(self, element: pa.Field) -> None:
        """Override this method to perform an action immediately before visiting an element within a ListType."""

    def after_list_element(self, element: pa.Field) -> None:
        """Override this method to perform an action immediately after visiting an element within a ListType."""

    def before_map_key(self, key: pa.Field) -> None:
        """Override this method to perform an action immediately before visiting a key within a MapType."""

    def after_map_key(self, key: pa.Field) -> None:
        """Override this method to perform an action immediately after visiting a key within a MapType."""

    def before_map_value(self, value: pa.Field) -> None:
        """Override this method to perform an action immediately before visiting a value within a MapType."""

    def after_map_value(self, value: pa.Field) -> None:
        """Override this method to perform an action immediately after visiting a value within a MapType."""

    @abstractmethod
    def schema(self, schema: pa.Schema, struct_result: T) -> T:
        """Visit a schema."""

    @abstractmethod
    def struct(self, struct: pa.StructType, field_results: List[T]) -> T:
        """Visit a struct."""

    @abstractmethod
    def field(self, field: pa.Field, field_result: T) -> T:
        """Visit a field."""

    @abstractmethod
    def list(self, list_type: pa.ListType, element_result: T) -> T:
        """Visit a list."""

    @abstractmethod
    def map(self, map_type: pa.MapType, key_result: T, value_result: T) -> T:
        """Visit a map."""

    @abstractmethod
    def primitive(self, primitive: pa.DataType) -> T:
        """Visit a primitive type."""


def _get_field_id(field: pa.Field) -> Optional[int]:
    return (
        int(field_id_str.decode())
        if (field.metadata and (field_id_str := field.metadata.get(PYARROW_PARQUET_FIELD_ID_KEY)))
        else None
    )


class _HasIds(PyArrowSchemaVisitor[bool]):
    def schema(self, schema: pa.Schema, struct_result: bool) -> bool:
        return struct_result

    def struct(self, struct: pa.StructType, field_results: List[bool]) -> bool:
        return all(field_results)

    def field(self, field: pa.Field, field_result: bool) -> bool:
        return all([_get_field_id(field) is not None, field_result])

    def list(self, list_type: pa.ListType, element_result: bool) -> bool:
        element_field = list_type.value_field
        element_id = _get_field_id(element_field)
        return element_result and element_id is not None

    def map(self, map_type: pa.MapType, key_result: bool, value_result: bool) -> bool:
        key_field = map_type.key_field
        key_id = _get_field_id(key_field)
        value_field = map_type.item_field
        value_id = _get_field_id(value_field)
        return all([key_id is not None, value_id is not None, key_result, value_result])

    def primitive(self, primitive: pa.DataType) -> bool:
        return True


class _ConvertToIceberg(PyArrowSchemaVisitor[Union[IcebergType, Schema]]):
    """Converts PyArrowSchema to Iceberg Schema. Applies the IDs from name_mapping if provided."""

    _field_names: List[str]
    _name_mapping: Optional[NameMapping]

    def __init__(self, name_mapping: Optional[NameMapping] = None) -> None:
        self._field_names = []
        self._name_mapping = name_mapping

    def _field_id(self, field: pa.Field) -> int:
        if self._name_mapping:
            return self._name_mapping.find(*self._field_names).field_id
        elif (field_id := _get_field_id(field)) is not None:
            return field_id
        else:
            raise ValueError(f"Cannot convert {field} to Iceberg Field as field_id is empty.")

    def schema(self, schema: pa.Schema, struct_result: StructType) -> Schema:
        return Schema(*struct_result.fields)

    def struct(self, struct: pa.StructType, field_results: List[NestedField]) -> StructType:
        return StructType(*field_results)

    def field(self, field: pa.Field, field_result: IcebergType) -> NestedField:
        field_id = self._field_id(field)
        field_doc = doc_str.decode() if (field.metadata and (doc_str := field.metadata.get(PYARROW_FIELD_DOC_KEY))) else None
        field_type = field_result
        return NestedField(field_id, field.name, field_type, required=not field.nullable, doc=field_doc)

    def list(self, list_type: pa.ListType, element_result: IcebergType) -> ListType:
        element_field = list_type.value_field
        self._field_names.append(LIST_ELEMENT_NAME)
        element_id = self._field_id(element_field)
        self._field_names.pop()
        return ListType(element_id, element_result, element_required=not element_field.nullable)

    def map(self, map_type: pa.MapType, key_result: IcebergType, value_result: IcebergType) -> MapType:
        key_field = map_type.key_field
        self._field_names.append(MAP_KEY_NAME)
        key_id = self._field_id(key_field)
        self._field_names.pop()
        value_field = map_type.item_field
        self._field_names.append(MAP_VALUE_NAME)
        value_id = self._field_id(value_field)
        self._field_names.pop()
        return MapType(key_id, key_result, value_id, value_result, value_required=not value_field.nullable)

    def primitive(self, primitive: pa.DataType) -> PrimitiveType:
        if pa.types.is_boolean(primitive):
            return BooleanType()
        elif pa.types.is_integer(primitive):
            width = primitive.bit_width
            if width <= 32:
                return IntegerType()
            elif width <= 64:
                return LongType()
            else:
                # Does not exist (yet)
                raise TypeError(f"Unsupported integer type: {primitive}")
        elif pa.types.is_float32(primitive):
            return FloatType()
        elif pa.types.is_float64(primitive):
            return DoubleType()
        elif isinstance(primitive, pa.Decimal128Type):
            primitive = cast(pa.Decimal128Type, primitive)
            return DecimalType(primitive.precision, primitive.scale)
        elif pa.types.is_string(primitive) or pa.types.is_large_string(primitive):
            return StringType()
        elif pa.types.is_date32(primitive):
            return DateType()
        elif isinstance(primitive, pa.Time64Type) and primitive.unit == "us":
            return TimeType()
        elif pa.types.is_timestamp(primitive):
            primitive = cast(pa.TimestampType, primitive)
            if primitive.unit == "us":
                if primitive.tz == "UTC" or primitive.tz == "+00:00":
                    return TimestamptzType()
                elif primitive.tz is None:
                    return TimestampType()
        elif pa.types.is_binary(primitive) or pa.types.is_large_binary(primitive):
            return BinaryType()
        elif pa.types.is_fixed_size_binary(primitive):
            primitive = cast(pa.FixedSizeBinaryType, primitive)
            return FixedType(primitive.byte_width)

        raise TypeError(f"Unsupported type: {primitive}")

    def before_field(self, field: pa.Field) -> None:
        self._field_names.append(field.name)

    def after_field(self, field: pa.Field) -> None:
        self._field_names.pop()

    def before_list_element(self, element: pa.Field) -> None:
        self._field_names.append(LIST_ELEMENT_NAME)

    def after_list_element(self, element: pa.Field) -> None:
        self._field_names.pop()

    def before_map_key(self, key: pa.Field) -> None:
        self._field_names.append(MAP_KEY_NAME)

    def after_map_key(self, element: pa.Field) -> None:
        self._field_names.pop()

    def before_map_value(self, value: pa.Field) -> None:
        self._field_names.append(MAP_VALUE_NAME)

    def after_map_value(self, element: pa.Field) -> None:
        self._field_names.pop()


class _ConvertToIcebergWithoutIDs(_ConvertToIceberg):
    """
    Converts PyArrowSchema to Iceberg Schema with all -1 ids.

    The schema generated through this visitor should always be
    used in conjunction with `new_table_metadata` function to
    assign new field ids in order. This is currently used only
    when creating an Iceberg Schema from a PyArrow schema when
    creating a new Iceberg table.
    """

    def _field_id(self, field: pa.Field) -> int:
        return -1


def _task_to_table(
    fs: FileSystem,
    task: FileScanTask,
    bound_row_filter: BooleanExpression,
    projected_schema: Schema,
    projected_field_ids: Set[int],
    positional_deletes: Optional[List[ChunkedArray]],
    case_sensitive: bool,
    row_counts: List[int],
    limit: Optional[int] = None,
    name_mapping: Optional[NameMapping] = None,
) -> Optional[pa.Table]:
    if limit and sum(row_counts) >= limit:
        return None

    _, _, path = PyArrowFileIO.parse_location(task.file.file_path)
    arrow_format = ds.ParquetFileFormat(pre_buffer=True, buffer_size=(ONE_MEGABYTE * 8))
    with fs.open_input_file(path) as fin:
        fragment = arrow_format.make_fragment(fin)
        physical_schema = fragment.physical_schema
        schema_raw = None
        if metadata := physical_schema.metadata:
            schema_raw = metadata.get(ICEBERG_SCHEMA)
        file_schema = (
            Schema.model_validate_json(schema_raw) if schema_raw is not None else pyarrow_to_schema(physical_schema, name_mapping)
        )

        pyarrow_filter = None
        if bound_row_filter is not AlwaysTrue():
            translated_row_filter = translate_column_names(bound_row_filter, file_schema, case_sensitive=case_sensitive)
            bound_file_filter = bind(file_schema, translated_row_filter, case_sensitive=case_sensitive)
            pyarrow_filter = expression_to_pyarrow(bound_file_filter)

        file_project_schema = sanitize_column_names(prune_columns(file_schema, projected_field_ids, select_full_types=False))

        if file_schema is None:
            raise ValueError(f"Missing Iceberg schema in Metadata for file: {path}")

        fragment_scanner = ds.Scanner.from_fragment(
            fragment=fragment,
            schema=physical_schema,
            # This will push down the query to Arrow.
            # But in case there are positional deletes, we have to apply them first
            filter=pyarrow_filter if not positional_deletes else None,
            columns=[col.name for col in file_project_schema.columns],
        )

        if positional_deletes:
            # Create the mask of indices that we're interested in
            indices = _combine_positional_deletes(positional_deletes, fragment.count_rows())

            if limit:
                if pyarrow_filter is not None:
                    # In case of the filter, we don't exactly know how many rows
                    # we need to fetch upfront, can be optimized in the future:
                    # https://github.com/apache/arrow/issues/35301
                    arrow_table = fragment_scanner.take(indices)
                    arrow_table = arrow_table.filter(pyarrow_filter)
                    arrow_table = arrow_table.slice(0, limit)
                else:
                    arrow_table = fragment_scanner.take(indices[0:limit])
            else:
                arrow_table = fragment_scanner.take(indices)
                # Apply the user filter
                if pyarrow_filter is not None:
                    arrow_table = arrow_table.filter(pyarrow_filter)
        else:
            # If there are no deletes, we can just take the head
            # and the user-filter is already applied
            if limit:
                arrow_table = fragment_scanner.head(limit)
            else:
                arrow_table = fragment_scanner.to_table()

        if len(arrow_table) < 1:
            return None

        if limit is not None and sum(row_counts) >= limit:
            return None

        row_counts.append(len(arrow_table))

        return to_requested_schema(projected_schema, file_project_schema, arrow_table)


def _read_all_delete_files(fs: FileSystem, tasks: Iterable[FileScanTask]) -> Dict[str, List[ChunkedArray]]:
    deletes_per_file: Dict[str, List[ChunkedArray]] = {}
    unique_deletes = set(itertools.chain.from_iterable([task.delete_files for task in tasks]))
    if len(unique_deletes) > 0:
        executor = ExecutorFactory.get_or_create()
        deletes_per_files: Iterator[Dict[str, ChunkedArray]] = executor.map(
            lambda args: _read_deletes(*args), [(fs, delete) for delete in unique_deletes]
        )
        for delete in deletes_per_files:
            for file, arr in delete.items():
                if file in deletes_per_file:
                    deletes_per_file[file].append(arr)
                else:
                    deletes_per_file[file] = [arr]

    return deletes_per_file


def project_table(
    tasks: Iterable[FileScanTask],
    table: Table,
    row_filter: BooleanExpression,
    projected_schema: Schema,
    case_sensitive: bool = True,
    limit: Optional[int] = None,
) -> pa.Table:
    """Resolve the right columns based on the identifier.

    Args:
        tasks (Iterable[FileScanTask]): A URI or a path to a local file.
        table (Table): The table that's being queried.
        row_filter (BooleanExpression): The expression for filtering rows.
        projected_schema (Schema): The output schema.
        case_sensitive (bool): Case sensitivity when looking up column names.
        limit (Optional[int]): Limit the number of records.

    Raises:
        ResolveError: When an incompatible query is done.
    """
    scheme, netloc, _ = PyArrowFileIO.parse_location(table.location())
    if isinstance(table.io, PyArrowFileIO):
        fs = table.io.fs_by_scheme(scheme, netloc)
    else:
        try:
            from pyiceberg.io.fsspec import FsspecFileIO

            if isinstance(table.io, FsspecFileIO):
                from pyarrow.fs import PyFileSystem

                fs = PyFileSystem(FSSpecHandler(table.io.get_fs(scheme)))
            else:
                raise ValueError(f"Expected PyArrowFileIO or FsspecFileIO, got: {table.io}")
        except ModuleNotFoundError as e:
            # When FsSpec is not installed
            raise ValueError(f"Expected PyArrowFileIO or FsspecFileIO, got: {table.io}") from e

    bound_row_filter = bind(table.schema(), row_filter, case_sensitive=case_sensitive)

    projected_field_ids = {
        id for id in projected_schema.field_ids if not isinstance(projected_schema.find_type(id), (MapType, ListType))
    }.union(extract_field_ids(bound_row_filter))

    row_counts: List[int] = []
    deletes_per_file = _read_all_delete_files(fs, tasks)
    executor = ExecutorFactory.get_or_create()
    futures = [
        executor.submit(
            _task_to_table,
            fs,
            task,
            bound_row_filter,
            projected_schema,
            projected_field_ids,
            deletes_per_file.get(task.file.file_path),
            case_sensitive,
            row_counts,
            limit,
            table.name_mapping(),
        )
        for task in tasks
    ]

    # for consistent ordering, we need to maintain future order
    futures_index = {f: i for i, f in enumerate(futures)}
    completed_futures: SortedList[Future[pa.Table]] = SortedList(iterable=[], key=lambda f: futures_index[f])
    for future in concurrent.futures.as_completed(futures):
        completed_futures.add(future)

        # stop early if limit is satisfied
        if limit is not None and sum(row_counts) >= limit:
            break

    # by now, we've either completed all tasks or satisfied the limit
    if limit is not None:
        _ = [f.cancel() for f in futures if not f.done()]

    tables = [f.result() for f in completed_futures if f.result()]

    if len(tables) < 1:
        return pa.Table.from_batches([], schema=schema_to_pyarrow(projected_schema))

    result = pa.concat_tables(tables)

    if limit is not None:
        return result.slice(0, limit)

    return result


def to_requested_schema(requested_schema: Schema, file_schema: Schema, table: pa.Table) -> pa.Table:
    struct_array = visit_with_partner(requested_schema, table, ArrowProjectionVisitor(file_schema), ArrowAccessor(file_schema))

    arrays = []
    fields = []
    for pos, field in enumerate(requested_schema.fields):
        array = struct_array.field(pos)
        arrays.append(array)
        fields.append(pa.field(field.name, array.type, field.optional))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


class ArrowProjectionVisitor(SchemaWithPartnerVisitor[pa.Array, Optional[pa.Array]]):
    file_schema: Schema

    def __init__(self, file_schema: Schema):
        self.file_schema = file_schema

    def _cast_if_needed(self, field: NestedField, values: pa.Array) -> pa.Array:
        file_field = self.file_schema.find_field(field.field_id)
        if field.field_type.is_primitive and field.field_type != file_field.field_type:
            return values.cast(schema_to_pyarrow(promote(file_field.field_type, field.field_type)))
        return values

    def _construct_field(self, field: NestedField, arrow_type: pa.DataType) -> pa.Field:
        return pa.field(
            name=field.name,
            type=arrow_type,
            nullable=field.optional,
            metadata={DOC: field.doc} if field.doc is not None else None,
        )

    def schema(self, schema: Schema, schema_partner: Optional[pa.Array], struct_result: Optional[pa.Array]) -> Optional[pa.Array]:
        return struct_result

    def struct(
        self, struct: StructType, struct_array: Optional[pa.Array], field_results: List[Optional[pa.Array]]
    ) -> Optional[pa.Array]:
        if struct_array is None:
            return None
        field_arrays: List[pa.Array] = []
        fields: List[pa.Field] = []
        for field, field_array in zip(struct.fields, field_results):
            if field_array is not None:
                array = self._cast_if_needed(field, field_array)
                field_arrays.append(array)
                fields.append(self._construct_field(field, array.type))
            elif field.optional:
                arrow_type = schema_to_pyarrow(field.field_type)
                field_arrays.append(pa.nulls(len(struct_array), type=arrow_type))
                fields.append(self._construct_field(field, arrow_type))
            else:
                raise ResolveError(f"Field is required, and could not be found in the file: {field}")

        return pa.StructArray.from_arrays(arrays=field_arrays, fields=pa.struct(fields))

    def field(self, field: NestedField, _: Optional[pa.Array], field_array: Optional[pa.Array]) -> Optional[pa.Array]:
        return field_array

    def list(self, list_type: ListType, list_array: Optional[pa.Array], value_array: Optional[pa.Array]) -> Optional[pa.Array]:
        if isinstance(list_array, pa.ListArray) and value_array is not None:
            if isinstance(value_array, pa.StructArray):
                # This can be removed once this has been fixed:
                # https://github.com/apache/arrow/issues/38809
                list_array = pa.ListArray.from_arrays(list_array.offsets, value_array)

            arrow_field = pa.list_(self._construct_field(list_type.element_field, value_array.type))
            return list_array.cast(arrow_field)
        else:
            return None

    def map(
        self, map_type: MapType, map_array: Optional[pa.Array], key_result: Optional[pa.Array], value_result: Optional[pa.Array]
    ) -> Optional[pa.Array]:
        if isinstance(map_array, pa.MapArray) and key_result is not None and value_result is not None:
            arrow_field = pa.map_(
                self._construct_field(map_type.key_field, key_result.type),
                self._construct_field(map_type.value_field, value_result.type),
            )
            if isinstance(value_result, pa.StructArray):
                # Arrow does not allow reordering of fields, therefore we have to copy the array :(
                return pa.MapArray.from_arrays(map_array.offsets, key_result, value_result, arrow_field)
            else:
                return map_array.cast(arrow_field)
        else:
            return None

    def primitive(self, _: PrimitiveType, array: Optional[pa.Array]) -> Optional[pa.Array]:
        return array


class ArrowAccessor(PartnerAccessor[pa.Array]):
    file_schema: Schema

    def __init__(self, file_schema: Schema):
        self.file_schema = file_schema

    def schema_partner(self, partner: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner

    def field_partner(self, partner_struct: Optional[pa.Array], field_id: int, _: str) -> Optional[pa.Array]:
        if partner_struct:
            # use the field name from the file schema
            try:
                name = self.file_schema.find_field(field_id).name
            except ValueError:
                return None

            if isinstance(partner_struct, pa.StructArray):
                return partner_struct.field(name)
            elif isinstance(partner_struct, pa.Table):
                return partner_struct.column(name).combine_chunks()

        return None

    def list_element_partner(self, partner_list: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner_list.values if isinstance(partner_list, pa.ListArray) else None

    def map_key_partner(self, partner_map: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner_map.keys if isinstance(partner_map, pa.MapArray) else None

    def map_value_partner(self, partner_map: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner_map.items if isinstance(partner_map, pa.MapArray) else None


def _primitive_to_physical(iceberg_type: PrimitiveType) -> str:
    return visit(iceberg_type, _PRIMITIVE_TO_PHYSICAL_TYPE_VISITOR)


class PrimitiveToPhysicalType(SchemaVisitorPerPrimitiveType[str]):
    def schema(self, schema: Schema, struct_result: str) -> str:
        raise ValueError(f"Expected primitive-type, got: {schema}")

    def struct(self, struct: StructType, field_results: List[str]) -> str:
        raise ValueError(f"Expected primitive-type, got: {struct}")

    def field(self, field: NestedField, field_result: str) -> str:
        raise ValueError(f"Expected primitive-type, got: {field}")

    def list(self, list_type: ListType, element_result: str) -> str:
        raise ValueError(f"Expected primitive-type, got: {list_type}")

    def map(self, map_type: MapType, key_result: str, value_result: str) -> str:
        raise ValueError(f"Expected primitive-type, got: {map_type}")

    def visit_fixed(self, fixed_type: FixedType) -> str:
        return "FIXED_LEN_BYTE_ARRAY"

    def visit_decimal(self, decimal_type: DecimalType) -> str:
        return "FIXED_LEN_BYTE_ARRAY"

    def visit_boolean(self, boolean_type: BooleanType) -> str:
        return "BOOLEAN"

    def visit_integer(self, integer_type: IntegerType) -> str:
        return "INT32"

    def visit_long(self, long_type: LongType) -> str:
        return "INT64"

    def visit_float(self, float_type: FloatType) -> str:
        return "FLOAT"

    def visit_double(self, double_type: DoubleType) -> str:
        return "DOUBLE"

    def visit_date(self, date_type: DateType) -> str:
        return "INT32"

    def visit_time(self, time_type: TimeType) -> str:
        return "INT64"

    def visit_timestamp(self, timestamp_type: TimestampType) -> str:
        return "INT64"

    def visit_timestamptz(self, timestamptz_type: TimestamptzType) -> str:
        return "INT64"

    def visit_string(self, string_type: StringType) -> str:
        return "BYTE_ARRAY"

    def visit_uuid(self, uuid_type: UUIDType) -> str:
        return "FIXED_LEN_BYTE_ARRAY"

    def visit_binary(self, binary_type: BinaryType) -> str:
        return "BYTE_ARRAY"


_PRIMITIVE_TO_PHYSICAL_TYPE_VISITOR = PrimitiveToPhysicalType()


class StatsAggregator:
    current_min: Any
    current_max: Any
    trunc_length: Optional[int]

    def __init__(self, iceberg_type: PrimitiveType, physical_type_string: str, trunc_length: Optional[int] = None) -> None:
        self.current_min = None
        self.current_max = None
        self.trunc_length = trunc_length

        expected_physical_type = _primitive_to_physical(iceberg_type)
        if expected_physical_type != physical_type_string:
            raise ValueError(
                f"Unexpected physical type {physical_type_string} for {iceberg_type}, expected {expected_physical_type}"
            )

        self.primitive_type = iceberg_type

    def serialize(self, value: Any) -> bytes:
        return to_bytes(self.primitive_type, value)

    def update_min(self, val: Optional[Any]) -> None:
        if self.current_min is None:
            self.current_min = val
        elif val is not None:
            self.current_min = min(val, self.current_min)

    def update_max(self, val: Optional[Any]) -> None:
        if self.current_max is None:
            self.current_max = val
        elif val is not None:
            self.current_max = max(val, self.current_max)

    def min_as_bytes(self) -> Optional[bytes]:
        if self.current_min is None:
            return None

        return self.serialize(
            self.current_min
            if self.trunc_length is None
            else TruncateTransform(width=self.trunc_length).transform(self.primitive_type)(self.current_min)
        )

    def max_as_bytes(self) -> Optional[bytes]:
        if self.current_max is None:
            return None

        if self.primitive_type == StringType():
            if not isinstance(self.current_max, str):
                raise ValueError("Expected the current_max to be a string")
            s_result = truncate_upper_bound_text_string(self.current_max, self.trunc_length)
            return self.serialize(s_result) if s_result is not None else None
        elif self.primitive_type == BinaryType():
            if not isinstance(self.current_max, bytes):
                raise ValueError("Expected the current_max to be bytes")
            b_result = truncate_upper_bound_binary_string(self.current_max, self.trunc_length)
            return self.serialize(b_result) if b_result is not None else None
        else:
            if self.trunc_length is not None:
                raise ValueError(f"{self.primitive_type} cannot be truncated")
            return self.serialize(self.current_max)


DEFAULT_TRUNCATION_LENGTH = 16
TRUNCATION_EXPR = r"^truncate\((\d+)\)$"


class MetricModeTypes(Enum):
    TRUNCATE = "truncate"
    NONE = "none"
    COUNTS = "counts"
    FULL = "full"


@dataclass(frozen=True)
class MetricsMode(Singleton):
    type: MetricModeTypes
    length: Optional[int] = None


def match_metrics_mode(mode: str) -> MetricsMode:
    sanitized_mode = mode.strip().lower()
    if sanitized_mode.startswith("truncate"):
        m = re.match(TRUNCATION_EXPR, sanitized_mode)
        if m:
            length = int(m[1])
            if length < 1:
                raise ValueError("Truncation length must be larger than 0")
            return MetricsMode(MetricModeTypes.TRUNCATE, int(m[1]))
        else:
            raise ValueError(f"Malformed truncate: {mode}")
    elif sanitized_mode == "none":
        return MetricsMode(MetricModeTypes.NONE)
    elif sanitized_mode == "counts":
        return MetricsMode(MetricModeTypes.COUNTS)
    elif sanitized_mode == "full":
        return MetricsMode(MetricModeTypes.FULL)
    else:
        raise ValueError(f"Unsupported metrics mode: {mode}")


@dataclass(frozen=True)
class StatisticsCollector:
    field_id: int
    iceberg_type: PrimitiveType
    mode: MetricsMode
    column_name: str


class PyArrowStatisticsCollector(PreOrderSchemaVisitor[List[StatisticsCollector]]):
    _field_id: int = 0
    _schema: Schema
    _properties: Dict[str, str]
    _default_mode: str

    def __init__(self, schema: Schema, properties: Dict[str, str]):
        self._schema = schema
        self._properties = properties
        self._default_mode = self._properties.get(
            TableProperties.DEFAULT_WRITE_METRICS_MODE, TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT
        )

    def schema(self, schema: Schema, struct_result: Callable[[], List[StatisticsCollector]]) -> List[StatisticsCollector]:
        return struct_result()

    def struct(
        self, struct: StructType, field_results: List[Callable[[], List[StatisticsCollector]]]
    ) -> List[StatisticsCollector]:
        return list(itertools.chain(*[result() for result in field_results]))

    def field(self, field: NestedField, field_result: Callable[[], List[StatisticsCollector]]) -> List[StatisticsCollector]:
        self._field_id = field.field_id
        return field_result()

    def list(self, list_type: ListType, element_result: Callable[[], List[StatisticsCollector]]) -> List[StatisticsCollector]:
        self._field_id = list_type.element_id
        return element_result()

    def map(
        self,
        map_type: MapType,
        key_result: Callable[[], List[StatisticsCollector]],
        value_result: Callable[[], List[StatisticsCollector]],
    ) -> List[StatisticsCollector]:
        self._field_id = map_type.key_id
        k = key_result()
        self._field_id = map_type.value_id
        v = value_result()
        return k + v

    def primitive(self, primitive: PrimitiveType) -> List[StatisticsCollector]:
        column_name = self._schema.find_column_name(self._field_id)
        if column_name is None:
            return []

        metrics_mode = match_metrics_mode(self._default_mode)

        col_mode = self._properties.get(f"{TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX}.{column_name}")
        if col_mode:
            metrics_mode = match_metrics_mode(col_mode)

        if (
            not (isinstance(primitive, StringType) or isinstance(primitive, BinaryType))
            and metrics_mode.type == MetricModeTypes.TRUNCATE
        ):
            metrics_mode = MetricsMode(MetricModeTypes.FULL)

        is_nested = column_name.find(".") >= 0

        if is_nested and metrics_mode.type in [MetricModeTypes.TRUNCATE, MetricModeTypes.FULL]:
            metrics_mode = MetricsMode(MetricModeTypes.COUNTS)

        return [StatisticsCollector(field_id=self._field_id, iceberg_type=primitive, mode=metrics_mode, column_name=column_name)]


def compute_statistics_plan(
    schema: Schema,
    table_properties: Dict[str, str],
) -> Dict[int, StatisticsCollector]:
    """
    Compute the statistics plan for all columns.

    The resulting list is assumed to have the same length and same order as the columns in the pyarrow table.
    This allows the list to map from the column index to the Iceberg column ID.
    For each element, the desired metrics collection that was provided by the user in the configuration
    is computed and then adjusted according to the data type of the column. For nested columns the minimum
    and maximum values are not computed. And truncation is only applied to text of binary strings.

    Args:
        table_properties (from pyiceberg.table.metadata.TableMetadata): The Iceberg table metadata properties.
            They are required to compute the mapping of column position to iceberg schema type id. It's also
            used to set the mode for column metrics collection
    """
    stats_cols = pre_order_visit(schema, PyArrowStatisticsCollector(schema, table_properties))
    result: Dict[int, StatisticsCollector] = {}
    for stats_col in stats_cols:
        result[stats_col.field_id] = stats_col
    return result


@dataclass(frozen=True)
class ID2ParquetPath:
    field_id: int
    parquet_path: str


class ID2ParquetPathVisitor(PreOrderSchemaVisitor[List[ID2ParquetPath]]):
    _field_id: int = 0
    _path: List[str]

    def __init__(self) -> None:
        self._path = []

    def schema(self, schema: Schema, struct_result: Callable[[], List[ID2ParquetPath]]) -> List[ID2ParquetPath]:
        return struct_result()

    def struct(self, struct: StructType, field_results: List[Callable[[], List[ID2ParquetPath]]]) -> List[ID2ParquetPath]:
        return list(itertools.chain(*[result() for result in field_results]))

    def field(self, field: NestedField, field_result: Callable[[], List[ID2ParquetPath]]) -> List[ID2ParquetPath]:
        self._field_id = field.field_id
        self._path.append(field.name)
        result = field_result()
        self._path.pop()
        return result

    def list(self, list_type: ListType, element_result: Callable[[], List[ID2ParquetPath]]) -> List[ID2ParquetPath]:
        self._field_id = list_type.element_id
        self._path.append("list.element")
        result = element_result()
        self._path.pop()
        return result

    def map(
        self,
        map_type: MapType,
        key_result: Callable[[], List[ID2ParquetPath]],
        value_result: Callable[[], List[ID2ParquetPath]],
    ) -> List[ID2ParquetPath]:
        self._field_id = map_type.key_id
        self._path.append("key_value.key")
        k = key_result()
        self._path.pop()
        self._field_id = map_type.value_id
        self._path.append("key_value.value")
        v = value_result()
        self._path.pop()
        return k + v

    def primitive(self, primitive: PrimitiveType) -> List[ID2ParquetPath]:
        return [ID2ParquetPath(field_id=self._field_id, parquet_path=".".join(self._path))]


def parquet_path_to_id_mapping(
    schema: Schema,
) -> Dict[str, int]:
    """
    Compute the mapping of parquet column path to Iceberg ID.

    For each column, the parquet file metadata has a path_in_schema attribute that follows
    a specific naming scheme for nested columnds. This function computes a mapping of
    the full paths to the corresponding Iceberg IDs.

    Args:
        schema (pyiceberg.schema.Schema): The current table schema.
    """
    result: Dict[str, int] = {}
    for pair in pre_order_visit(schema, ID2ParquetPathVisitor()):
        result[pair.parquet_path] = pair.field_id
    return result


def fill_parquet_file_metadata(
    data_file: DataFile,
    parquet_metadata: pq.FileMetaData,
    stats_columns: Dict[int, StatisticsCollector],
    parquet_column_mapping: Dict[str, int],
) -> None:
    """
    Compute and fill the following fields of the DataFile object.

    - file_format
    - column_sizes
    - value_counts
    - null_value_counts
    - nan_value_counts
    - lower_bounds
    - upper_bounds
    - split_offsets

    Args:
        data_file (DataFile): A DataFile object representing the Parquet file for which metadata is to be filled.
        parquet_metadata (pyarrow.parquet.FileMetaData): A pyarrow metadata object.
        stats_columns (Dict[int, StatisticsCollector]): The statistics gathering plan. It is required to
            set the mode for column metrics collection
    """
    if parquet_metadata.num_columns != len(stats_columns):
        raise ValueError(
            f"Number of columns in statistics configuration ({len(stats_columns)}) is different from the number of columns in pyarrow table ({parquet_metadata.num_columns})"
        )

    if parquet_metadata.num_columns != len(parquet_column_mapping):
        raise ValueError(
            f"Number of columns in column mapping ({len(parquet_column_mapping)}) is different from the number of columns in pyarrow table ({parquet_metadata.num_columns})"
        )

    column_sizes: Dict[int, int] = {}
    value_counts: Dict[int, int] = {}
    split_offsets: List[int] = []

    null_value_counts: Dict[int, int] = {}
    nan_value_counts: Dict[int, int] = {}

    col_aggs = {}

    for r in range(parquet_metadata.num_row_groups):
        # References:
        # https://github.com/apache/iceberg/blob/fc381a81a1fdb8f51a0637ca27cd30673bd7aad3/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L232
        # https://github.com/apache/parquet-mr/blob/ac29db4611f86a07cc6877b416aa4b183e09b353/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/metadata/ColumnChunkMetaData.java#L184

        row_group = parquet_metadata.row_group(r)

        data_offset = row_group.column(0).data_page_offset
        dictionary_offset = row_group.column(0).dictionary_page_offset

        if row_group.column(0).has_dictionary_page and dictionary_offset < data_offset:
            split_offsets.append(dictionary_offset)
        else:
            split_offsets.append(data_offset)

        invalidate_col: Set[int] = set()

        for pos in range(parquet_metadata.num_columns):
            column = row_group.column(pos)
            field_id = parquet_column_mapping[column.path_in_schema]

            stats_col = stats_columns[field_id]

            column_sizes.setdefault(field_id, 0)
            column_sizes[field_id] += column.total_compressed_size

            if stats_col.mode == MetricsMode(MetricModeTypes.NONE):
                continue

            value_counts[field_id] = value_counts.get(field_id, 0) + column.num_values

            if column.is_stats_set:
                try:
                    statistics = column.statistics

                    if statistics.has_null_count:
                        null_value_counts[field_id] = null_value_counts.get(field_id, 0) + statistics.null_count

                    if stats_col.mode == MetricsMode(MetricModeTypes.COUNTS):
                        continue

                    if field_id not in col_aggs:
                        col_aggs[field_id] = StatsAggregator(
                            stats_col.iceberg_type, statistics.physical_type, stats_col.mode.length
                        )

                    col_aggs[field_id].update_min(statistics.min)
                    col_aggs[field_id].update_max(statistics.max)

                except pyarrow.lib.ArrowNotImplementedError as e:
                    invalidate_col.add(field_id)
                    logger.warning(e)
            else:
                invalidate_col.add(field_id)
                logger.warning("PyArrow statistics missing for column %d when writing file", pos)

    split_offsets.sort()

    lower_bounds = {}
    upper_bounds = {}

    for k, agg in col_aggs.items():
        _min = agg.min_as_bytes()
        if _min is not None:
            lower_bounds[k] = _min
        _max = agg.max_as_bytes()
        if _max is not None:
            upper_bounds[k] = _max

    for field_id in invalidate_col:
        del lower_bounds[field_id]
        del upper_bounds[field_id]
        del null_value_counts[field_id]

    data_file.record_count = parquet_metadata.num_rows
    data_file.column_sizes = column_sizes
    data_file.value_counts = value_counts
    data_file.null_value_counts = null_value_counts
    data_file.nan_value_counts = nan_value_counts
    data_file.lower_bounds = lower_bounds
    data_file.upper_bounds = upper_bounds
    data_file.split_offsets = split_offsets


def write_file(table: Table, tasks: Iterator[WriteTask]) -> Iterator[DataFile]:
    task = next(tasks)

    try:
        _ = next(tasks)
        # If there are more tasks, raise an exception
        raise NotImplementedError("Only unpartitioned writes are supported: https://github.com/apache/iceberg-python/issues/208")
    except StopIteration:
        pass

    parquet_writer_kwargs = _get_parquet_writer_kwargs(table.properties)

    file_path = f'{table.location()}/data/{task.generate_data_file_filename("parquet")}'
    file_schema = schema_to_pyarrow(table.schema())

    fo = table.io.new_output(file_path)
    row_group_size = PropertyUtil.property_as_int(
        properties=table.properties,
        property_name=TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
        default=TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
    )
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=file_schema, **parquet_writer_kwargs) as writer:
            writer.write_table(task.df, row_group_size=row_group_size)

    data_file = DataFile(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        file_size_in_bytes=len(fo),
        # After this has been fixed:
        # https://github.com/apache/iceberg-python/issues/271
        # sort_order_id=task.sort_order_id,
        sort_order_id=None,
        # Just copy these from the table for now
        spec_id=table.spec().spec_id,
        equality_ids=None,
        key_metadata=None,
    )

    fill_parquet_file_metadata(
        data_file=data_file,
        parquet_metadata=writer.writer.metadata,
        stats_columns=compute_statistics_plan(table.schema(), table.properties),
        parquet_column_mapping=parquet_path_to_id_mapping(table.schema()),
    )
    return iter([data_file])


ICEBERG_UNCOMPRESSED_CODEC = "uncompressed"
PYARROW_UNCOMPRESSED_CODEC = "none"


def _get_parquet_writer_kwargs(table_properties: Properties) -> Dict[str, Any]:
    for key_pattern in [
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
        TableProperties.PARQUET_PAGE_ROW_LIMIT,
        TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES,
        f"{TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX}.*",
    ]:
        if unsupported_keys := fnmatch.filter(table_properties, key_pattern):
            raise NotImplementedError(f"Parquet writer option(s) {unsupported_keys} not implemented")

    compression_codec = table_properties.get(TableProperties.PARQUET_COMPRESSION, TableProperties.PARQUET_COMPRESSION_DEFAULT)
    compression_level = PropertyUtil.property_as_int(
        properties=table_properties,
        property_name=TableProperties.PARQUET_COMPRESSION_LEVEL,
        default=TableProperties.PARQUET_COMPRESSION_LEVEL_DEFAULT,
    )
    if compression_codec == ICEBERG_UNCOMPRESSED_CODEC:
        compression_codec = PYARROW_UNCOMPRESSED_CODEC

    return {
        "compression": compression_codec,
        "compression_level": compression_level,
        "data_page_size": PropertyUtil.property_as_int(
            properties=table_properties,
            property_name=TableProperties.PARQUET_PAGE_SIZE_BYTES,
            default=TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT,
        ),
        "dictionary_pagesize_limit": PropertyUtil.property_as_int(
            properties=table_properties,
            property_name=TableProperties.PARQUET_DICT_SIZE_BYTES,
            default=TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT,
        ),
        "write_batch_size": PropertyUtil.property_as_int(
            properties=table_properties,
            property_name=TableProperties.PARQUET_PAGE_ROW_LIMIT,
            default=TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT,
        ),
    }
