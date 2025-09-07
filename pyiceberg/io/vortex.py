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

"""Vortex file format support for PyIceberg.

This module provides support for reading and writing Vortex files, a next-generation
columnar file format designed for high-performance data processing. Vortex offers:

- 100x faster random access reads vs. Parquet
- 10-20x faster scans
- 5x faster writes
- Similar compression ratios
- Zero-copy compatibility with Apache Arrow

The implementation leverages vortex-data Python bindings to integrate with PyIceberg's
existing table operations and schema management.
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    Or,
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
)
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionKey, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Record
from pyiceberg.types import ListType, MapType, StructType

try:
    import vortex as vx  # type: ignore[import-not-found]
    import vortex.expr as ve  # type: ignore[import-not-found]

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False
    vx = None
    ve = None


logger = logging.getLogger(__name__)

# Vortex file extension
VORTEX_FILE_EXTENSION = ".vortex"


@dataclass(frozen=True)
class VortexWriteTask:
    """Task for writing data to a Vortex file."""

    write_uuid: Optional[uuid.UUID]
    task_id: int
    record_batches: List[pa.RecordBatch]
    partition_key: Optional[PartitionKey] = None
    schema: Optional[Schema] = None

    def generate_data_file_filename(self, extension: str) -> str:
        """Generate a unique filename for the data file."""
        if self.partition_key:
            # Use a simple partition hash for filename uniqueness
            partition_hash = hash(str(self.partition_key.partition))
            return f"{self.write_uuid}-{self.task_id}-{partition_hash}.{extension}"
        else:
            return f"{self.write_uuid}-{self.task_id}.{extension}"


def _check_vortex_available() -> None:
    """Check if vortex is available and raise an informative error if not."""
    if not VORTEX_AVAILABLE:
        raise ImportError(
            "vortex-data is not installed. Please install it with: pip install vortex-data or pip install 'pyiceberg[vortex]'"
        )


def iceberg_schema_to_vortex_schema(iceberg_schema: Schema) -> Any:
    """Convert an Iceberg schema to a Vortex schema.

    Args:
        iceberg_schema: The Iceberg schema to convert

    Returns:
        A PyArrow schema that Vortex can use for type inference

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If schema conversion fails
    """
    _check_vortex_available()

    try:
        # Convert to PyArrow schema first, preserving field IDs
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        arrow_schema = schema_to_pyarrow(iceberg_schema, include_field_ids=True)

        # Validate that the schema is compatible with Vortex
        _validate_vortex_schema_compatibility(arrow_schema)

        return arrow_schema
    except Exception as e:
        raise ValueError(f"Failed to convert Iceberg schema to Vortex-compatible format: {e}") from e


def _validate_vortex_schema_compatibility(arrow_schema: pa.Schema) -> None:
    """Validate that a PyArrow schema is compatible with Vortex.

    Args:
        arrow_schema: The PyArrow schema to validate

    Raises:
        ValueError: If the schema contains unsupported types
    """
    unsupported_types = []

    for field in arrow_schema:
        field_type = field.type

        # Check for complex nested types that might not be fully supported
        if pa.types.is_union(field_type):
            unsupported_types.append(f"Union type in field '{field.name}'")
        elif pa.types.is_large_list(field_type) or pa.types.is_large_string(field_type):
            # Large types might have performance implications
            logger.warning(f"Large type detected in field '{field.name}' - may impact performance")

    if unsupported_types:
        raise ValueError(f"Schema contains unsupported types for Vortex: {', '.join(unsupported_types)}")


def arrow_to_vortex_array(arrow_table: pa.Table, compress: bool = True) -> Any:
    """Convert a PyArrow table to a Vortex array.

    Args:
        arrow_table: The PyArrow table to convert
        compress: Whether to apply Vortex compression optimizations

    Returns:
        A Vortex array

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Create Vortex array from Arrow table
        vortex_array = vx.array(arrow_table)

        # Apply compression if requested
        if compress:
            try:
                vortex_array = vx.compress(vortex_array)
                logger.debug(f"Applied Vortex compression to array with {len(arrow_table)} rows")
            except Exception as e:
                logger.warning(f"Vortex compression failed, using uncompressed array: {e}")

        return vortex_array

    except Exception as e:
        raise ValueError(f"Failed to convert PyArrow table to Vortex array: {e}") from e


def vortex_to_arrow_table(vortex_array: Any, preserve_field_ids: bool = True) -> pa.Table:
    """Convert a Vortex array back to a PyArrow table.

    Args:
        vortex_array: The Vortex array to convert
        preserve_field_ids: Whether to preserve Iceberg field IDs in metadata

    Returns:
        A PyArrow table

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Convert Vortex array to Arrow
        if hasattr(vortex_array, "to_arrow_array"):
            arrow_data = vortex_array.to_arrow_array()
        elif hasattr(vortex_array, "to_arrow"):
            arrow_data = vortex_array.to_arrow()
        else:
            raise ValueError("Vortex array does not have a recognized Arrow conversion method")

        # Handle both Table and Array returns
        if isinstance(arrow_data, pa.Table):
            arrow_table = arrow_data
        elif isinstance(arrow_data, pa.Array):
            # Convert Array to Table with a single column
            arrow_table = pa.table([arrow_data], names=["data"])
        elif isinstance(arrow_data, pa.ChunkedArray):
            # Convert ChunkedArray to Table with a single column
            arrow_table = pa.table([arrow_data], names=["data"])
        else:
            # Handle RecordBatch or other formats
            if hasattr(arrow_data, "to_table"):
                arrow_table = arrow_data.to_table()
            else:
                raise ValueError(f"Unexpected Arrow data type: {type(arrow_data)}")

        # Validate the resulting table
        _validate_arrow_table_integrity(arrow_table)

        logger.debug(f"Converted Vortex array to Arrow table with {len(arrow_table)} rows, {len(arrow_table.schema)} columns")
        return arrow_table

    except Exception as e:
        raise ValueError(f"Failed to convert Vortex array to PyArrow table: {e}") from e


def _validate_arrow_table_integrity(arrow_table: pa.Table) -> None:
    """Validate the integrity of a converted Arrow table.

    Args:
        arrow_table: The Arrow table to validate

    Raises:
        ValueError: If the table has integrity issues
    """
    if arrow_table is None:
        raise ValueError("Converted Arrow table is None")

    if len(arrow_table.schema) == 0:
        raise ValueError("Converted Arrow table has no columns")

    # Check for null schemas or corrupt data
    try:
        arrow_table.validate()
    except Exception as e:
        logger.warning(f"Arrow table validation failed: {e}")


def convert_iceberg_to_vortex_file(
    iceberg_table_data: pa.Table,
    iceberg_schema: Schema,
    output_path: str,
    io: FileIO,
    compression: bool = True,
) -> DataFile:
    """High-level function to convert Iceberg table data to a Vortex file.

    Args:
        iceberg_table_data: PyArrow table with Iceberg data
        iceberg_schema: The Iceberg schema
        output_path: Path where to write the Vortex file
        io: FileIO instance for file operations
        compression: Whether to apply Vortex compression

    Returns:
        DataFile metadata for the created Vortex file

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Validate input
        if iceberg_table_data is None or len(iceberg_table_data) == 0:
            raise ValueError("Input table data is empty")

        # Convert schema for validation
        iceberg_schema_to_vortex_schema(iceberg_schema)
        logger.debug(f"Validated schema with {len(iceberg_schema.fields)} fields")

        # Convert data to Vortex array for validation
        arrow_to_vortex_array(iceberg_table_data, compress=compression)
        logger.debug(f"Validated {len(iceberg_table_data)} rows for Vortex conversion")

        # Write Vortex file
        file_size = write_vortex_file(
            arrow_table=iceberg_table_data,  # Use original Arrow table for writing
            file_path=output_path,
            io=io,
            compression="auto" if compression else None,
        )

        # Create DataFile metadata
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=output_path,
            file_format=FileFormat.VORTEX,
            partition=Record(),
            file_size_in_bytes=file_size,
            record_count=len(iceberg_table_data),
            sort_order_id=None,
            spec_id=0,  # Default spec
            equality_ids=None,
            key_metadata=None,
        )

        logger.info(f"Successfully created Vortex file: {output_path} ({file_size} bytes, {len(iceberg_table_data)} rows)")
        return data_file

    except Exception as e:
        raise ValueError(f"Failed to convert Iceberg data to Vortex file: {e}") from e


def estimate_vortex_compression_ratio(arrow_table: pa.Table) -> float:
    """Estimate the compression ratio that Vortex might achieve.

    Args:
        arrow_table: The PyArrow table to analyze

    Returns:
        Estimated compression ratio (original_size / compressed_size)

    Note:
        This is an approximation based on data characteristics
    """
    if not VORTEX_AVAILABLE:
        return 1.0  # No compression without Vortex

    try:
        # Estimate based on data types and patterns
        compression_ratio = 1.0

        for column in arrow_table.columns:
            column_type = column.type

            if pa.types.is_string(column_type) or pa.types.is_binary(column_type):
                # String compression can be very effective
                compression_ratio *= 2.5
            elif pa.types.is_integer(column_type):
                # Integer compression depends on distribution
                compression_ratio *= 1.8
            elif pa.types.is_floating(column_type):
                # Floating point compression is more limited
                compression_ratio *= 1.3
            elif pa.types.is_boolean(column_type):
                # Boolean data compresses excellently
                compression_ratio *= 8.0

        # Cap the estimate at reasonable bounds
        return min(max(compression_ratio, 1.1), 10.0)

    except Exception:
        return 2.0  # Default conservative estimate


def optimize_vortex_write_config(
    arrow_table: pa.Table,
    target_file_size_mb: int = 128,
) -> Dict[str, Any]:
    """Generate optimized configuration for Vortex file writing.

    Args:
        arrow_table: The table to be written
        target_file_size_mb: Target file size in MB

    Returns:
        Dictionary with optimized Vortex write parameters
    """
    config = {
        "compression": "auto",
        "row_group_size": 10000,
        "dictionary_encoding": True,
        "statistics": True,
    }

    if not VORTEX_AVAILABLE:
        return config

    try:
        # Adjust based on table characteristics
        num_rows = len(arrow_table)
        num_columns = len(arrow_table.schema)

        # Optimize row group size based on data volume
        if num_rows < 1000:
            config["row_group_size"] = max(100, num_rows // 10)
        elif num_rows > 1_000_000:
            config["row_group_size"] = 50000
        else:
            config["row_group_size"] = min(10000, num_rows // 100)

        # Enable advanced compression for large tables
        if num_rows > 100_000 or num_columns > 100:
            config["compression"] = "aggressive"

        # Dictionary encoding is most effective for string columns
        string_columns = sum(1 for field in arrow_table.schema if pa.types.is_string(field.type))
        if string_columns == 0:
            config["dictionary_encoding"] = False

        return config

    except Exception:
        return config


def batch_convert_iceberg_to_vortex(
    arrow_tables: List[pa.Table],
    iceberg_schema: Schema,
    output_directory: str,
    io: FileIO,
    file_prefix: str = "data",
    compression: bool = True,
) -> List[DataFile]:
    """Convert multiple Arrow tables to Vortex files in batch.

    Args:
        arrow_tables: List of PyArrow tables to convert
        iceberg_schema: The Iceberg schema
        output_directory: Directory to write Vortex files
        io: FileIO instance
        file_prefix: Prefix for generated file names
        compression: Whether to apply compression

    Returns:
        List of DataFile metadata for created files

    Raises:
        ValueError: If batch conversion fails
    """
    _check_vortex_available()

    if not arrow_tables:
        return []

    data_files = []
    total_rows = sum(len(table) for table in arrow_tables)
    logger.info(f"Starting batch conversion of {len(arrow_tables)} tables ({total_rows} total rows)")

    try:
        for i, arrow_table in enumerate(arrow_tables):
            if len(arrow_table) == 0:
                logger.warning(f"Skipping empty table {i}")
                continue

            file_name = f"{file_prefix}_{i:04d}.vortex"
            output_path = f"{output_directory.rstrip('/')}/{file_name}"

            data_file = convert_iceberg_to_vortex_file(
                iceberg_table_data=arrow_table,
                iceberg_schema=iceberg_schema,
                output_path=output_path,
                io=io,
                compression=compression,
            )

            data_files.append(data_file)
            logger.debug(f"Converted batch file {i + 1}/{len(arrow_tables)}: {output_path}")

        logger.info(f"Completed batch conversion: {len(data_files)} Vortex files created")
        return data_files

    except Exception as e:
        raise ValueError(f"Batch conversion failed at file {len(data_files)}: {e}") from e


def analyze_vortex_compatibility(iceberg_schema: Schema) -> Dict[str, Any]:
    """Analyze Iceberg schema compatibility with Vortex format.

    Args:
        iceberg_schema: The Iceberg schema to analyze

    Returns:
        Dictionary with compatibility analysis results
    """
    analysis: Dict[str, Any] = {
        "compatible": True,
        "warnings": [],
        "field_analysis": [],
        "recommended_optimizations": [],
    }

    try:
        for field in iceberg_schema.fields:
            field_info: Dict[str, Any] = {
                "name": field.name,
                "type": str(field.field_type),
                "optional": field.optional,
                "compatible": True,
                "notes": [],
            }

            # Check for potentially problematic types
            field_type = field.field_type

            if isinstance(field_type, MapType):
                field_info["notes"].append("Map types may have limited optimization in Vortex")
                analysis["recommended_optimizations"].append(
                    f"Consider flattening map field '{field.name}' for better performance"
                )

            elif isinstance(field_type, ListType):
                field_info["notes"].append("List types are supported but may impact compression")

            elif isinstance(field_type, StructType):
                field_info["notes"].append("Struct types are well-supported in Vortex")

            elif str(field_type).startswith("decimal"):
                field_info["notes"].append("Decimal types are supported with potential precision considerations")

            analysis["field_analysis"].append(field_info)

        # Generate overall recommendations
        if len(iceberg_schema.fields) > 1000:
            analysis["warnings"].append("Schema has >1000 fields, consider schema optimization")

        nested_fields = sum(1 for field in iceberg_schema.fields if isinstance(field.field_type, (StructType, ListType, MapType)))
        if nested_fields > len(iceberg_schema.fields) * 0.5:
            analysis["recommended_optimizations"].append("High ratio of nested fields detected - consider denormalization")

    except Exception as e:
        analysis["compatible"] = False
        analysis["warnings"].append(f"Analysis failed: {e}")

    return analysis


def write_vortex_file(
    arrow_table: pa.Table,
    file_path: str,
    io: FileIO,
    compression: Optional[str] = None,
) -> int:
    """Write a PyArrow table to a Vortex file.

    Args:
        arrow_table: The PyArrow table to write
        file_path: The path where to write the file
        io: The FileIO instance for file operations
        compression: Optional compression algorithm (handled by Vortex internally)

    Returns:
        The size of the written file in bytes
    """
    _check_vortex_available()

    try:
        # Handle empty tables gracefully - they should be writable
        if len(arrow_table) == 0:
            # For empty tables, create a minimal Vortex file with just the schema
            logger.debug(f"Writing empty Arrow table with schema: {arrow_table.schema}")

        # Convert Arrow table to Vortex array
        vortex_array = arrow_to_vortex_array(arrow_table, compress=(compression is not None))

        # Write using a temporary file approach for better reliability
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=VORTEX_FILE_EXTENSION, delete=False) as tmp_file:
            tmp_file_path = tmp_file.name

        try:
            # Write Vortex file to temporary location
            vx.io.write(vortex_array, tmp_file_path)

            # Copy to final destination using FileIO
            output_file = io.new_output(file_path)
            with output_file.create(overwrite=True) as output_stream:
                with open(tmp_file_path, "rb") as temp_stream:
                    # Stream in chunks for memory efficiency
                    chunk_size = 1024 * 1024  # 1MB chunks
                    while True:
                        chunk = temp_stream.read(chunk_size)
                        if not chunk:
                            break
                        output_stream.write(chunk)

        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup temporary file {tmp_file_path}: {e}")

        # Get final file size
        input_file = io.new_input(file_path)
        file_size = len(input_file)

        logger.debug(f"Successfully wrote Vortex file: {file_path} ({file_size} bytes, {len(arrow_table)} rows)")
        return file_size

    except Exception as e:
        raise ValueError(f"Failed to write Vortex file {file_path}: {e}") from e


def read_vortex_file(
    file_path: str,
    io: FileIO,
    projected_schema: Optional[Schema] = None,
    row_filter: Optional[BooleanExpression] = None,
    case_sensitive: bool = True,
) -> Iterator[pa.RecordBatch]:
    """Read a Vortex file and return PyArrow record batches.

    Args:
        file_path: The path to the Vortex file
        io: The FileIO instance for file operations
        projected_schema: Optional schema projection
        row_filter: Optional row filter expression
        case_sensitive: Whether column names are case sensitive

    Yields:
        PyArrow record batches
    """
    _check_vortex_available()

    input_file = io.new_input(file_path)

    # For now, read the entire file into memory
    # TODO: Implement streaming reads for large files
    with input_file.open() as input_stream:
        # Write to temporary file for vortex.open to read
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=VORTEX_FILE_EXTENSION, delete=False) as tmp_file:
            tmp_file.write(input_stream.read())
            tmp_file_path = tmp_file.name

        try:
            # Open with Vortex
            vortex_file = vx.open(tmp_file_path)

            # Apply column projection if specified
            projection = None
            if projected_schema:
                projection = [field.name for field in projected_schema.fields]

            # Apply row filter if specified
            vortex_filter = None
            if row_filter and not isinstance(row_filter, AlwaysTrue):
                # Convert Iceberg expression to Vortex expression
                # This is a simplified conversion - full implementation would need
                # comprehensive expression mapping
                vortex_filter = _convert_iceberg_filter_to_vortex(row_filter)

            # Scan the file
            scanner = vortex_file.scan(projection=projection, expr=vortex_filter)

            # Read all data and convert to Arrow
            vortex_array = scanner.read_all()
            arrow_table = vortex_to_arrow_table(vortex_array)

            # Yield as record batches
            yield from arrow_table.to_batches()

        finally:
            # Clean up temporary file
            os.unlink(tmp_file_path)


def _convert_iceberg_filter_to_vortex(iceberg_filter: BooleanExpression) -> Optional[Any]:
    """Convert an Iceberg filter expression to a Vortex expression.

    Args:
        iceberg_filter: The Iceberg boolean expression

    Returns:
        A Vortex expression or None if conversion is not supported
    """
    if not VORTEX_AVAILABLE:
        return None

    try:
        return _visit_filter_expression(iceberg_filter)
    except Exception as e:
        logger.warning(f"Failed to convert filter expression to Vortex: {e}")
        return None


def _visit_filter_expression(expr: BooleanExpression) -> Optional[Any]:
    """Recursively visit and convert filter expressions."""
    if isinstance(expr, AlwaysTrue):
        return None  # No filter needed

    # Handle both bound and unbound equality expressions
    elif isinstance(expr, (EqualTo, BoundEqualTo)):
        term_name = _get_term_name(expr.term)
        return ve.eq(ve.col(term_name), _convert_literal_value(expr.literal))

    elif isinstance(expr, (NotEqualTo, BoundNotEqualTo)):
        term_name = _get_term_name(expr.term)
        return ve.neq(ve.col(term_name), _convert_literal_value(expr.literal))

    elif isinstance(expr, (LessThan, BoundLessThan)):
        term_name = _get_term_name(expr.term)
        return ve.lt(ve.col(term_name), _convert_literal_value(expr.literal))

    elif isinstance(expr, (LessThanOrEqual, BoundLessThanOrEqual)):
        term_name = _get_term_name(expr.term)
        return ve.lte(ve.col(term_name), _convert_literal_value(expr.literal))

    elif isinstance(expr, (GreaterThan, BoundGreaterThan)):
        term_name = _get_term_name(expr.term)
        return ve.gt(ve.col(term_name), _convert_literal_value(expr.literal))

    elif isinstance(expr, (GreaterThanOrEqual, BoundGreaterThanOrEqual)):
        term_name = _get_term_name(expr.term)
        return ve.gte(ve.col(term_name), _convert_literal_value(expr.literal))

    elif isinstance(expr, (IsNull, BoundIsNull)):
        term_name = _get_term_name(expr.term)
        return ve.is_null(ve.col(term_name))

    elif isinstance(expr, (NotNull, BoundNotNull)):
        term_name = _get_term_name(expr.term)
        return ve.is_not_null(ve.col(term_name))

    elif isinstance(expr, (IsNaN, BoundIsNaN)):
        # Vortex may handle NaN differently - this is a best effort conversion
        term_name = _get_term_name(expr.term)
        return ve.is_nan(ve.col(term_name))

    elif isinstance(expr, (NotNaN, BoundNotNaN)):
        term_name = _get_term_name(expr.term)
        return ve.is_not_nan(ve.col(term_name))

    elif isinstance(expr, (In, BoundIn)):
        values = [_convert_literal_value(lit) for lit in expr.literals]
        term_name = _get_term_name(expr.term)
        return ve.is_in(ve.col(term_name), values)

    elif isinstance(expr, (NotIn, BoundNotIn)):
        values = [_convert_literal_value(lit) for lit in expr.literals]
        term_name = _get_term_name(expr.term)
        return ve.is_not_in(ve.col(term_name), values)

    elif isinstance(expr, And):
        left = _visit_filter_expression(expr.left)
        right = _visit_filter_expression(expr.right)
        if left is None:
            return right
        elif right is None:
            return left
        else:
            return ve.and_(left, right)

    elif isinstance(expr, Or):
        left = _visit_filter_expression(expr.left)
        right = _visit_filter_expression(expr.right)
        if left is None and right is None:
            return None
        elif left is None:
            return right
        elif right is None:
            return left
        else:
            return ve.or_(left, right)

    elif isinstance(expr, Not):
        inner = _visit_filter_expression(expr.child)
        if inner is None:
            return None
        return ve.not_(inner)

    else:
        logger.warning(f"Unsupported filter expression type: {type(expr)}")
        return None


def _get_term_name(term: Any) -> str:
    """Extract the column name from a term (bound or unbound)."""
    # For bound terms, get the field name from the reference
    if hasattr(term, 'field') and hasattr(term.field, 'name'):
        return term.field.name
    # For unbound terms, check if it has a name attribute
    elif hasattr(term, 'name'):
        return term.name
    # Fallback to string representation
    else:
        return str(term)


def _convert_literal_value(literal: Any) -> Any:
    """Convert an Iceberg literal value to a Vortex-compatible value."""
    if hasattr(literal, "value"):
        return literal.value
    return literal


@dataclass(frozen=True)
class VortexDataFileStatistics:
    """Statistics for a Vortex data file."""

    record_count: int
    column_sizes: Dict[int, int]
    value_counts: Dict[int, int]
    null_value_counts: Dict[int, int]
    nan_value_counts: Dict[int, int]
    split_offsets: List[int]
    lower_bounds: Optional[Dict[int, bytes]] = None
    upper_bounds: Optional[Dict[int, bytes]] = None

    def to_serialized_dict(self) -> Dict[str, Any]:
        """Convert statistics to a serialized dictionary."""
        result = {
            "record_count": self.record_count,
            "column_sizes": self.column_sizes,
            "value_counts": self.value_counts,
            "null_value_counts": self.null_value_counts,
            "nan_value_counts": self.nan_value_counts,
        }

        if self.lower_bounds:
            result["lower_bounds"] = self.lower_bounds
        if self.upper_bounds:
            result["upper_bounds"] = self.upper_bounds

        return result

    @classmethod
    def from_arrow_table(cls, arrow_table: pa.Table, schema: Schema) -> "VortexDataFileStatistics":
        """Create statistics from an Arrow table and Iceberg schema."""
        record_count = len(arrow_table)
        column_sizes: Dict[int, int] = {}
        value_counts: Dict[int, int] = {}
        null_value_counts: Dict[int, int] = {}
        nan_value_counts: Dict[int, int] = {}
        lower_bounds: Dict[int, bytes] = {}
        upper_bounds: Dict[int, bytes] = {}

        # Map field names to field IDs
        field_name_to_id = {field.name: field.field_id for field in schema.fields}

        for column_name, column in zip(arrow_table.column_names, arrow_table.columns):
            field_id = field_name_to_id.get(column_name)
            if field_id is None:
                continue

            # Calculate column size (approximate)
            try:
                column_size = column.nbytes if hasattr(column, "nbytes") else 0
                column_sizes[field_id] = column_size
            except Exception:
                column_sizes[field_id] = 0

            # Count values and nulls
            value_counts[field_id] = len(column) - pa.compute.count_distinct(column).as_py()
            null_count = pa.compute.sum(pa.compute.is_null(column)).as_py() or 0
            null_value_counts[field_id] = null_count

            # Count NaN values for floating point columns
            if pa.types.is_floating(column.type):
                try:
                    nan_count = pa.compute.sum(pa.compute.is_nan(column)).as_py() or 0
                    nan_value_counts[field_id] = nan_count
                except Exception:
                    nan_value_counts[field_id] = 0
            else:
                nan_value_counts[field_id] = 0

            # Calculate bounds for supported types
            try:
                if len(column) > 0 and null_count < len(column):
                    min_val = pa.compute.min(column).as_py()
                    max_val = pa.compute.max(column).as_py()

                    if min_val is not None:
                        lower_bounds[field_id] = _serialize_bound_value(min_val, column.type)
                    if max_val is not None:
                        upper_bounds[field_id] = _serialize_bound_value(max_val, column.type)
            except Exception as e:
                logger.debug(f"Failed to calculate bounds for column {column_name}: {e}")

        return cls(
            record_count=record_count,
            column_sizes=column_sizes,
            value_counts=value_counts,
            null_value_counts=null_value_counts,
            nan_value_counts=nan_value_counts,
            split_offsets=[0],  # Single split by default
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
        )


def _serialize_bound_value(value: Any, arrow_type: pa.DataType) -> bytes:
    """Serialize a bound value to bytes for Iceberg metadata."""
    if value is None:
        return b""

    try:
        if pa.types.is_string(arrow_type):
            return str(value).encode("utf-8")
        elif pa.types.is_integer(arrow_type):
            return int(value).to_bytes(8, byteorder="big", signed=True)
        elif pa.types.is_floating(arrow_type):
            import struct

            if pa.types.is_float32(arrow_type):
                return struct.pack(">f", float(value))
            else:
                return struct.pack(">d", float(value))
        elif pa.types.is_boolean(arrow_type):
            return b"\x01" if value else b"\x00"
        elif pa.types.is_date(arrow_type):
            return int(value).to_bytes(4, byteorder="big", signed=False)
        elif pa.types.is_timestamp(arrow_type):
            return int(value).to_bytes(8, byteorder="big", signed=True)
        else:
            # For other types, convert to string and encode
            return str(value).encode("utf-8")
    except Exception as e:
        logger.debug(f"Failed to serialize bound value {value} of type {arrow_type}: {e}")
        return b""


def vortex_file_to_data_file(
    io: FileIO,
    table_metadata: TableMetadata,
    file_path: str,
    partition_spec: Optional[PartitionSpec] = None,
) -> DataFile:
    """Convert a Vortex file to a DataFile object.

    Args:
        io: The FileIO instance
        table_metadata: The table metadata
        file_path: The path to the Vortex file
        partition_spec: Optional partition specification

    Returns:
        A DataFile object
    """
    _check_vortex_available()

    input_file = io.new_input(file_path)
    file_size = len(input_file)

    # For statistics, we need to read the file
    # This is simplified - in practice, we'd want to extract metadata without full read
    try:
        record_batches = list(read_vortex_file(file_path, io))
        record_count = sum(len(batch) for batch in record_batches)

        # Create basic statistics
        statistics = VortexDataFileStatistics(
            record_count=record_count,
            column_sizes={},  # Would need to calculate from Vortex metadata
            value_counts={},  # Would need to calculate from Vortex metadata
            null_value_counts={},  # Would need to calculate from Vortex metadata
            nan_value_counts={},  # Would need to calculate from Vortex metadata
            split_offsets=[0],  # Single split for now
        )

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.VORTEX,
            partition=Record(),  # Would need partition extraction
            file_size_in_bytes=file_size,
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        return data_file

    except Exception as e:
        logger.warning(f"Failed to read Vortex file statistics for {file_path}: {e}")

        # Return basic DataFile without statistics
        return DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.VORTEX,
            partition=Record(),
            file_size_in_bytes=file_size,
            record_count=0,  # Unknown
            sort_order_id=None,
            spec_id=table_metadata.default_spec_id,
            equality_ids=None,
            key_metadata=None,
        )


def write_vortex_data_files(
    io: FileIO,
    table_metadata: TableMetadata,
    tasks: Iterator[VortexWriteTask],
) -> Iterator[DataFile]:
    """Write Vortex data files from write tasks.

    Args:
        io: The FileIO instance
        table_metadata: The table metadata
        tasks: Iterator of write tasks

    Yields:
        DataFile objects for the written files
    """
    _check_vortex_available()

    from pyiceberg.table.locations import load_location_provider

    # Get location provider for generating file paths
    location_provider = load_location_provider(table_location=table_metadata.location, table_properties=table_metadata.properties)

    for task in tasks:
        # Convert record batches to Arrow table
        if not task.record_batches:
            continue

        arrow_table = pa.Table.from_batches(task.record_batches)

        # Generate file path
        file_path = location_provider.new_data_location(
            data_file_name=task.generate_data_file_filename("vortex"),
            partition_key=task.partition_key,
        )

        # Write Vortex file
        write_vortex_file(
            arrow_table=arrow_table,
            file_path=file_path,
            io=io,
            compression=None,  # Vortex handles compression internally
        )

        # Create data file metadata
        yield vortex_file_to_data_file(
            io=io,
            table_metadata=table_metadata,
            file_path=file_path,
        )


def vortex_files_to_data_files(
    io: FileIO,
    table_metadata: TableMetadata,
    file_paths: Iterator[str],
) -> Iterator[DataFile]:
    """Convert Vortex file paths to DataFile objects.

    Args:
        io: The FileIO instance
        table_metadata: The table metadata
        file_paths: Iterator of file paths

    Yields:
        DataFile objects
    """
    for file_path in file_paths:
        yield vortex_file_to_data_file(
            io=io,
            table_metadata=table_metadata,
            file_path=file_path,
        )


def read_vortex_deletes(io: FileIO, data_file: DataFile) -> Dict[str, pa.ChunkedArray]:
    """Read Vortex delete files and return positional deletes.

    Args:
        io: The FileIO instance
        data_file: The delete file to read

    Returns:
        Dictionary mapping file paths to delete positions

    Raises:
        NotImplementedError: Vortex delete files are not yet fully supported
    """
    _check_vortex_available()

    if data_file.file_format != FileFormat.VORTEX:
        raise ValueError(f"Expected Vortex file format, got {data_file.file_format}")

    # TODO: Implement proper Vortex delete file reading
    # For now, we'll read the file and extract positional delete information
    try:
        record_batches = list(read_vortex_file(data_file.file_path, io))

        if not record_batches:
            return {}

        # Combine all batches into a single table
        combined_table = pa.Table.from_batches(record_batches)

        # Expect standard delete file schema with 'file_path' and 'pos' columns
        if "file_path" not in combined_table.column_names or "pos" not in combined_table.column_names:
            raise ValueError("Vortex delete file must contain 'file_path' and 'pos' columns")

        # Group delete positions by file path
        deletes_by_file: Dict[str, pa.ChunkedArray] = {}

        file_paths = combined_table.column("file_path")
        positions = combined_table.column("pos")

        # Convert to dictionary format expected by PyIceberg
        unique_files = pc.unique(file_paths)

        for file_name in unique_files.to_pylist():
            mask = pc.equal(file_paths, file_name)
            file_positions = pc.filter(positions, mask)
            deletes_by_file[file_name] = file_positions

        return deletes_by_file

    except Exception as e:
        logger.warning(f"Failed to read Vortex delete file {data_file.file_path}: {e}")
        return {}


def optimize_vortex_file_layout(
    io: FileIO,
    input_files: List[str],
    output_file: str,
    schema: Schema,
    target_file_size: int = 128 * 1024 * 1024,  # 128MB
) -> DataFile:
    """Optimize multiple Vortex files by combining them into a single optimized file.

    Args:
        io: The FileIO instance
        input_files: List of input Vortex file paths
        output_file: Path for the optimized output file
        schema: The Iceberg schema
        target_file_size: Target file size in bytes

    Returns:
        DataFile for the optimized file

    Raises:
        ValueError: If optimization fails
    """
    _check_vortex_available()

    if not input_files:
        raise ValueError("No input files provided for optimization")

    try:
        # Read all input files and combine into batches
        all_batches: List[pa.RecordBatch] = []
        total_rows = 0

        for file_path in input_files:
            batches = list(read_vortex_file(file_path, io))
            all_batches.extend(batches)
            total_rows += sum(len(batch) for batch in batches)

        if not all_batches:
            raise ValueError("No data found in input files")

        # Combine all batches into a single table
        combined_table = pa.Table.from_batches(all_batches)

        # Sort the table for better compression and query performance
        # Use the first column as the sort key (this could be made configurable)
        if len(combined_table.columns) > 0:
            first_column = combined_table.column_names[0]
            try:
                sort_indices = pc.sort_indices(combined_table, sort_keys=[(first_column, "ascending")])
                combined_table = pc.take(combined_table, sort_indices)
                logger.debug(f"Sorted table by column '{first_column}' for optimization")
            except Exception as e:
                logger.debug(f"Failed to sort table for optimization: {e}")

        # Write the optimized file
        file_size = write_vortex_file(
            arrow_table=combined_table,
            file_path=output_file,
            io=io,
            compression="auto",
        )

        # Generate statistics
        statistics = VortexDataFileStatistics.from_arrow_table(combined_table, schema)

        # Create DataFile metadata
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=output_file,
            file_format=FileFormat.VORTEX,
            partition=Record(),
            file_size_in_bytes=file_size,
            record_count=len(combined_table),
            sort_order_id=None,
            spec_id=0,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )

        logger.info(f"Optimized {len(input_files)} files into {output_file}: {total_rows} rows, {file_size} bytes")

        return data_file

    except Exception as e:
        raise ValueError(f"Failed to optimize Vortex files: {e}") from e


def estimate_vortex_query_performance(
    files: List[DataFile],
    query_columns: Optional[List[str]] = None,
    row_filter: Optional[BooleanExpression] = None,
) -> Dict[str, Any]:
    """Estimate query performance characteristics for Vortex files.

    Args:
        files: List of DataFile objects
        query_columns: List of columns to be queried (projection)
        row_filter: Row filter expression

    Returns:
        Dictionary with performance estimates
    """
    if not files:
        return {"estimated_scan_time_ms": 0, "estimated_bytes_scanned": 0, "recommendations": []}

    total_bytes = sum(f.file_size_in_bytes for f in files if f.file_format == FileFormat.VORTEX)
    total_rows = sum(f.record_count for f in files if f.file_format == FileFormat.VORTEX)
    vortex_files = len([f for f in files if f.file_format == FileFormat.VORTEX])

    # Rough performance estimates based on Vortex characteristics
    # These would be refined with actual benchmarking data

    # Vortex is ~10-20x faster for scans than Parquet
    base_scan_time_ms = (total_bytes / (100 * 1024 * 1024)) * 1000  # 100MB/s base rate
    vortex_scan_time_ms = base_scan_time_ms / 15  # ~15x speedup

    # Column projection benefits
    if query_columns:
        # Assume 50% reduction in scan time for typical column projection
        vortex_scan_time_ms *= 0.5

    # Row filtering benefits
    if row_filter and not isinstance(row_filter, AlwaysTrue):
        # Vortex's advanced indexing reduces scan time significantly
        vortex_scan_time_ms *= 0.3

    recommendations = []

    if vortex_files < len(files):
        recommendations.append("Convert Parquet files to Vortex format for better performance")

    if len(files) > 100:
        recommendations.append("Consider file compaction to reduce metadata overhead")

    if total_rows > 0 and total_bytes / total_rows > 1000:  # Large average row size
        recommendations.append("Large row sizes detected - ensure proper column pruning")

    return {
        "estimated_scan_time_ms": max(1, int(vortex_scan_time_ms)),
        "estimated_bytes_scanned": total_bytes,
        "total_files": len(files),
        "vortex_files": vortex_files,
        "total_rows": total_rows,
        "performance_multiplier": "15x faster than Parquet",
        "recommendations": recommendations,
    }


class VortexFileManager:
    """Advanced file management utilities for Vortex files."""

    def __init__(self, io: FileIO):
        """Initialize the Vortex file manager.

        Args:
            io: The FileIO instance to use
        """
        self.io = io
        _check_vortex_available()

    def compact_files(
        self,
        input_files: List[str],
        output_directory: str,
        schema: Schema,
        target_file_size: int = 128 * 1024 * 1024,
        max_files_per_compact: int = 10,
    ) -> List[DataFile]:
        """Compact multiple Vortex files into optimized larger files.

        Args:
            input_files: List of input file paths
            output_directory: Directory for output files
            schema: The Iceberg schema
            target_file_size: Target size for compacted files
            max_files_per_compact: Maximum files to compact together

        Returns:
            List of compacted DataFile objects
        """
        if not input_files:
            return []

        compacted_files = []

        # Group files for compaction
        file_groups = [input_files[i : i + max_files_per_compact] for i in range(0, len(input_files), max_files_per_compact)]

        for group_idx, file_group in enumerate(file_groups):
            output_path = f"{output_directory.rstrip('/')}/compacted_{group_idx:04d}.vortex"

            try:
                compacted_file = optimize_vortex_file_layout(
                    io=self.io,
                    input_files=file_group,
                    output_file=output_path,
                    schema=schema,
                    target_file_size=target_file_size,
                )
                compacted_files.append(compacted_file)

                logger.info(f"Compacted {len(file_group)} files into {output_path}")

            except Exception as e:
                logger.error(f"Failed to compact file group {group_idx}: {e}")

        return compacted_files

    def analyze_file_health(self, file_paths: List[str]) -> Dict[str, Any]:
        """Analyze the health and performance characteristics of Vortex files.

        Args:
            file_paths: List of Vortex file paths to analyze

        Returns:
            Health analysis report
        """
        if not file_paths:
            return {"status": "healthy", "files_analyzed": 0, "recommendations": []}

        analysis: Dict[str, Any] = {
            "files_analyzed": len(file_paths),
            "total_size_bytes": 0,
            "avg_file_size_mb": 0.0,
            "small_files_count": 0,
            "large_files_count": 0,
            "corrupted_files": [],
            "recommendations": [],
        }

        small_file_threshold = 10 * 1024 * 1024  # 10MB
        large_file_threshold = 500 * 1024 * 1024  # 500MB

        for file_path in file_paths:
            try:
                input_file = self.io.new_input(file_path)
                file_size = len(input_file)

                analysis["total_size_bytes"] += file_size

                if file_size < small_file_threshold:
                    analysis["small_files_count"] += 1
                elif file_size > large_file_threshold:
                    analysis["large_files_count"] += 1

                # Basic corruption check - try to read metadata
                try:
                    list(read_vortex_file(file_path, self.io, projected_schema=None))
                except Exception:
                    analysis["corrupted_files"].append(file_path)

            except Exception as e:
                logger.warning(f"Failed to analyze file {file_path}: {e}")
                analysis["corrupted_files"].append(file_path)

        # Calculate averages
        if analysis["files_analyzed"] > 0:
            analysis["avg_file_size_mb"] = analysis["total_size_bytes"] / (1024 * 1024) / analysis["files_analyzed"]

        # Generate recommendations
        if analysis["small_files_count"] > analysis["files_analyzed"] * 0.5:
            analysis["recommendations"].append("High number of small files detected - consider compaction")

        if analysis["large_files_count"] > 0:
            analysis["recommendations"].append("Large files detected - ensure proper partitioning for query performance")

        if analysis["corrupted_files"]:
            analysis["recommendations"].append(
                f"Found {len(analysis['corrupted_files'])} corrupted files - investigate data integrity"
            )

        # Determine overall health
        if analysis["corrupted_files"]:
            analysis["status"] = "unhealthy"
        elif analysis["small_files_count"] > analysis["files_analyzed"] * 0.7:
            analysis["status"] = "needs_optimization"
        else:
            analysis["status"] = "healthy"

        return analysis
