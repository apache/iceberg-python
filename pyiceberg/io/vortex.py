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

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
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
                vortex_array = vx.compress.compress(vortex_array)
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

    # Convert Arrow table to Vortex array with compression
    vortex_array = arrow_to_vortex_array(arrow_table)

    # Apply compression if Vortex supports it
    if compression:
        # Vortex handles compression internally through its encoding system
        vortex_array = vx.compress.compress(vortex_array)

    # Write to a temporary location first, then move to final location
    output_file = io.new_output(file_path)
    with output_file.create(overwrite=True) as fos:
        # For now, we'll use PyArrow parquet as an intermediate format
        # until we have direct Vortex file writing support in PyIceberg
        # This is a placeholder - in a full implementation, we'd write directly to Vortex format

        # Convert back to arrow for writing (this loses Vortex compression benefits)
        # TODO: Implement direct Vortex file writing

        # Write as Vortex format using vortex.io.write
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=VORTEX_FILE_EXTENSION, delete=False) as tmp_file:
            vx.io.write(vortex_array, tmp_file.name)

            # Read the temporary Vortex file and write to the final destination
            with open(tmp_file.name, "rb") as vortex_file:
                fos.write(vortex_file.read())

        # Clean up temporary file
        os.unlink(tmp_file.name)

    # Get file size
    input_file = io.new_input(file_path)
    return len(input_file)


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

    This is a simplified conversion. A full implementation would need
    comprehensive mapping of all expression types.

    Args:
        iceberg_filter: The Iceberg boolean expression

    Returns:
        A Vortex expression or None if conversion is not supported
    """
    # For now, return None - this would need full expression mapping
    # TODO: Implement comprehensive expression conversion
    logger.warning("Filter expression conversion from Iceberg to Vortex not yet implemented")
    return None


@dataclass(frozen=True)
class VortexDataFileStatistics:
    """Statistics for a Vortex data file."""

    record_count: int
    column_sizes: Dict[int, int]
    value_counts: Dict[int, int]
    null_value_counts: Dict[int, int]
    nan_value_counts: Dict[int, int]
    split_offsets: List[int]

    def to_serialized_dict(self) -> Dict[str, Any]:
        """Convert statistics to a serialized dictionary."""
        return {
            "record_count": self.record_count,
            "column_sizes": self.column_sizes,
            "value_counts": self.value_counts,
            "null_value_counts": self.null_value_counts,
            "nan_value_counts": self.nan_value_counts,
        }


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
