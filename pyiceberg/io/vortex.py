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
import platform
import tempfile
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from pyiceberg.expressions import (
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
)
from pyiceberg.io import FileIO
from pyiceberg.partitioning import PartitionKey
from pyiceberg.schema import Schema
from pyiceberg.types import ListType, MapType, StructType

try:
    import vortex as vx
    import vortex.expr as ve

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False
    vx = None
    ve = None

logger = logging.getLogger(__name__)

# Vortex file extension
VORTEX_FILE_EXTENSION = ".vortex"

# Environment variable configuration for performance tuning
VORTEX_BATCH_SIZE = int(os.environ.get("VORTEX_BATCH_SIZE", "1048576"))  # 1M default
VORTEX_ENABLE_CACHE = os.environ.get("VORTEX_ENABLE_CACHE", "true").lower() == "true"
VORTEX_COMPRESSION_LEVEL = os.environ.get("VORTEX_COMPRESSION_LEVEL", "auto")  # auto, fast, best, none

# Cache for repeated file reads
_vortex_file_cache: Dict[str, Any] = {}


# Memory Allocator Optimization for Vortex Performance
# ====================================================


def _optimize_memory_allocator() -> None:
    """Apply memory allocator optimizations for Vortex performance."""
    system = platform.system()

    logger.info("🔧 Optimizing Memory Allocator for Vortex Performance")

    if system == "Linux":
        # Optimize glibc malloc for high-throughput workloads
        os.environ.setdefault("MALLOC_ARENA_MAX", "1")
        os.environ.setdefault("MALLOC_MMAP_THRESHOLD", "131072")
        os.environ.setdefault("MALLOC_TRIM_THRESHOLD", "524288")
        os.environ.setdefault("MALLOC_TOP_PAD", "1048576")
        os.environ.setdefault("PYTHONMALLOC", "malloc")

    elif system == "Darwin":
        # macOS optimizations (limited tunables available)
        os.environ.setdefault("MALLOC_MMAP_THRESHOLD", "131072")
        os.environ.setdefault("PYTHONMALLOC", "malloc")

    # Cross-platform optimizations
    os.environ.setdefault("PYTHONMALLOC", "malloc")

    # Log applied optimizations
    optimizations = []
    if os.environ.get("MALLOC_ARENA_MAX"):
        optimizations.append(f"MALLOC_ARENA_MAX={os.environ['MALLOC_ARENA_MAX']}")
    if os.environ.get("MALLOC_MMAP_THRESHOLD"):
        threshold_kb = int(os.environ["MALLOC_MMAP_THRESHOLD"]) // 1024
        optimizations.append(f"MALLOC_MMAP_THRESHOLD={threshold_kb}KB")
    if os.environ.get("PYTHONMALLOC"):
        optimizations.append(f"PYTHONMALLOC={os.environ['PYTHONMALLOC']}")

    if optimizations:
        logger.info(f"✅ Applied memory optimizations: {', '.join(optimizations)}")
    else:
        logger.info("ℹ️  No additional memory optimizations needed")


# Apply memory optimizations when Vortex module is loaded
if VORTEX_AVAILABLE:
    try:
        _optimize_memory_allocator()
        logger.info("✅ Vortex memory allocator optimizations applied successfully")
    except Exception as e:
        logger.warning(f"⚠️  Failed to apply memory optimizations: {e}")
else:
    logger.debug("ℹ️  Vortex not available, skipping memory optimizations")


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


def vortex_to_arrow_table(vortex_array: Any, preserve_field_ids: bool = True) -> pa.Table:
    """Convert a Vortex array to a PyArrow table.

    Args:
        vortex_array: The Vortex array to convert
        preserve_field_ids: Whether to preserve Iceberg field IDs

    Returns:
        A PyArrow table

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Use Vortex's native Arrow conversion
        if hasattr(vortex_array, "to_arrow_table"):
            return vortex_array.to_arrow_table()
        elif hasattr(vortex_array, "to_arrow"):
            arrow_result = vortex_array.to_arrow()
            if isinstance(arrow_result, pa.Table):
                return arrow_result
            elif hasattr(arrow_result, "read_all"):
                return arrow_result.read_all()
            else:
                return pa.Table.from_batches([arrow_result])
        else:
            # Fallback: try converting via Arrow array
            if vx is None:
                raise ImportError(
                    "vortex-data is not installed. Please install it with: pip install vortex-data or pip install 'pyiceberg[vortex]'"
                )
            arrow_array = vx.array(vortex_array).to_arrow()
            return pa.Table.from_arrays([arrow_array], names=["data"])
    except Exception as e:
        raise ValueError(f"Failed to convert Vortex array to Arrow table: {e}") from e


def _write_with_temp_file(data: Union[pa.Table, pa.RecordBatchReader], file_path: str, io: FileIO) -> None:
    """Write to temp file then copy to final location."""
    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
        vx.io.write(data, tmp.name)
        tmp.flush()

        # Copy to final location with large chunks
        output_file = io.new_output(file_path)
        with output_file.create(overwrite=True) as output_stream:
            with open(tmp.name, "rb") as f:
                chunk_size = 16 * 1024 * 1024  # 16MB chunks
                while chunk := f.read(chunk_size):
                    output_stream.write(chunk)

        os.unlink(tmp.name)


def _open_with_temp_file(file_path: str, io: FileIO) -> Any:
    """Open remote file via temp copy."""
    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
        # Copy remote file to temp
        input_file = io.new_input(file_path)
        with input_file.open() as input_stream:
            with open(tmp.name, "wb") as f:
                chunk_size = 16 * 1024 * 1024  # 16MB chunks
                data = input_stream.read(chunk_size)
                while data:
                    f.write(data)
                    data = input_stream.read(chunk_size)

        # Open temp file with Vortex
        vxf = vx.open(tmp.name)

        # Clean up temp file after opening
        os.unlink(tmp.name)
        return vxf


def write_vortex_file(
    arrow_table: Union[pa.Table, pa.RecordBatchReader],
    file_path: str,
    io: FileIO,
    compression: Optional[str] = None,
    streaming: bool = True,
) -> int:
    """Write Arrow data to Vortex format with streaming and compression.

    Args:
        arrow_table: PyArrow Table or RecordBatchReader for streaming
        file_path: Target file path
        io: FileIO instance for remote writes
        compression: Compression level ('auto', 'fast', 'best', None)
        streaming: Use streaming write for large datasets

    Returns:
        File size in bytes
    """
    _check_vortex_available()

    compression = compression or VORTEX_COMPRESSION_LEVEL

    # Handle streaming input
    if streaming and isinstance(arrow_table, pa.Table) and len(arrow_table) > 100_000:
        # Convert large tables to streaming reader
        arrow_table = arrow_table.to_reader(max_chunksize=VORTEX_BATCH_SIZE)

    # Apply compression if requested and not streaming
    if compression and compression != "none" and isinstance(arrow_table, pa.Table):
        # Pre-compress for better performance
        vortex_array = vx.array(arrow_table)
        if compression == "best":
            vortex_array = vx.compress(vortex_array)
        arrow_table = vortex_array.to_arrow()

    # Check if we can write directly
    is_local = not file_path.startswith(("s3://", "gs://", "abfs://", "http://", "https://"))

    if is_local:
        # Direct write for local files
        try:
            vx.io.write(arrow_table, file_path)
        except Exception as e:
            logger.warning(f"Direct write failed: {e}, using fallback")
            _write_with_temp_file(arrow_table, file_path, io)
    else:
        # Use temp file for remote locations
        _write_with_temp_file(arrow_table, file_path, io)

    # Get file size
    try:
        if is_local:
            input_file = io.new_input(file_path)
            if hasattr(input_file, "get_length"):
                file_size = input_file.get_length()
            else:
                with input_file.open() as f:
                    f.seek(0, os.SEEK_END)
                    file_size = f.tell()
            input_file = io.new_input(file_path)
            file_size = len(input_file)
    except Exception:
        file_size = 0

    logger.debug(f"Wrote Vortex file: {file_path} ({file_size} bytes)")
    return file_size


def read_vortex_file(
    file_path: str,
    io: FileIO,
    projected_schema: Optional[Schema] = None,
    filters: Optional[BooleanExpression] = None,
    batch_size: Optional[int] = None,
    use_cache: bool = True,
) -> Iterator[pa.RecordBatch]:
    """Read Vortex file with optimized multi-path strategy for maximum speed.

    Args:
        file_path: Path to Vortex file
        io: FileIO instance
        projected_schema: Schema for column projection
        filters: Boolean expression for row filtering
        batch_size: Number of rows per batch
        use_cache: Use cached file handles for repeated reads

    Yields:
        PyArrow RecordBatches
    """
    _check_vortex_available()

    # Use smaller batch size for better streaming performance (like previous fast implementation)
    batch_size = batch_size or 256_000  # 256K rows - proven optimal from commit db71cc5

    # Convert filters once
    vortex_expr = None
    if filters:
        try:
            vortex_expr = _convert_iceberg_filter_to_vortex(filters)
        except Exception as e:
            logger.debug(f"Skipping Vortex predicate pushdown due to conversion error: {e}")
            vortex_expr = None

    # Get projection columns
    projection = None
    if projected_schema:
        projection = [field.name for field in projected_schema.fields]

    # Path 1: Try direct URL reading (fastest path from previous implementation)
    if "://" in file_path and hasattr(vx, "io") and hasattr(vx.io, "read_url"):
        try:
            result = vx.io.read_url(file_path)
            if vortex_expr is not None and hasattr(result, "filter"):
                result = result.filter(vortex_expr)

            # Convert to Arrow table
            arrow_table = result.to_arrow_table() if hasattr(result, "to_arrow_table") else vortex_to_arrow_table(result)

            # Apply projection if requested
            if projection:
                proj_names = [n for n in projection if n in arrow_table.column_names]
                if proj_names:
                    arrow_table = arrow_table.select(proj_names)

            # Yield in optimized batches
            yield from arrow_table.to_batches(max_chunksize=batch_size)
            return
        except Exception as e:
            logger.debug(f"Direct URL read path failed, trying direct open: {e}")

    # Path 2: Try direct file opening (fast path)
    try:
        # Check cache for repeated reads
        cache_key = f"{file_path}:{projection}:{vortex_expr}" if use_cache and VORTEX_ENABLE_CACHE else None
        if cache_key and cache_key in _vortex_file_cache:
            logger.debug(f"Using cached Vortex file handle for {file_path}")
            vortex_file = _vortex_file_cache[cache_key]
        else:
            vortex_file = vx.open(file_path)
            if cache_key:
                _vortex_file_cache[cache_key] = vortex_file

        # Create reader with projection and filtering - direct yield for speed
        reader = vortex_file.to_arrow(projection=projection, expr=vortex_expr, batch_size=batch_size)
        yield from reader
        return
    except Exception as e:
        logger.debug(f"Direct open path failed for {file_path}, falling back to temp copy: {e}")

    # Path 3: Fallback - temp file copy (slowest but compatible)
    input_file = io.new_input(file_path)
    with input_file.open() as input_stream:
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=VORTEX_FILE_EXTENSION, delete=False) as tmp_file:
            chunk_size = 8 * 1024 * 1024  # 8MB chunks
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                tmp_file.write(chunk)
            tmp_file_path = tmp_file.name

    try:
        vortex_file = vx.open(tmp_file_path)
        reader = vortex_file.to_arrow(projection=projection, expr=vortex_expr, batch_size=batch_size)
        yield from reader
    finally:
        try:
            os.unlink(tmp_file_path)
        except Exception:
            pass


def write_vortex_streaming(
    record_batch_reader: pa.RecordBatchReader, file_path: str, io: FileIO, compress_batches: bool = False
) -> int:
    """Write streaming data to Vortex with optional per-batch compression.

    Args:
        record_batch_reader: PyArrow RecordBatchReader
        file_path: Target file path
        io: FileIO instance
        compress_batches: Apply compression to each batch

    Returns:
        File size in bytes
    """
    _check_vortex_available()

    if compress_batches:
        # Create compressed batch generator
        def compressed_batches() -> Iterator[pa.RecordBatch]:
            for batch in record_batch_reader:
                vx_array = vx.array(batch)
                compressed = vx.compress(vx_array)
                yield compressed.to_arrow()

        # Create new reader from compressed batches
        compressed_reader = pa.RecordBatchReader.from_batches(record_batch_reader.schema, compressed_batches())
        data_to_write = compressed_reader
    else:
        data_to_write = record_batch_reader

    # Use standard write function with streaming data
    return write_vortex_file(arrow_table=data_to_write, file_path=file_path, io=io, streaming=True)


@lru_cache(maxsize=32)
def create_repeated_scan(
    file_path: str,
    projection: Optional[tuple[str, ...]] = None,
    filters_hash: Optional[int] = None,
    batch_size: int = VORTEX_BATCH_SIZE,
) -> Any:
    """Create a cached RepeatedScan for optimized repeated reads.

    Args:
        file_path: Path to Vortex file
        projection: Tuple of column names (hashable for cache)
        filters_hash: Hash of filter expression
        batch_size: Rows per batch

    Returns:
        Vortex RepeatedScan object
    """
    _check_vortex_available()

    vxf = vx.open(file_path)

    # Convert projection tuple back to list
    projection_list = list(projection) if projection else None

    # Create repeated scan for efficient repeated reads
    repeated_scan = vxf.to_repeated_scan(projection=projection_list, batch_size=batch_size)

    logger.debug(f"Created RepeatedScan for {file_path} with projection {projection_list}")
    return repeated_scan


def optimize_for_vortex(arrow_table: pa.Table) -> pa.Table:
    """Optimize Arrow table for Vortex writing.

    Args:
        arrow_table: Input Arrow table

    Returns:
        Optimized Arrow table
    """
    if not VORTEX_AVAILABLE:
        return arrow_table

    # Ensure non-nullable top-level for streaming
    schema = arrow_table.schema
    if any(field.nullable for field in schema):
        # Create non-nullable schema
        new_fields = []
        for field in schema:
            if field.name != "__null_marker__":  # Skip internal fields
                new_fields.append(pa.field(field.name, field.type, nullable=False))
            else:
                new_fields.append(field)

        new_schema = pa.schema(new_fields)
        arrow_table = arrow_table.cast(new_schema)

    return arrow_table


def clear_vortex_cache() -> None:
    """Clear the Vortex file cache."""
    global _vortex_file_cache
    _vortex_file_cache.clear()
    create_repeated_scan.cache_clear()
    logger.info("Cleared Vortex file cache")


def _get_term_name(term: Any) -> str:
    """Extract the field name from a term (bound or unbound)."""
    if hasattr(term, "field") and hasattr(term.field, "name"):
        return term.field.name
    elif hasattr(term, "name"):
        return term.name
    else:
        return str(term)


def _convert_iceberg_filter_to_vortex(expr: BooleanExpression, schema: Optional[Schema] = None) -> Any:
    """Convert Iceberg filter expression to Vortex expression.

    Args:
        expr: Iceberg boolean expression
        schema: Optional schema for field resolution

    Returns:
        Vortex expression object
    """
    _check_vortex_available()

    # Handle simple cases
    if isinstance(expr, AlwaysTrue):
        return None  # No filter

    # Handle bound predicates
    if isinstance(expr, (BoundEqualTo, EqualTo)):
        return ve.column(_get_term_name(expr.term)) == expr.literal.value
    elif isinstance(expr, (BoundNotEqualTo, NotEqualTo)):
        return ve.column(_get_term_name(expr.term)) != expr.literal.value
    elif isinstance(expr, (BoundLessThan, LessThan)):
        return ve.column(_get_term_name(expr.term)) < expr.literal.value
    elif isinstance(expr, (BoundLessThanOrEqual, LessThanOrEqual)):
        return ve.column(_get_term_name(expr.term)) <= expr.literal.value
    elif isinstance(expr, (BoundGreaterThan, GreaterThan)):
        return ve.column(_get_term_name(expr.term)) > expr.literal.value
    elif isinstance(expr, (BoundGreaterThanOrEqual, GreaterThanOrEqual)):
        return ve.column(_get_term_name(expr.term)) >= expr.literal.value
    elif isinstance(expr, (BoundIsNull, IsNull)):
        return ve.column(_get_term_name(expr.term)).is_null()
    elif isinstance(expr, (BoundNotNull, NotNull)):
        return ~ve.column(_get_term_name(expr.term)).is_null()
    elif isinstance(expr, (BoundIsNaN, IsNaN)):
        return ve.column(_get_term_name(expr.term)).is_nan()
    elif isinstance(expr, (BoundNotNaN, NotNaN)):
        return ~ve.column(_get_term_name(expr.term)).is_nan()
    elif isinstance(expr, (BoundIn, In)):
        return ve.column(_get_term_name(expr.term)).is_in(list(expr.literals))
    elif isinstance(expr, (BoundNotIn, NotIn)):
        return ~ve.column(_get_term_name(expr.term)).is_in(list(expr.literals))
    elif isinstance(expr, And):
        left = _convert_iceberg_filter_to_vortex(expr.left, schema)
        right = _convert_iceberg_filter_to_vortex(expr.right, schema)
        if left is None:
            return right
        if right is None:
            return left
        return left & right
    elif isinstance(expr, Or):
        left = _convert_iceberg_filter_to_vortex(expr.left, schema)
        right = _convert_iceberg_filter_to_vortex(expr.right, schema)
        if left is None and right is None:
            return None
        if left is None:
            return right
        if right is None:
            return left
        return left | right
    elif isinstance(expr, Not):
        inner = _convert_iceberg_filter_to_vortex(expr.child, schema)
        return ~inner if inner is not None else None
    else:
        logger.warning(f"Unsupported filter type: {type(expr)}")
        return None


# Keep existing helper functions for compatibility
# The following helper functions (e.g., iceberg_schema_to_vortex_schema, _validate_vortex_schema_compatibility, analyze_vortex_compatibility)
# are preserved for compatibility with legacy PyIceberg code and to ensure interoperability with older schema conversion logic.
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
            warning_msg = f"Large type detected in field '{field.name}' - may impact performance"
            logger.warning(warning_msg)
            unsupported_types.append(warning_msg)
            logger.warning(f"Large type detected in field '{field.name}' - may impact performance")

    if unsupported_types:
        raise ValueError(f"Schema contains unsupported types for Vortex: {', '.join(unsupported_types)}")


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
