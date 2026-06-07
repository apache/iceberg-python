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
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from pyiceberg.io import FileIO
from pyiceberg.schema import Schema
from pyiceberg.table.statistics import PartitionStatisticsFile
from pyiceberg.typedef import EMPTY_DICT, Properties, Record
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StructType,
)

if TYPE_CHECKING:
    from pyiceberg.manifest import ManifestFile
    from pyiceberg.table.metadata import TableMetadata

# Field positions within the partition statistics record
PARTITION_POSITION = 0
SPEC_ID_POSITION = 1
DATA_RECORD_COUNT_POSITION = 2
DATA_FILE_COUNT_POSITION = 3
TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION = 4
POSITION_DELETE_RECORD_COUNT_POSITION = 5
POSITION_DELETE_FILE_COUNT_POSITION = 6
EQUALITY_DELETE_RECORD_COUNT_POSITION = 7
EQUALITY_DELETE_FILE_COUNT_POSITION = 8
TOTAL_RECORD_COUNT_POSITION = 9
LAST_UPDATED_AT_POSITION = 10
LAST_UPDATED_SNAPSHOT_ID_POSITION = 11
DV_COUNT_POSITION = 12


class PartitionStatistics(Record):
    """Partition statistics for a single partition in an Iceberg table.

    Implements the partition statistics structure as defined in the Iceberg spec.
    Each PartitionStatistics record represents statistics for one unique partition tuple.
    """

    def __init__(self, *data: Any) -> None:
        """Initialize PartitionStatistics with field values.

        Args:
            data: Field values in positional order matching the schema.
        """
        super().__init__(*data)

    @property
    def partition(self) -> Record:
        """Return the partition tuple."""
        return self[PARTITION_POSITION]

    @property
    def spec_id(self) -> int:
        """Return the spec ID of the partition."""
        return self[SPEC_ID_POSITION]

    @property
    def data_record_count(self) -> int:
        """Return the count of data records."""
        return self[DATA_RECORD_COUNT_POSITION]

    @property
    def data_file_count(self) -> int:
        """Return the count of data files."""
        return self[DATA_FILE_COUNT_POSITION]

    @property
    def total_data_file_size_in_bytes(self) -> int:
        """Return the total size of data files in bytes."""
        return self[TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION]

    @property
    def position_delete_record_count(self) -> int | None:
        """Return the count of position delete records (includes deletion vectors)."""
        return self[POSITION_DELETE_RECORD_COUNT_POSITION]

    @property
    def position_delete_file_count(self) -> int | None:
        """Return the count of position delete files (excludes deletion vectors)."""
        return self[POSITION_DELETE_FILE_COUNT_POSITION]

    @property
    def equality_delete_record_count(self) -> int | None:
        """Return the count of equality delete records."""
        return self[EQUALITY_DELETE_RECORD_COUNT_POSITION]

    @property
    def equality_delete_file_count(self) -> int | None:
        """Return the count of equality delete files."""
        return self[EQUALITY_DELETE_FILE_COUNT_POSITION]

    @property
    def total_record_count(self) -> int | None:
        """Return the total count of records after applying deletes."""
        return self[TOTAL_RECORD_COUNT_POSITION]

    @property
    def last_updated_at(self) -> int | None:
        """Return the timestamp in milliseconds when partition was last updated."""
        return self[LAST_UPDATED_AT_POSITION]

    @property
    def last_updated_snapshot_id(self) -> int | None:
        """Return the snapshot ID that last updated this partition."""
        return self[LAST_UPDATED_SNAPSHOT_ID_POSITION]

    @property
    def dv_count(self) -> int | None:
        """Return the count of deletion vectors (v3+ only)."""
        if len(self._data) > DV_COUNT_POSITION:
            return self[DV_COUNT_POSITION]
        return None

    def set(self, pos: int, value: Any) -> None:
        """Set a value at the specified position."""
        self[pos] = value

    def __repr__(self) -> str:
        """Return string representation."""
        return (
            f"PartitionStatistics(spec_id={self.spec_id}, "
            f"data_record_count={self.data_record_count}, "
            f"data_file_count={self.data_file_count})"
        )


def partition_statistics_schema(unified_partition_type: StructType, format_version: int) -> Schema:
    """Generate the schema for partition statistics based on format version.

    Args:
        unified_partition_type: The unified partition type (union of all partition specs).
        format_version: The table format version (1, 2, or 3).

    Returns:
        Schema for partition statistics records.

    Raises:
        ValueError: If the table is unpartitioned or format version is invalid.
    """
    if not unified_partition_type.fields:
        raise ValueError("Table must be partitioned")
    if format_version < 1:
        raise ValueError(f"Invalid format version: {format_version}")

    if format_version <= 2:
        return _v2_schema(unified_partition_type)
    return _v3_schema(unified_partition_type)


def _v2_schema(unified_partition_type: StructType) -> Schema:
    """Generate v2 schema (optional delete fields)."""
    return Schema(
        NestedField(field_id=1, name="partition", field_type=unified_partition_type, required=True),
        NestedField(field_id=2, name="spec_id", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="data_record_count", field_type=LongType(), required=True),
        NestedField(field_id=4, name="data_file_count", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="total_data_file_size_in_bytes", field_type=LongType(), required=True),
        NestedField(field_id=6, name="position_delete_record_count", field_type=LongType(), required=False),
        NestedField(field_id=7, name="position_delete_file_count", field_type=IntegerType(), required=False),
        NestedField(field_id=8, name="equality_delete_record_count", field_type=LongType(), required=False),
        NestedField(field_id=9, name="equality_delete_file_count", field_type=IntegerType(), required=False),
        NestedField(field_id=10, name="total_record_count", field_type=LongType(), required=False),
        NestedField(field_id=11, name="last_updated_at", field_type=LongType(), required=False),
        NestedField(field_id=12, name="last_updated_snapshot_id", field_type=LongType(), required=False),
    )


def _v3_schema(unified_partition_type: StructType) -> Schema:
    """Generate v3 schema (required delete fields + dv_count)."""
    return Schema(
        NestedField(field_id=1, name="partition", field_type=unified_partition_type, required=True),
        NestedField(field_id=2, name="spec_id", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="data_record_count", field_type=LongType(), required=True),
        NestedField(field_id=4, name="data_file_count", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="total_data_file_size_in_bytes", field_type=LongType(), required=True),
        NestedField(field_id=6, name="position_delete_record_count", field_type=LongType(), required=True),
        NestedField(field_id=7, name="position_delete_file_count", field_type=IntegerType(), required=True),
        NestedField(field_id=8, name="equality_delete_record_count", field_type=LongType(), required=True),
        NestedField(field_id=9, name="equality_delete_file_count", field_type=IntegerType(), required=True),
        NestedField(field_id=10, name="total_record_count", field_type=LongType(), required=False),
        NestedField(field_id=11, name="last_updated_at", field_type=LongType(), required=False),
        NestedField(field_id=12, name="last_updated_snapshot_id", field_type=LongType(), required=False),
        NestedField(
            field_id=13,
            name="dv_count",
            field_type=IntegerType(),
            required=True,
            initial_default=0,
            write_default=0,
        ),
    )


class PartitionStatisticsScan(ABC):
    """Scan partition statistics from a statistics file."""

    table_metadata: TableMetadata
    io: FileIO
    snapshot_id: int | None
    options: Properties

    def __init__(
        self,
        table_metadata: TableMetadata,
        io: FileIO,
        snapshot_id: int | None = None,
        options: Properties = EMPTY_DICT,
    ):
        """Initialize partition statistics scan.

        Args:
            table_metadata: Table metadata containing partition statistics files.
            io: FileIO for reading statistics files.
            snapshot_id: Optional snapshot ID to scan statistics for.
            options: Scan options.
        """
        self.table_metadata = table_metadata
        self.io = io
        self.snapshot_id = snapshot_id
        self.options = options

    def use_snapshot(self, snapshot_id: int) -> PartitionStatisticsScan:
        """Set snapshot ID for this scan.

        Args:
            snapshot_id: Snapshot ID to scan.

        Returns:
            Updated scan instance.
        """
        return type(self)(
            table_metadata=self.table_metadata,
            io=self.io,
            snapshot_id=snapshot_id,
            options=self.options,
        )

    def _find_stats_file(self) -> PartitionStatisticsFile | None:
        """Find partition statistics file for snapshot."""
        target_snapshot_id = self.snapshot_id or self.table_metadata.current_snapshot_id
        if target_snapshot_id is None:
            return None

        for stats_file in self.table_metadata.partition_statistics:
            if stats_file.snapshot_id == target_snapshot_id:
                return stats_file
        return None

    @abstractmethod
    def scan(self) -> Iterable[PartitionStatistics]:
        """Scan partition statistics records."""
        ...


class PartitionStatisticsScanArrow(PartitionStatisticsScan):
    """Arrow-based partition statistics scan."""

    def scan(self) -> Iterator[PartitionStatistics]:
        """Read partition statistics from Parquet/ORC file using Arrow.

        Yields:
            PartitionStatistics records.
        """
        stats_file = self._find_stats_file()
        if not stats_file:
            return

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # Get unified partition type from all specs
        unified_partition_type = _compute_unified_partition_type(self.table_metadata)
        if not unified_partition_type.fields:
            return

        # Build schema for reading
        format_version = self.table_metadata.format_version
        read_schema = partition_statistics_schema(unified_partition_type, format_version)
        pyarrow_schema = schema_to_pyarrow(read_schema)

        # Read statistics file
        input_file = self.io.new_input(stats_file.statistics_path)

        # Read Parquet file directly
        import pyarrow.parquet as pq

        arrow_table = pq.read_table(input_file.open(), schema=pyarrow_schema)

        # Convert Arrow table to PartitionStatistics records
        for batch in arrow_table.to_batches():
            for row_idx in range(len(batch)):
                row_data = []
                for col in batch.columns:
                    value = col[row_idx].as_py()
                    row_data.append(value)
                yield PartitionStatistics(*row_data)


def _compute_unified_partition_type(table_metadata: TableMetadata) -> StructType:
    """Compute unified partition type from all specs.

    Returns a struct containing union of all partition fields across specs,
    sorted by field ID.

    Args:
        table_metadata: Table metadata.

    Returns:
        Unified partition type struct.
    """
    from pyiceberg.partitioning import PartitionSpec

    all_fields: dict[int, NestedField] = {}

    for spec in table_metadata.partition_specs:
        if isinstance(spec, PartitionSpec):
            partition_type = spec.partition_type(table_metadata.schema())
            for field in partition_type.fields:
                if field.field_id not in all_fields:
                    all_fields[field.field_id] = field

    # Sort by field ID
    sorted_fields = sorted(all_fields.values(), key=lambda f: f.field_id)
    return StructType(*sorted_fields)


def write_partition_statistics_file(
    io: FileIO,
    table_metadata: TableMetadata,
    snapshot_id: int,
    statistics: Iterable[PartitionStatistics],
) -> PartitionStatisticsFile:
    """Write partition statistics to a file.

    Args:
        io: FileIO for writing.
        table_metadata: Table metadata.
        snapshot_id: Snapshot ID for the statistics.
        statistics: Partition statistics records to write.

    Returns:
        PartitionStatisticsFile metadata.

    Raises:
        ValueError: If table is unpartitioned.
    """
    import pyarrow.parquet as pq

    from pyiceberg.io.pyarrow import schema_to_pyarrow

    # Get unified partition type and schema
    unified_partition_type = _compute_unified_partition_type(table_metadata)
    if not unified_partition_type.fields:
        raise ValueError("Table must be partitioned")

    format_version = table_metadata.format_version
    write_schema = partition_statistics_schema(unified_partition_type, format_version)

    # Convert to Arrow table
    arrow_schema = schema_to_pyarrow(write_schema)

    # Convert stats to dicts for Arrow
    record_dicts = []
    for stat in statistics:
        # Convert partition Record to dict
        partition_dict = {}
        partition_record = stat.partition
        for idx, field in enumerate(unified_partition_type.fields):
            partition_dict[field.name] = partition_record[idx]

        # Build full record dict
        record_dicts.append(
            {
                "partition": partition_dict,
                "spec_id": stat.spec_id,
                "data_record_count": stat.data_record_count,
                "data_file_count": stat.data_file_count,
                "total_data_file_size_in_bytes": stat.total_data_file_size_in_bytes,
                "position_delete_record_count": stat.position_delete_record_count,
                "position_delete_file_count": stat.position_delete_file_count,
                "equality_delete_record_count": stat.equality_delete_record_count,
                "equality_delete_file_count": stat.equality_delete_file_count,
                "total_record_count": stat.total_record_count,
                "last_updated_at": stat.last_updated_at,
                "last_updated_snapshot_id": stat.last_updated_snapshot_id,
                **({"dv_count": stat.dv_count} if format_version >= 3 else {}),
            }
        )

    arrow_table = pa.Table.from_pylist(record_dicts, schema=arrow_schema)

    # Sort by partition field (ascending, NULL FIRST per spec)
    # Arrow sorts NULL first by default for ascending
    arrow_table = arrow_table.sort_by([("partition", "ascending")])

    # Generate file path
    file_format = table_metadata.properties.get("write.data.default-file-format", "parquet")
    file_path = _new_partition_stats_path(table_metadata, snapshot_id, file_format)

    # Write to Parquet
    output_file = io.new_output(file_path)
    with output_file.create(overwrite=True) as output_stream:
        with pq.ParquetWriter(output_stream, schema=arrow_schema) as writer:
            writer.write_table(arrow_table)

    # Get file size
    file_size = len(output_file)

    return PartitionStatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path=file_path,
        file_size_in_bytes=file_size,
    )


def _new_partition_stats_path(table_metadata: TableMetadata, snapshot_id: int, file_format: str) -> str:
    """Generate path for new partition statistics file.

    Args:
        table_metadata: Table metadata.
        snapshot_id: Snapshot ID.
        file_format: File format extension.

    Returns:
        Full path for statistics file.
    """
    import uuid

    filename = f"partition-stats-{snapshot_id}-{uuid.uuid4()}.{file_format}"
    metadata_location = table_metadata.location
    return f"{metadata_location}/metadata/{filename}"


def compute_partition_statistics(
    io: FileIO,
    table_metadata: TableMetadata,
    snapshot_id: int | None = None,
) -> dict[tuple[int, tuple[Any, ...]], PartitionStatistics]:
    """Compute partition statistics for a snapshot.

    Args:
        io: FileIO for reading manifests.
        table_metadata: Table metadata.
        snapshot_id: Snapshot ID to compute stats for. Uses current if None.

    Returns:
        Dict mapping (spec_id, partition_tuple) to PartitionStatistics.

    Raises:
        ValueError: If snapshot not found or table unpartitioned.
    """
    from pyiceberg.manifest import read_manifest_list

    # Get snapshot
    if snapshot_id is None:
        snapshot = table_metadata.current_snapshot()
    else:
        snapshot = table_metadata.snapshot_by_id(snapshot_id)

    if snapshot is None:
        raise ValueError(f"Snapshot not found: {snapshot_id}")

    # Check table is partitioned
    unified_partition_type = _compute_unified_partition_type(table_metadata)
    if not unified_partition_type.fields:
        raise ValueError("Table must be partitioned")

    # Collect stats from all manifests
    stats_map: dict[tuple[int, tuple[Any, ...]], PartitionStatistics] = {}

    manifest_list = read_manifest_list(
        input_file=io.new_input(snapshot.manifest_list),
    )

    for manifest_file in manifest_list:
        _collect_stats_from_manifest(
            io=io,
            table_metadata=table_metadata,
            manifest_file=manifest_file,
            unified_partition_type=unified_partition_type,
            stats_map=stats_map,
            snapshot=snapshot,
        )

    return stats_map


def _collect_stats_from_manifest(
    io: FileIO,
    table_metadata: TableMetadata,
    manifest_file: ManifestFile,
    unified_partition_type: StructType,
    stats_map: dict[tuple[int, tuple[Any, ...]], PartitionStatistics],
    snapshot: Any,
) -> None:
    """Collect statistics from a single manifest file.

    Args:
        io: FileIO for reading.
        table_metadata: Table metadata.
        manifest_file: Manifest file to process.
        unified_partition_type: Unified partition type.
        stats_map: Stats map to update (modified in-place).
        snapshot: Snapshot for timestamp info.
    """
    from pyiceberg.manifest import DataFileContent

    spec_id = manifest_file.partition_spec_id

    for entry in manifest_file.fetch_manifest_entry(io, discard_deleted=True):
        data_file = entry.data_file
        partition_key = _partition_to_tuple(data_file.partition)
        key = (spec_id, partition_key)

        # Initialize or get existing stats
        if key not in stats_map:
            stats_map[key] = _create_empty_stats(
                unified_partition_type=unified_partition_type,
                spec_id=spec_id,
                partition=data_file.partition,
            )

        stats = stats_map[key]

        # Update counts based on file content
        if data_file.content == DataFileContent.DATA:
            stats.set(DATA_RECORD_COUNT_POSITION, stats.data_record_count + data_file.record_count)
            stats.set(DATA_FILE_COUNT_POSITION, stats.data_file_count + 1)
            stats.set(
                TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION,
                stats.total_data_file_size_in_bytes + data_file.file_size_in_bytes,
            )
        elif data_file.content == DataFileContent.POSITION_DELETES:
            current = stats.position_delete_record_count or 0
            stats.set(POSITION_DELETE_RECORD_COUNT_POSITION, current + data_file.record_count)
            current_count = stats.position_delete_file_count or 0
            stats.set(POSITION_DELETE_FILE_COUNT_POSITION, current_count + 1)
        elif data_file.content == DataFileContent.EQUALITY_DELETES:
            current = stats.equality_delete_record_count or 0
            stats.set(EQUALITY_DELETE_RECORD_COUNT_POSITION, current + data_file.record_count)
            current_count = stats.equality_delete_file_count or 0
            stats.set(EQUALITY_DELETE_FILE_COUNT_POSITION, current_count + 1)

        # Update snapshot info
        if snapshot:
            _update_snapshot_info(stats, snapshot.snapshot_id, snapshot.timestamp_ms)


def _create_empty_stats(
    unified_partition_type: StructType,
    spec_id: int,
    partition: Record,
) -> PartitionStatistics:
    """Create empty partition statistics record.

    Args:
        unified_partition_type: Unified partition type.
        spec_id: Partition spec ID.
        partition: Partition data.

    Returns:
        New PartitionStatistics with zeroed counts.
    """
    # Initialize all fields
    data = [
        partition,  # partition
        spec_id,  # spec_id
        0,  # data_record_count
        0,  # data_file_count
        0,  # total_data_file_size_in_bytes
        0,  # position_delete_record_count
        0,  # position_delete_file_count
        0,  # equality_delete_record_count
        0,  # equality_delete_file_count
        None,  # total_record_count
        None,  # last_updated_at
        None,  # last_updated_snapshot_id
        0,  # dv_count (v3+)
    ]
    return PartitionStatistics(*data)


def _partition_to_tuple(partition: Record) -> tuple[Any, ...]:
    """Convert partition Record to hashable tuple.

    Args:
        partition: Partition record.

    Returns:
        Tuple of partition values.
    """
    return tuple(partition._data)


def _update_snapshot_info(stats: PartitionStatistics, snapshot_id: int, timestamp_ms: int) -> None:
    """Update snapshot tracking fields in stats.

    Args:
        stats: Stats to update.
        snapshot_id: Snapshot ID.
        timestamp_ms: Timestamp in milliseconds.
    """
    last_updated = stats.last_updated_at
    if last_updated is None or last_updated < timestamp_ms:
        stats.set(LAST_UPDATED_AT_POSITION, timestamp_ms)
        stats.set(LAST_UPDATED_SNAPSHOT_ID_POSITION, snapshot_id)
