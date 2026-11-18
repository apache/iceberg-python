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
"""Computes the partition statistics file of a snapshot, mirroring Java's PartitionStatsHandler."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyiceberg.io.fileformat import FileFormatFactory
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.table.statistics import PartitionStatisticsFile
from pyiceberg.transforms import VoidTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StructType

if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.snapshots import Snapshot


@dataclass
class _PartitionStatistics:
    """The statistics of the files of one partition of one spec."""

    spec_id: int
    partition: tuple[Any, ...]
    data_record_count: int = 0
    data_file_count: int = 0
    total_data_file_size_in_bytes: int = 0
    position_delete_record_count: int = 0
    position_delete_file_count: int = 0
    equality_delete_record_count: int = 0
    equality_delete_file_count: int = 0
    last_updated_at: int | None = None
    last_updated_snapshot_id: int | None = None


def compute_and_write_partition_statistics_file(table: Table, snapshot_id: int) -> PartitionStatisticsFile | None:
    """Compute the partition statistics of a snapshot and write them to a new Parquet file.

    The file is not registered in the table metadata. The statistics are computed from the manifests of the snapshot.

    Args:
        table: The table to compute the statistics for.
        snapshot_id: The ID of the snapshot to compute the statistics for.

    Returns:
        The partition statistics file, or None if the snapshot has no manifest entries.

    Raises:
        ValueError: If the table is not partitioned, the snapshot does not exist, the table's default file format
            has no writer, or historical partition fields have conflicting output names.
        NotImplementedError: If the table is format version 3 or later.
        ArrowNotImplementedError: If the unified partition type contains a UUID field.
    """
    partition_type = _partition_type(table.metadata)
    if not partition_type.fields:
        raise ValueError("Table must be partitioned")
    if table.metadata.format_version >= 3:
        raise NotImplementedError("Partition statistics are not supported for format version 3 tables yet")
    if (snapshot := table.snapshot_by_id(snapshot_id)) is None:
        raise ValueError(f"Snapshot not found: {snapshot_id}")

    rows = _collect_partition_statistics(table, snapshot, partition_type)
    if not rows:
        return None
    # Rows are sorted by partition, NULL first in each field
    rows.sort(key=lambda row: tuple(_partition_value_key(value) for value in row.partition))
    return _write_partition_statistics_file(table, snapshot_id, partition_type, rows)


def _partition_type(metadata: TableMetadata) -> StructType:
    """Return the unified partition type, as Java's Partitioning.partitionType does.

    It holds the partition fields of all specs whose source column is still in the current schema, sorted by field ID.
    Fields of dropped source columns are left out, unlike TableMetadata.specs_struct.
    """
    schema = metadata.schema()
    fields: dict[int, NestedField] = {}
    # Specs are visited in ascending order, so the name from the latest spec wins
    for spec in sorted(metadata.specs().values(), key=lambda spec: spec.spec_id):
        for field in spec.fields:
            if schema.find_column_name(field.source_id) is not None:
                previous = fields.get(field.field_id)
                # A v1 spec drops a partition field by replacing its transform with void, which keeps the output type
                # of the older transform, as Java's Partitioning does
                if isinstance(field.transform, VoidTransform) and previous is not None:
                    result_type = previous.field_type
                else:
                    result_type = field.transform.result_type(schema.find_type(field.source_id))
                fields[field.field_id] = NestedField(field.field_id, field.name, result_type, required=False)
    return StructType(*(fields[field_id] for field_id in sorted(fields)))


def _partition_value_key(value: Any) -> tuple[Any, ...]:
    """Return a key that groups and orders partition values as Iceberg does.

    NULL sorts first, NaN values are equal and sort after every other float, and -0.0 and 0.0 are different values.
    """
    if value is None:
        return (False,)
    if isinstance(value, float):
        if math.isnan(value):
            return (True, True)
        return (True, False, value, math.copysign(1.0, value))
    return (True, value)


def _collect_partition_statistics(table: Table, snapshot: Snapshot, partition_type: StructType) -> list[_PartitionStatistics]:
    """Collect the statistics of all partitions in the manifests of a snapshot, as Java's full compute does.

    Live files add to the counts. Every entry, deleted ones included, can move the last update of its partition forward.
    """
    specs = table.metadata.specs()
    snapshots_by_id = {snap.snapshot_id: snap for snap in table.snapshots()}
    positions = {field.field_id: index for index, field in enumerate(partition_type.fields)}
    stats_by_key: dict[tuple[int, tuple[Any, ...]], _PartitionStatistics] = {}

    for manifest in snapshot.manifests(table.io, table.encryption_manager):
        spec = specs[manifest.partition_spec_id]
        # Maps each partition value of the spec to its position in the unified partition type
        spec_positions = [
            (pos, positions[field.field_id]) for pos, field in enumerate(spec.fields) if field.field_id in positions
        ]
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
            partition: list[Any] = [None] * len(partition_type.fields)
            for pos, index in spec_positions:
                partition[index] = entry.data_file.partition[pos]
            key = (spec.spec_id, tuple(_partition_value_key(value) for value in partition))
            if (stats := stats_by_key.get(key)) is None:
                stats = stats_by_key[key] = _PartitionStatistics(spec.spec_id, tuple(partition))

            if entry.status != ManifestEntryStatus.DELETED:
                _add_live_file(stats, entry.data_file)
            if entry.snapshot_id is not None and (entry_snapshot := snapshots_by_id.get(entry.snapshot_id)) is not None:
                _update_last_updated(stats, entry_snapshot)

    return list(stats_by_key.values())


def _add_live_file(stats: _PartitionStatistics, data_file: DataFile) -> None:
    if data_file.content == DataFileContent.DATA:
        stats.data_record_count += data_file.record_count
        stats.data_file_count += 1
        stats.total_data_file_size_in_bytes += data_file.file_size_in_bytes
    elif data_file.content == DataFileContent.POSITION_DELETES:
        stats.position_delete_record_count += data_file.record_count
        stats.position_delete_file_count += 1
    elif data_file.content == DataFileContent.EQUALITY_DELETES:
        stats.equality_delete_record_count += data_file.record_count
        stats.equality_delete_file_count += 1


def _update_last_updated(stats: _PartitionStatistics, snapshot: Snapshot) -> None:
    if stats.last_updated_at is None or stats.last_updated_at < snapshot.timestamp_ms:
        stats.last_updated_at = snapshot.timestamp_ms
        stats.last_updated_snapshot_id = snapshot.snapshot_id


def _partition_statistics_schema(partition_type: StructType) -> Schema:
    """Return the version 2 schema of the partition statistics file, with the field IDs of the Iceberg spec."""
    return Schema(
        NestedField(1, "partition", partition_type, required=True),
        NestedField(2, "spec_id", IntegerType(), required=True),
        NestedField(3, "data_record_count", LongType(), required=True),
        NestedField(4, "data_file_count", IntegerType(), required=True),
        NestedField(5, "total_data_file_size_in_bytes", LongType(), required=True),
        NestedField(6, "position_delete_record_count", LongType(), required=False),
        NestedField(7, "position_delete_file_count", IntegerType(), required=False),
        NestedField(8, "equality_delete_record_count", LongType(), required=False),
        NestedField(9, "equality_delete_file_count", IntegerType(), required=False),
        NestedField(10, "total_record_count", LongType(), required=False),
        NestedField(11, "last_updated_at", LongType(), required=False),
        NestedField(12, "last_updated_snapshot_id", LongType(), required=False),
    )


def _write_partition_statistics_file(
    table: Table, snapshot_id: int, partition_type: StructType, rows: list[_PartitionStatistics]
) -> PartitionStatisticsFile:
    import pyarrow as pa

    # Importing pyiceberg.io.pyarrow registers the Parquet format model
    from pyiceberg.io.pyarrow import schema_to_pyarrow
    from pyiceberg.table import TableProperties

    file_schema = _partition_statistics_schema(partition_type)
    partition_names = [field.name for field in partition_type.fields]
    arrow_table = pa.Table.from_pylist(
        [
            {
                "partition": dict(zip(partition_names, row.partition, strict=True)),
                "spec_id": row.spec_id,
                "data_record_count": row.data_record_count,
                "data_file_count": row.data_file_count,
                "total_data_file_size_in_bytes": row.total_data_file_size_in_bytes,
                "position_delete_record_count": row.position_delete_record_count,
                "position_delete_file_count": row.position_delete_file_count,
                "equality_delete_record_count": row.equality_delete_record_count,
                "equality_delete_file_count": row.equality_delete_file_count,
                "total_record_count": None,
                "last_updated_at": row.last_updated_at,
                "last_updated_snapshot_id": row.last_updated_snapshot_id,
            }
            for row in rows
        ],
        schema=schema_to_pyarrow(file_schema),
    )

    file_format = FileFormat(table.properties.get(TableProperties.WRITE_FILE_FORMAT, TableProperties.WRITE_FILE_FORMAT_DEFAULT))
    format_model = FileFormatFactory.get(file_format)
    location = table.location_provider().new_metadata_location(
        f"partition-stats-{snapshot_id}-{uuid.uuid4()}.{format_model.file_extension()}"
    )
    output = table.io.new_output(location)
    with format_model.create_writer(output, file_schema, table.properties) as writer:
        writer.write(arrow_table)

    return PartitionStatisticsFile(snapshot_id=snapshot_id, statistics_path=location, file_size_in_bytes=len(output))
