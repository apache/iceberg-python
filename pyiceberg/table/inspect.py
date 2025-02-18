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

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple

import pyarrow as pa

from pyiceberg.conversions import from_bytes
from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent, PartitionFieldSummary
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.snapshots import Snapshot, ancestors_of
from pyiceberg.types import PrimitiveType
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.singleton import _convert_to_hashable_type

if TYPE_CHECKING:
    from pyiceberg.table import Table


class InspectTable:
    """A utility class for inspecting and analyzing Iceberg table metadata.

    This class provides methods to extract and analyze information such as:
                     - Snapshots and their metadata.
                     - Manifests and partition data.
                     - Table history, references, and file-level statistics.

    Attributes:
                     tbl (Table): The Iceberg table instance to inspect.


    """

    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

        try:
            import pyarrow as pa  # noqa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For metadata operations PyArrow needs to be installed") from e

    def _get_snapshot(self, snapshot_id: Optional[int] = None) -> Snapshot:
        """Retrieve a specific snapshot or the current snapshot of the Iceberg table.

        This method allows "time travel" to a specific snapshot by providing its ID.
        If no `snapshot_id` is provided, the method returns the current snapshot
        of the table.

        Args:
        snapshot_id (Optional[int]): The ID of the snapshot to retrieve.
        If None, the current snapshot is returned.

        Returns:
        Snapshot: The requested snapshot instance.

        Raises:
        ValueError: If the specified snapshot ID is not found or if the table
        does not have any snapshots.
        """
        if snapshot_id is not None:
            if snapshot := self.tbl.metadata.snapshot_by_id(snapshot_id):
                return snapshot
            else:
                raise ValueError(f"Cannot find snapshot with ID {snapshot_id}")

        if snapshot := self.tbl.metadata.current_snapshot():
            return snapshot
        else:
            raise ValueError("Cannot get a snapshot as the table does not have any.")

    def snapshots(self) -> "pa.Table":
        """Generate a table of all snapshots in the Iceberg table.

         This method retrieves metadata about all snapshots stored in the Iceberg table,
         including details such as timestamps, snapshot IDs, and parent-child relationships.

        Returns:
         pa.Table: A PyArrow table containing metadata for all snapshots, including fields like:
           - committed_at: Timestamp when the snapshot was committed.
           - snapshot_id: Unique ID of the snapshot.
           - parent_id: ID of the parent snapshot (if any).
           - operation: Type of operation performed (e.g., append, overwrite).
           - manifest_list: Path to the manifest list file.
           - summary: Additional metadata properties.
        """

        snapshots_schema = pa.schema(
            [
                pa.field("committed_at", pa.timestamp(unit="ms"), nullable=False),
                pa.field("snapshot_id", pa.int64(), nullable=False),
                pa.field("parent_id", pa.int64(), nullable=True),
                pa.field("operation", pa.string(), nullable=True),
                pa.field("manifest_list", pa.string(), nullable=False),
                pa.field("summary", pa.map_(pa.string(), pa.string()), nullable=True),
            ]
        )
        snapshots = []
        for snapshot in self.tbl.metadata.snapshots:
            if summary := snapshot.summary:
                operation = summary.operation.value
                additional_properties = snapshot.summary.additional_properties
            else:
                operation = None
                additional_properties = None

            snapshots.append(
                {
                    "committed_at": datetime.fromtimestamp(snapshot.timestamp_ms / 1000.0, tz=timezone.utc),
                    "snapshot_id": snapshot.snapshot_id,
                    "parent_id": snapshot.parent_snapshot_id,
                    "operation": str(operation),
                    "manifest_list": snapshot.manifest_list,
                    "summary": additional_properties,
                }
            )

        return pa.Table.from_pylist(
            snapshots,
            schema=snapshots_schema,
        )

    def entries(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        """Generate a table of manifest entries for a specific snapshot.

        This method retrieves metadata for all manifest entries within a given snapshot,
        including file-level statistics such as column sizes and value counts.

        Args:
           snapshot_id (Optional[int]): The ID of the snapshot to retrieve entries for.
             If None, entries for the current snapshot are returned.

        Returns:
          pa.Table: A PyArrow table containing manifest entries, including fields like:
           - status: Status of the entry (e.g., added, existing, deleted).
           - snapshot_id: The ID of the snapshot containing the entry.
           - file details: Metadata such as file path, format, size, and partition."""

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = self.tbl.metadata.schema()

        readable_metrics_struct = []

        def _readable_metrics_struct(bound_type: PrimitiveType) -> pa.StructType:
            pa_bound_type = schema_to_pyarrow(bound_type)
            return pa.struct(
                [
                    pa.field("column_size", pa.int64(), nullable=True),
                    pa.field("value_count", pa.int64(), nullable=True),
                    pa.field("null_value_count", pa.int64(), nullable=True),
                    pa.field("nan_value_count", pa.int64(), nullable=True),
                    pa.field("lower_bound", pa_bound_type, nullable=True),
                    pa.field("upper_bound", pa_bound_type, nullable=True),
                ]
            )

        for field in self.tbl.metadata.schema().fields:
            readable_metrics_struct.append(
                pa.field(schema.find_column_name(field.field_id), _readable_metrics_struct(field.field_type), nullable=False)
            )

        partition_record = self.tbl.metadata.specs_struct()
        pa_record_struct = schema_to_pyarrow(partition_record)

        entries_schema = pa.schema(
            [
                pa.field("status", pa.int8(), nullable=False),
                pa.field("snapshot_id", pa.int64(), nullable=False),
                pa.field("sequence_number", pa.int64(), nullable=False),
                pa.field("file_sequence_number", pa.int64(), nullable=False),
                pa.field(
                    "data_file",
                    pa.struct(
                        [
                            pa.field("content", pa.int8(), nullable=False),
                            pa.field("file_path", pa.string(), nullable=False),
                            pa.field("file_format", pa.string(), nullable=False),
                            pa.field("partition", pa_record_struct, nullable=False),
                            pa.field("record_count", pa.int64(), nullable=False),
                            pa.field("file_size_in_bytes", pa.int64(), nullable=False),
                            pa.field("column_sizes", pa.map_(pa.int32(), pa.int64()), nullable=True),
                            pa.field("value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                            pa.field("null_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                            pa.field("nan_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                            pa.field("lower_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
                            pa.field("upper_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
                            pa.field("key_metadata", pa.binary(), nullable=True),
                            pa.field("split_offsets", pa.list_(pa.int64()), nullable=True),
                            pa.field("equality_ids", pa.list_(pa.int32()), nullable=True),
                            pa.field("sort_order_id", pa.int32(), nullable=True),
                        ]
                    ),
                    nullable=False,
                ),
                pa.field("readable_metrics", pa.struct(readable_metrics_struct), nullable=True),
            ]
        )

        entries = []
        snapshot = self._get_snapshot(snapshot_id)
        for manifest in snapshot.manifests(self.tbl.io):
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io):
                column_sizes = entry.data_file.column_sizes or {}
                value_counts = entry.data_file.value_counts or {}
                null_value_counts = entry.data_file.null_value_counts or {}
                nan_value_counts = entry.data_file.nan_value_counts or {}
                lower_bounds = entry.data_file.lower_bounds or {}
                upper_bounds = entry.data_file.upper_bounds or {}
                readable_metrics = {
                    schema.find_column_name(field.field_id): {
                        "column_size": column_sizes.get(field.field_id),
                        "value_count": value_counts.get(field.field_id),
                        "null_value_count": null_value_counts.get(field.field_id),
                        "nan_value_count": nan_value_counts.get(field.field_id),
                        # Makes them readable
                        "lower_bound": from_bytes(field.field_type, lower_bound)
                        if (lower_bound := lower_bounds.get(field.field_id))
                        else None,
                        "upper_bound": from_bytes(field.field_type, upper_bound)
                        if (upper_bound := upper_bounds.get(field.field_id))
                        else None,
                    }
                    for field in self.tbl.metadata.schema().fields
                }

                partition = entry.data_file.partition
                partition_record_dict = {
                    field.name: partition[pos]
                    for pos, field in enumerate(self.tbl.metadata.specs()[manifest.partition_spec_id].fields)
                }

                entries.append(
                    {
                        "status": entry.status.value,
                        "snapshot_id": entry.snapshot_id,
                        "sequence_number": entry.sequence_number,
                        "file_sequence_number": entry.file_sequence_number,
                        "data_file": {
                            "content": entry.data_file.content,
                            "file_path": entry.data_file.file_path,
                            "file_format": entry.data_file.file_format,
                            "partition": partition_record_dict,
                            "record_count": entry.data_file.record_count,
                            "file_size_in_bytes": entry.data_file.file_size_in_bytes,
                            "column_sizes": dict(entry.data_file.column_sizes),
                            "value_counts": dict(entry.data_file.value_counts),
                            "null_value_counts": dict(entry.data_file.null_value_counts),
                            "nan_value_counts": dict(entry.data_file.nan_value_counts),
                            "lower_bounds": entry.data_file.lower_bounds,
                            "upper_bounds": entry.data_file.upper_bounds,
                            "key_metadata": entry.data_file.key_metadata,
                            "split_offsets": entry.data_file.split_offsets,
                            "equality_ids": entry.data_file.equality_ids,
                            "sort_order_id": entry.data_file.sort_order_id,
                            "spec_id": entry.data_file.spec_id,
                        },
                        "readable_metrics": readable_metrics,
                    }
                )

        return pa.Table.from_pylist(
            entries,
            schema=entries_schema,
        )

    def refs(self) -> "pa.Table":
        """Retrieve references from the Iceberg table metadata as a PyArrow Table.

        This method extracts reference information, such as branch or tag details,
        snapshot IDs, and associated configuration parameters.

        Returns:
         pa.Table: A PyArrow table with reference details, including:
           - name: Name of the reference.
           - type: Type of reference (branch or tag).
           - snapshot_id: ID of the associated snapshot.
           - max_reference_age_in_ms: Maximum age of the reference in milliseconds.
           - min_snapshots_to_keep: Minimum number of snapshots to retain.
           - max_snapshot_age_in_ms: Maximum age of snapshots in milliseconds.
        """

        ref_schema = pa.schema(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("type", pa.dictionary(pa.int32(), pa.string()), nullable=False),
                pa.field("snapshot_id", pa.int64(), nullable=False),
                pa.field("max_reference_age_in_ms", pa.int64(), nullable=True),
                pa.field("min_snapshots_to_keep", pa.int32(), nullable=True),
                pa.field("max_snapshot_age_in_ms", pa.int64(), nullable=True),
            ]
        )

        ref_results = []
        for ref in self.tbl.metadata.refs:
            if snapshot_ref := self.tbl.metadata.refs.get(ref):
                ref_results.append(
                    {
                        "name": ref,
                        "type": snapshot_ref.snapshot_ref_type.upper(),
                        "snapshot_id": snapshot_ref.snapshot_id,
                        "max_reference_age_in_ms": snapshot_ref.max_ref_age_ms,
                        "min_snapshots_to_keep": snapshot_ref.min_snapshots_to_keep,
                        "max_snapshot_age_in_ms": snapshot_ref.max_snapshot_age_ms,
                    }
                )

        return pa.Table.from_pylist(ref_results, schema=ref_schema)

    def partitions(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        """Retrieve partition information from the Iceberg table as a PyArrow Table.

        This method aggregates metadata and statistics for table partitions, enabling insights
        into partition-level details like record counts and file sizes. If a `snapshot_id`
        is provided, historical data can be retrieved from that specific snapshot.

        Args:
         snapshot_id (Optional[int]): The ID of the snapshot to retrieve partition
         information from. If None, the current snapshot is used.

        Returns:
            pa.Table: A PyArrow table containing partition metadata, including:
               - record_count: Number of records in the partition.
               - file_count: Number of files in the partition.
               - total_data_file_size_in_bytes: Total size of data files in bytes.
               - other partition-level metrics and timestamps."""

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        table_schema = pa.schema(
            [
                pa.field("record_count", pa.int64(), nullable=False),
                pa.field("file_count", pa.int32(), nullable=False),
                pa.field("total_data_file_size_in_bytes", pa.int64(), nullable=False),
                pa.field("position_delete_record_count", pa.int64(), nullable=False),
                pa.field("position_delete_file_count", pa.int32(), nullable=False),
                pa.field("equality_delete_record_count", pa.int64(), nullable=False),
                pa.field("equality_delete_file_count", pa.int32(), nullable=False),
                pa.field("last_updated_at", pa.timestamp(unit="ms"), nullable=True),
                pa.field("last_updated_snapshot_id", pa.int64(), nullable=True),
            ]
        )

        partition_record = self.tbl.metadata.specs_struct()
        has_partitions = len(partition_record.fields) > 0

        if has_partitions:
            pa_record_struct = schema_to_pyarrow(partition_record)
            partitions_schema = pa.schema(
                [
                    pa.field("partition", pa_record_struct, nullable=False),
                    pa.field("spec_id", pa.int32(), nullable=False),
                ]
            )

            table_schema = pa.unify_schemas([partitions_schema, table_schema])

        def update_partitions_map(
            partitions_map: Dict[Tuple[str, Any], Any],
            file: DataFile,
            partition_record_dict: Dict[str, Any],
            snapshot: Optional[Snapshot],
        ) -> None:
            partition_record_key = _convert_to_hashable_type(partition_record_dict)
            if partition_record_key not in partitions_map:
                partitions_map[partition_record_key] = {
                    "partition": partition_record_dict,
                    "spec_id": file.spec_id,
                    "record_count": 0,
                    "file_count": 0,
                    "total_data_file_size_in_bytes": 0,
                    "position_delete_record_count": 0,
                    "position_delete_file_count": 0,
                    "equality_delete_record_count": 0,
                    "equality_delete_file_count": 0,
                    "last_updated_at": snapshot.timestamp_ms if snapshot else None,
                    "last_updated_snapshot_id": snapshot.snapshot_id if snapshot else None,
                }

            partition_row = partitions_map[partition_record_key]

            if snapshot is not None:
                if partition_row["last_updated_at"] is None or partition_row["last_updated_snapshot_id"] < snapshot.timestamp_ms:
                    partition_row["last_updated_at"] = snapshot.timestamp_ms
                    partition_row["last_updated_snapshot_id"] = snapshot.snapshot_id

            if file.content == DataFileContent.DATA:
                partition_row["record_count"] += file.record_count
                partition_row["file_count"] += 1
                partition_row["total_data_file_size_in_bytes"] += file.file_size_in_bytes
            elif file.content == DataFileContent.POSITION_DELETES:
                partition_row["position_delete_record_count"] += file.record_count
                partition_row["position_delete_file_count"] += 1
            elif file.content == DataFileContent.EQUALITY_DELETES:
                partition_row["equality_delete_record_count"] += file.record_count
                partition_row["equality_delete_file_count"] += 1
            else:
                raise ValueError(f"Unknown DataFileContent ({file.content})")

        partitions_map: Dict[Tuple[str, Any], Any] = {}
        snapshot = self._get_snapshot(snapshot_id)
        for manifest in snapshot.manifests(self.tbl.io):
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io):
                partition = entry.data_file.partition
                partition_record_dict = {
                    field.name: partition[pos]
                    for pos, field in enumerate(self.tbl.metadata.specs()[manifest.partition_spec_id].fields)
                }
                entry_snapshot = self.tbl.snapshot_by_id(entry.snapshot_id) if entry.snapshot_id is not None else None
                update_partitions_map(partitions_map, entry.data_file, partition_record_dict, entry_snapshot)

        return pa.Table.from_pylist(
            partitions_map.values(),
            schema=table_schema,
        )

    def _get_manifests_schema(self) -> "pa.Schema":
        """Define the schema for manifest data in the Iceberg table.

        This schema specifies the structure of manifest metadata, including fields
        for partition summaries and file-level statistics.

        Returns:
           pa.Schema: The schema for manifest data, with fields like:
               - content: Content type of the manifest (data or deletes).
               - path: Path to the manifest file.
               - length: Size of the manifest file in bytes.
               - partition summaries: Metadata summarizing partition details"""

        partition_summary_schema = pa.struct(
            [
                pa.field("contains_null", pa.bool_(), nullable=False),
                pa.field("contains_nan", pa.bool_(), nullable=True),
                pa.field("lower_bound", pa.string(), nullable=True),
                pa.field("upper_bound", pa.string(), nullable=True),
            ]
        )

        manifest_schema = pa.schema(
            [
                pa.field("content", pa.int8(), nullable=False),
                pa.field("path", pa.string(), nullable=False),
                pa.field("length", pa.int64(), nullable=False),
                pa.field("partition_spec_id", pa.int32(), nullable=False),
                pa.field("added_snapshot_id", pa.int64(), nullable=False),
                pa.field("added_data_files_count", pa.int32(), nullable=False),
                pa.field("existing_data_files_count", pa.int32(), nullable=False),
                pa.field("deleted_data_files_count", pa.int32(), nullable=False),
                pa.field("added_delete_files_count", pa.int32(), nullable=False),
                pa.field("existing_delete_files_count", pa.int32(), nullable=False),
                pa.field("deleted_delete_files_count", pa.int32(), nullable=False),
                pa.field("partition_summaries", pa.list_(partition_summary_schema), nullable=False),
            ]
        )
        return manifest_schema

    def _get_all_manifests_schema(self) -> "pa.Schema":
        """Extend the manifest schema to include additional fields.

        This method adds fields like reference snapshot IDs to the standard manifest schema,
        enabling support for tables with historical and branching capabilities.

        Returns:
         pa.Schema: The extended manifest schema, including additional fields for:
           - reference_snapshot_id: ID of the snapshot referencing the manifest."""

        all_manifests_schema = self._get_manifests_schema()
        all_manifests_schema = all_manifests_schema.append(pa.field("reference_snapshot_id", pa.int64(), nullable=False))
        return all_manifests_schema

    def _generate_manifests_table(self, snapshot: Optional[Snapshot], is_all_manifests_table: bool = False) -> "pa.Table":
        """Generate a table of manifests with detailed metadata.

         This method creates a structured table containing manifest metadata, including
         partition summaries, file counts, and content type. It supports filtering by
         snapshot and aggregating data for all manifests.

        Args:
            snapshot (Optional[Snapshot]): The snapshot for which manifests are generated.
            is_all_manifests_table (bool): Whether to include data for all manifests,
            including reference snapshots.

        Returns:
         pa.Table: A PyArrow table containing manifest details.
        """

        def _partition_summaries_to_rows(
            spec: PartitionSpec, partition_summaries: List[PartitionFieldSummary]
        ) -> List[Dict[str, Any]]:
            rows = []
            for i, field_summary in enumerate(partition_summaries):
                field = spec.fields[i]
                partition_field_type = spec.partition_type(self.tbl.schema()).fields[i].field_type
                lower_bound = (
                    (
                        field.transform.to_human_string(
                            partition_field_type, from_bytes(partition_field_type, field_summary.lower_bound)
                        )
                    )
                    if field_summary.lower_bound
                    else None
                )
                upper_bound = (
                    (
                        field.transform.to_human_string(
                            partition_field_type, from_bytes(partition_field_type, field_summary.upper_bound)
                        )
                    )
                    if field_summary.upper_bound
                    else None
                )
                rows.append(
                    {
                        "contains_null": field_summary.contains_null,
                        "contains_nan": field_summary.contains_nan,
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound,
                    }
                )
            return rows

        specs = self.tbl.metadata.specs()
        manifests = []
        if snapshot:
            for manifest in snapshot.manifests(self.tbl.io):
                is_data_file = manifest.content == ManifestContent.DATA
                is_delete_file = manifest.content == ManifestContent.DELETES
                manifest_row = {
                    "content": manifest.content,
                    "path": manifest.manifest_path,
                    "length": manifest.manifest_length,
                    "partition_spec_id": manifest.partition_spec_id,
                    "added_snapshot_id": manifest.added_snapshot_id,
                    "added_data_files_count": manifest.added_files_count if is_data_file else 0,
                    "existing_data_files_count": manifest.existing_files_count if is_data_file else 0,
                    "deleted_data_files_count": manifest.deleted_files_count if is_data_file else 0,
                    "added_delete_files_count": manifest.added_files_count if is_delete_file else 0,
                    "existing_delete_files_count": manifest.existing_files_count if is_delete_file else 0,
                    "deleted_delete_files_count": manifest.deleted_files_count if is_delete_file else 0,
                    "partition_summaries": _partition_summaries_to_rows(specs[manifest.partition_spec_id], manifest.partitions)
                    if manifest.partitions
                    else [],
                }
                if is_all_manifests_table:
                    manifest_row["reference_snapshot_id"] = snapshot.snapshot_id
                manifests.append(manifest_row)

        return pa.Table.from_pylist(
            manifests,
            schema=self._get_all_manifests_schema() if is_all_manifests_table else self._get_manifests_schema(),
        )

    def manifests(self) -> "pa.Table":
        """Retrieve the table of manifests for the current snapshot.

        This method extracts metadata about manifests, including file paths, partition summaries,
        and snapshot-level metrics for the current state of the Iceberg table.

        Returns:
         pa.Table: A PyArrow table containing details about manifests.
        """
        return self._generate_manifests_table(self.tbl.current_snapshot())

    def metadata_log_entries(self) -> "pa.Table":
        """Retrieve the table of metadata log entries.

        This method fetches historical metadata changes logged for the table, including
        information about timestamps, schema updates, and snapshot IDs.

        Returns:
         pa.Table: A PyArrow table containing metadata log details, including:
              - timestamp: Time of the metadata update.
              - file: Path to the metadata file.
              - latest_snapshot_id: The most recent snapshot ID.
        """

        from pyiceberg.table.snapshots import MetadataLogEntry

        table_schema = pa.schema(
            [
                pa.field("timestamp", pa.timestamp(unit="ms"), nullable=False),
                pa.field("file", pa.string(), nullable=False),
                pa.field("latest_snapshot_id", pa.int64(), nullable=True),
                pa.field("latest_schema_id", pa.int32(), nullable=True),
                pa.field("latest_sequence_number", pa.int64(), nullable=True),
            ]
        )

        def metadata_log_entry_to_row(metadata_entry: MetadataLogEntry) -> Dict[str, Any]:
            latest_snapshot = self.tbl.snapshot_as_of_timestamp(metadata_entry.timestamp_ms)
            return {
                "timestamp": metadata_entry.timestamp_ms,
                "file": metadata_entry.metadata_file,
                "latest_snapshot_id": latest_snapshot.snapshot_id if latest_snapshot else None,
                "latest_schema_id": latest_snapshot.schema_id if latest_snapshot else None,
                "latest_sequence_number": latest_snapshot.sequence_number if latest_snapshot else None,
            }

        # similar to MetadataLogEntriesTable in Java
        # https://github.com/apache/iceberg/blob/8a70fe0ff5f241aec8856f8091c77fdce35ad256/core/src/main/java/org/apache/iceberg/MetadataLogEntriesTable.java#L62-L66
        metadata_log_entries = self.tbl.metadata.metadata_log + [
            MetadataLogEntry(metadata_file=self.tbl.metadata_location, timestamp_ms=self.tbl.metadata.last_updated_ms)
        ]

        return pa.Table.from_pylist(
            [metadata_log_entry_to_row(entry) for entry in metadata_log_entries],
            schema=table_schema,
        )

    def history(self) -> "pa.Table":
        """Retrieve the history table for the Iceberg table.

        This method provides a historical view of the table's state changes, including
        timestamps and parent-child relationships of snapshots.

        Returns:
           pa.Table: A PyArrow table with historical details, including:
               - made_current_at: Timestamp when the snapshot was made current.
               - snapshot_id: ID of the snapshot.
               - parent_id: ID of the parent snapshot.
            - is_current_ancestor: Whether the snapshot is an ancestor of the current state.
        """

        history_schema = pa.schema(
            [
                pa.field("made_current_at", pa.timestamp(unit="ms"), nullable=False),
                pa.field("snapshot_id", pa.int64(), nullable=False),
                pa.field("parent_id", pa.int64(), nullable=True),
                pa.field("is_current_ancestor", pa.bool_(), nullable=False),
            ]
        )

        ancestors_ids = {snapshot.snapshot_id for snapshot in ancestors_of(self.tbl.current_snapshot(), self.tbl.metadata)}

        history = []
        metadata = self.tbl.metadata

        for snapshot_entry in metadata.snapshot_log:
            snapshot = metadata.snapshot_by_id(snapshot_entry.snapshot_id)

            history.append(
                {
                    "made_current_at": datetime.fromtimestamp(snapshot_entry.timestamp_ms / 1000.0, tz=timezone.utc),
                    "snapshot_id": snapshot_entry.snapshot_id,
                    "parent_id": snapshot.parent_snapshot_id if snapshot else None,
                    "is_current_ancestor": snapshot_entry.snapshot_id in ancestors_ids,
                }
            )

        return pa.Table.from_pylist(history, schema=history_schema)

    def _files(self, snapshot_id: Optional[int] = None, data_file_filter: Optional[Set[DataFileContent]] = None) -> "pa.Table":
        """Retrieve a table of files from a specific snapshot, optionally filtered by content type.

        This method fetches file-level metadata, including details about data and delete files,
        for a given snapshot.

        Args:
         snapshot_id (Optional[int]): The snapshot ID to retrieve files for.
         data_file_filter (Optional[Set[DataFileContent]]): A set of file content types
           (e.g., data or delete files) to filter the results.

        Returns:
          pa.Table: A PyArrow table containing file metadata, including fields like:
             - content: Type of file content (data or deletes).
             - file_path: Path to the file.
             - record_count: Number of records in the file.
             - file_size_in_bytes: Size of the file in bytes.
        """

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = self.tbl.metadata.schema()
        readable_metrics_struct = []

        def _readable_metrics_struct(bound_type: PrimitiveType) -> pa.StructType:
            pa_bound_type = schema_to_pyarrow(bound_type)
            return pa.struct(
                [
                    pa.field("column_size", pa.int64(), nullable=True),
                    pa.field("value_count", pa.int64(), nullable=True),
                    pa.field("null_value_count", pa.int64(), nullable=True),
                    pa.field("nan_value_count", pa.int64(), nullable=True),
                    pa.field("lower_bound", pa_bound_type, nullable=True),
                    pa.field("upper_bound", pa_bound_type, nullable=True),
                ]
            )

        for field in self.tbl.metadata.schema().fields:
            readable_metrics_struct.append(
                pa.field(schema.find_column_name(field.field_id), _readable_metrics_struct(field.field_type), nullable=False)
            )

        files_schema = pa.schema(
            [
                pa.field("content", pa.int8(), nullable=False),
                pa.field("file_path", pa.string(), nullable=False),
                pa.field("file_format", pa.dictionary(pa.int32(), pa.string()), nullable=False),
                pa.field("spec_id", pa.int32(), nullable=False),
                pa.field("record_count", pa.int64(), nullable=False),
                pa.field("file_size_in_bytes", pa.int64(), nullable=False),
                pa.field("column_sizes", pa.map_(pa.int32(), pa.int64()), nullable=True),
                pa.field("value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                pa.field("null_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                pa.field("nan_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                pa.field("lower_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
                pa.field("upper_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
                pa.field("key_metadata", pa.binary(), nullable=True),
                pa.field("split_offsets", pa.list_(pa.int64()), nullable=True),
                pa.field("equality_ids", pa.list_(pa.int32()), nullable=True),
                pa.field("sort_order_id", pa.int32(), nullable=True),
                pa.field("readable_metrics", pa.struct(readable_metrics_struct), nullable=True),
            ]
        )

        files: list[dict[str, Any]] = []

        if not snapshot_id and not self.tbl.metadata.current_snapshot():
            return pa.Table.from_pylist(
                files,
                schema=files_schema,
            )
        snapshot = self._get_snapshot(snapshot_id)

        io = self.tbl.io
        for manifest_list in snapshot.manifests(io):
            for manifest_entry in manifest_list.fetch_manifest_entry(io):
                data_file = manifest_entry.data_file
                if data_file_filter and data_file.content not in data_file_filter:
                    continue
                column_sizes = data_file.column_sizes or {}
                value_counts = data_file.value_counts or {}
                null_value_counts = data_file.null_value_counts or {}
                nan_value_counts = data_file.nan_value_counts or {}
                lower_bounds = data_file.lower_bounds or {}
                upper_bounds = data_file.upper_bounds or {}
                readable_metrics = {
                    schema.find_column_name(field.field_id): {
                        "column_size": column_sizes.get(field.field_id),
                        "value_count": value_counts.get(field.field_id),
                        "null_value_count": null_value_counts.get(field.field_id),
                        "nan_value_count": nan_value_counts.get(field.field_id),
                        "lower_bound": from_bytes(field.field_type, lower_bound)
                        if (lower_bound := lower_bounds.get(field.field_id))
                        else None,
                        "upper_bound": from_bytes(field.field_type, upper_bound)
                        if (upper_bound := upper_bounds.get(field.field_id))
                        else None,
                    }
                    for field in self.tbl.metadata.schema().fields
                }
                files.append(
                    {
                        "content": data_file.content,
                        "file_path": data_file.file_path,
                        "file_format": data_file.file_format,
                        "spec_id": data_file.spec_id,
                        "record_count": data_file.record_count,
                        "file_size_in_bytes": data_file.file_size_in_bytes,
                        "column_sizes": dict(data_file.column_sizes) if data_file.column_sizes is not None else None,
                        "value_counts": dict(data_file.value_counts) if data_file.value_counts is not None else None,
                        "null_value_counts": dict(data_file.null_value_counts)
                        if data_file.null_value_counts is not None
                        else None,
                        "nan_value_counts": dict(data_file.nan_value_counts) if data_file.nan_value_counts is not None else None,
                        "lower_bounds": dict(data_file.lower_bounds) if data_file.lower_bounds is not None else None,
                        "upper_bounds": dict(data_file.upper_bounds) if data_file.upper_bounds is not None else None,
                        "key_metadata": data_file.key_metadata,
                        "split_offsets": data_file.split_offsets,
                        "equality_ids": data_file.equality_ids,
                        "sort_order_id": data_file.sort_order_id,
                        "readable_metrics": readable_metrics,
                    }
                )

        return pa.Table.from_pylist(
            files,
            schema=files_schema,
        )

    def files(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        """Retrieve a table of files for the current snapshot or a specific snapshot ID.

        This method fetches file-level metadata for data and delete files in the table.

        Args:
         snapshot_id (Optional[int]): The snapshot ID to retrieve files for. If None,
           the current snapshot is used.

        Returns:
          pa.Table: A PyArrow table containing file metadata.
        """
        return self._files(snapshot_id)

    def data_files(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        """Retrieve a table of data files for the current snapshot or a specific snapshot ID.

        This method fetches metadata for files containing table data (excluding delete files).

        Args:
          snapshot_id (Optional[int]): The snapshot ID to filter data files for. If None,
            the current snapshot is used.

        Returns:
           pa.Table: A PyArrow table containing metadata for data files."""
        return self._files(snapshot_id, {DataFileContent.DATA})

    def delete_files(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        """Retrieve a table of delete files for the current snapshot or a specific snapshot ID.

        This method fetches metadata for files containing delete markers.

        Args:
            snapshot_id (Optional[int]): The snapshot ID to filter delete files for. If None,
               the current snapshot is used.

        Returns:
           pa.Table: A PyArrow table containing metadata for delete files.
        """
        return self._files(snapshot_id, {DataFileContent.POSITION_DELETES, DataFileContent.EQUALITY_DELETES})

    def all_manifests(self) -> "pa.Table":
        """Retrieve a table of all manifests from all snapshots in the table.

         This method aggregates metadata for all manifests, including historical ones,
         providing a comprehensive view of the table's state.

        Returns:
                pa.Table: A PyArrow table containing metadata for all manifests, including fields like:
                   - content: Content type of the manifest.
                   - path: Path to the manifest file.
                   - added_snapshot_id: Snapshot ID when the manifest was added.
        """

        snapshots = self.tbl.snapshots()
        if not snapshots:
            return pa.Table.from_pylist([], schema=self._get_all_manifests_schema())

        executor = ExecutorFactory.get_or_create()
        manifests_by_snapshots: Iterator["pa.Table"] = executor.map(
            lambda args: self._generate_manifests_table(*args), [(snapshot, True) for snapshot in snapshots]
        )
        return pa.concat_tables(manifests_by_snapshots)
