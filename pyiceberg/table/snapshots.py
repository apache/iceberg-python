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
import time
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
)

from pydantic import Field, PrivateAttr, model_serializer

from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent, ManifestFile, read_manifest_list
from pyiceberg.typedef import IcebergBaseModel

OPERATION = "operation"


class Operation(Enum):
    """Describes the operation.

    Possible operation values are:
        - append: Only data files were added and no files were removed.
        - replace: Data and delete files were added and removed without changing table data; i.e., compaction, changing the data file format, or relocating data files.
        - overwrite: Data and delete files were added and removed in a logical overwrite operation.
        - delete: Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    """

    APPEND = "append"
    REPLACE = "replace"
    OVERWRITE = "overwrite"
    DELETE = "delete"

    def __repr__(self) -> str:
        """Return the string representation of the Operation class."""
        return f"Operation.{self.name}"


class Summary(IcebergBaseModel, Mapping[str, str]):
    """A class that stores the summary information for a Snapshot.

    The snapshot summary’s operation field is used by some operations,
    like snapshot expiration, to skip processing certain snapshots.
    """

    operation: Operation = Field()
    _additional_properties: Dict[str, str] = PrivateAttr()

    def __init__(self, operation: Operation, **data: Any) -> None:
        super().__init__(operation=operation, **data)
        self._additional_properties = data

    def __getitem__(self, __key: str) -> Optional[Any]:  # type: ignore
        """Return a key as it is a map."""
        if __key == 'operation':
            return self.operation
        else:
            return self._additional_properties.get(__key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set a key as it is a map."""
        if key == 'operation':
            self.operation = value
        else:
            self._additional_properties[key] = value

    def __len__(self) -> int:
        """Return the number of keys in the summary."""
        # Operation is required
        return 1 + len(self._additional_properties)

    @model_serializer
    def ser_model(self) -> Dict[str, str]:
        return {
            "operation": str(self.operation.value),
            **self._additional_properties,
        }

    @property
    def additional_properties(self) -> Dict[str, str]:
        return self._additional_properties

    def __repr__(self) -> str:
        """Return the string representation of the Summary class."""
        repr_properties = f", **{repr(self._additional_properties)}" if self._additional_properties else ""
        return f"Summary({repr(self.operation)}{repr_properties})"

    def __eq__(self, other: Any) -> bool:
        """Compare if the summary is equal to another summary."""
        return (
            self.operation == other.operation and self.additional_properties == other.additional_properties
            if isinstance(other, Summary)
            else False
        )


class Snapshot(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    parent_snapshot_id: Optional[int] = Field(alias="parent-snapshot-id", default=None)
    sequence_number: Optional[int] = Field(alias="sequence-number", default=None)
    timestamp_ms: int = Field(alias="timestamp-ms", default_factory=lambda: int(time.time() * 1000))
    manifest_list: Optional[str] = Field(
        alias="manifest-list", description="Location of the snapshot's manifest list file", default=None
    )
    summary: Optional[Summary] = Field(default=None)
    schema_id: Optional[int] = Field(alias="schema-id", default=None)

    def __str__(self) -> str:
        """Return the string representation of the Snapshot class."""
        operation = f"{self.summary.operation}: " if self.summary else ""
        parent_id = f", parent_id={self.parent_snapshot_id}" if self.parent_snapshot_id else ""
        schema_id = f", schema_id={self.schema_id}" if self.schema_id is not None else ""
        result_str = f"{operation}id={self.snapshot_id}{parent_id}{schema_id}"
        return result_str

    def manifests(self, io: FileIO) -> List[ManifestFile]:
        if self.manifest_list is not None:
            file = io.new_input(self.manifest_list)
            return list(read_manifest_list(file))
        return []


class MetadataLogEntry(IcebergBaseModel):
    metadata_file: str = Field(alias="metadata-file")
    timestamp_ms: int = Field(alias="timestamp-ms")


class SnapshotLogEntry(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    timestamp_ms: int = Field(alias="timestamp-ms")


class SnapshotSummaryCollector:
    added_size: int
    removed_size: int
    added_files: int
    removed_files: int
    added_eq_delete_files: int
    removed_eq_delete_files: int
    added_pos_delete_files: int
    removed_pos_delete_files: int
    added_delete_files: int
    removed_delete_files: int
    added_records: int
    deleted_records: int
    added_pos_deletes: int
    removed_pos_deletes: int
    added_eq_deletes: int
    removed_eq_deletes: int

    def __init__(self) -> None:
        self.added_size = 0
        self.removed_size = 0
        self.added_files = 0
        self.removed_files = 0
        self.added_eq_delete_files = 0
        self.removed_eq_delete_files = 0
        self.added_pos_delete_files = 0
        self.removed_pos_delete_files = 0
        self.added_delete_files = 0
        self.removed_delete_files = 0
        self.added_records = 0
        self.deleted_records = 0
        self.added_pos_deletes = 0
        self.removed_pos_deletes = 0
        self.added_eq_deletes = 0
        self.removed_eq_deletes = 0

    def add_file(self, data_file: DataFile) -> None:
        if data_file.content == DataFileContent.DATA:
            self.added_files += 1
            self.added_records += data_file.record_count
            self.added_size += data_file.file_size_in_bytes
        elif data_file.content == DataFileContent.POSITION_DELETES:
            self.added_delete_files += 1
            self.added_pos_delete_files += 1
            self.added_pos_deletes += data_file.record_count
        elif data_file.content == DataFileContent.EQUALITY_DELETES:
            self.added_delete_files += 1
            self.added_eq_delete_files += 1
            self.added_eq_deletes += data_file.record_count
        else:
            raise ValueError(f"Unknown data file content: {data_file.content}")

    def removed_file(self, data_file: DataFile) -> None:
        if data_file.content == DataFileContent.DATA:
            self.removed_files += 1
            self.deleted_records += data_file.record_count
        elif data_file.content == DataFileContent.POSITION_DELETES:
            self.removed_delete_files += 1
            self.removed_pos_delete_files += 1
            self.removed_pos_deletes += data_file.record_count
        elif data_file.content == DataFileContent.EQUALITY_DELETES:
            self.removed_delete_files += 1
            self.removed_eq_delete_files += 1
            self.removed_eq_deletes += data_file.record_count
        else:
            raise ValueError(f"Unknown data file content: {data_file.content}")

    def added_manifest(self, manifest: ManifestFile) -> None:
        if manifest.content == ManifestContent.DATA:
            self.added_files += manifest.added_files_count or 0
            self.added_records += manifest.added_rows_count or 0
            self.removed_files += manifest.deleted_files_count or 0
            self.deleted_records += manifest.deleted_rows_count or 0
        elif manifest.content == ManifestContent.DELETES:
            self.added_delete_files += manifest.added_files_count or 0
            self.removed_delete_files += manifest.deleted_files_count or 0
        else:
            raise ValueError(f"Unknown manifest file content: {manifest.content}")

    def build(self) -> Dict[str, str]:
        def set_non_zero(properties: Dict[str, str], num: int, property_name: str) -> None:
            if num > 0:
                properties[property_name] = str(num)

        properties: Dict[str, str] = {}
        set_non_zero(properties, self.added_size, 'added-files-size')
        set_non_zero(properties, self.removed_size, 'removed-files-size')
        set_non_zero(properties, self.added_files, 'added-data-files')
        set_non_zero(properties, self.removed_files, 'removed-data-files')
        set_non_zero(properties, self.added_eq_delete_files, 'added-equality-delete-files')
        set_non_zero(properties, self.removed_eq_delete_files, 'removed-equality-delete-files')
        set_non_zero(properties, self.added_pos_delete_files, 'added-position-delete-files')
        set_non_zero(properties, self.removed_pos_delete_files, 'removed-position-delete-files')
        set_non_zero(properties, self.added_delete_files, 'added-delete-files')
        set_non_zero(properties, self.removed_delete_files, 'removed-delete-files')
        set_non_zero(properties, self.added_records, 'added-records')
        set_non_zero(properties, self.deleted_records, 'deleted-records')
        set_non_zero(properties, self.added_pos_deletes, 'added-position-deletes')
        set_non_zero(properties, self.removed_pos_deletes, 'removed-position-deletes')
        set_non_zero(properties, self.added_eq_deletes, 'added-equality-deletes')
        set_non_zero(properties, self.removed_eq_deletes, 'removed-equality-deletes')

        return properties


properties = ['records', 'files-size', 'data-files', 'delete-files', 'position-deletes', 'equality-deletes']


def truncate_table_summary(summary: Summary, previous_summary: Mapping[str, str]) -> Summary:
    for prop in {
        'total-data-files',
        'total-delete-files',
        'total-records',
        'total-files-size',
        'total-position-deletes',
        'total-equality-deletes',
    }:
        summary[prop] = '0'

    if value := previous_summary.get('total-data-files'):
        summary['deleted-data-files'] = value
    if value := previous_summary.get('total-delete-files'):
        summary['removed-delete-files'] = value
    if value := previous_summary.get('total-records'):
        summary['deleted-records'] = value
    if value := previous_summary.get('total-files-size'):
        summary['removed-files-size'] = value
    if value := previous_summary.get('total-position-deletes'):
        summary['removed-position-deletes'] = value
    if value := previous_summary.get('total-equality-deletes'):
        summary['removed-equality-deletes'] = value

    return summary


def merge_snapshot_summaries(
    summary: Summary,
    previous_summary: Optional[Mapping[str, str]] = None,
) -> Summary:
    if summary.operation == Operation.OVERWRITE and previous_summary is not None:
        summary = truncate_table_summary(summary, previous_summary)

    if not previous_summary:
        previous_summary = {
            'total-data-files': '0',
            'total-delete-files': '0',
            'total-records': '0',
            'total-files-size': '0',
            'total-position-deletes': '0',
            'total-equality-deletes': '0',
        }

    def _update_totals(total_property: str, added_property: str, removed_property: str) -> None:
        if new_total_str := previous_summary.get(total_property):
            new_total = int(new_total_str)
            if new_total >= 0 and (added := summary.get(added_property)):
                new_total += int(added)
            if new_total >= 0 and (removed := summary.get(removed_property)):
                new_total -= int(removed)
            if new_total >= 0:
                summary[total_property] = str(new_total)

    _update_totals(
        total_property='total-data-files',
        added_property='added-data-files',
        removed_property='deleted-data-files',
    )
    _update_totals(
        total_property='total-delete-files',
        added_property='added-delete-files',
        removed_property='removed-delete-files',
    )
    _update_totals(
        total_property='total-records',
        added_property='added-records',
        removed_property='deleted-records',
    )
    _update_totals(
        total_property='total-files-size',
        added_property='added-files-size',
        removed_property='deleted-files-size',
    )
    _update_totals(
        total_property='total-position-deletes',
        added_property='added-position-deletes',
        removed_property='removed-position-deletes',
    )
    _update_totals(
        total_property='total-equality-deletes',
        added_property='added-equality-deletes',
        removed_property='removed-equality-deletes',
    )

    return summary
