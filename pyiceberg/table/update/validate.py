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
from bisect import bisect_left
from typing import Iterator, Optional, Set

from pyiceberg.exceptions import ValidationException
from pyiceberg.expressions import BooleanExpression
from pyiceberg.expressions.visitors import ROWS_CANNOT_MATCH, _InclusiveMetricsEvaluator
from pyiceberg.manifest import (
    INITIAL_SEQUENCE_NUMBER,
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
)
from pyiceberg.partitioning import PartitionMap, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot, ancestors_between
from pyiceberg.typedef import Record

VALIDATE_DATA_FILES_EXIST_OPERATIONS: Set[Operation] = {Operation.OVERWRITE, Operation.REPLACE, Operation.DELETE}
VALIDATE_ADDED_DATA_FILES_OPERATIONS: Set[Operation] = {Operation.APPEND, Operation.OVERWRITE}
VALIDATE_ADDED_DELETE_FILES_OPERATIONS: Set[Operation] = {Operation.DELETE, Operation.OVERWRITE}


class _PositionDeletes:
    # Indexed state
    _seqs: list[int] = []
    _entries: list[ManifestEntry] = []

    # Buffer used to hold files before indexing
    _buffer: list[ManifestEntry] = []
    _indexed: bool = False

    def _index_if_needed(self) -> None:
        if self._indexed is False:
            self._entries = sorted(self._buffer, key=lambda entry: _get_sequence_number_or_raise(entry))
            self._seqs = [_get_sequence_number_or_raise(entry) for entry in self._entries]
            self._indexed = True

    def add_entry(self, entry: ManifestEntry) -> None:
        if self._indexed:
            raise Exception("Can't add files upon indexing.")
        self._buffer.append(entry)

    def filter(self, seq: int) -> list[ManifestEntry]:
        self._index_if_needed()
        start = _find_start_index(self._seqs, seq)

        if start >= len(self._entries):
            return []

        if start == 0:
            return self.referenced_delete_files()

        matching_entries_count: int = len(self._entries) - start
        return self._entries[matching_entries_count:]

    def referenced_delete_files(self) -> list[ManifestEntry]:
        self._index_if_needed()
        return self._entries

    def is_empty(self) -> bool:
        self._index_if_needed()
        return len(self._entries) > 0


class _EqualityDeletes:
    # Indexed state
    _seqs: list[int] = []
    _entries: list[ManifestEntry] = []

    # Buffer used to hold files before indexing
    _buffer: list[ManifestEntry] = []
    _indexed: bool = False

    def _index_if_needed(self) -> None:
        if self._indexed is False:
            self._entries = sorted(self._buffer, key=lambda entry: _get_sequence_number_or_raise(entry))
            self._seqs = [_get_sequence_number_or_raise(entry) for entry in self._entries]
            self._indexed = True

    def add_entry(self, spec: PartitionSpec, entry: ManifestEntry) -> None:
        # TODO: Equality deletes should consider the spec to get the equality fields
        if self._indexed:
            raise Exception("Can't add files upon indexing.")
        self._buffer.append(entry)

    def filter(self, seq: int, entry: ManifestEntry) -> list[ManifestEntry]:
        self._index_if_needed()
        start = _find_start_index(self._seqs, seq)

        if start >= len(self._entries):
            return []

        if start == 0:
            return self.referenced_delete_files()

        matching_entries_count: int = len(self._entries) - start
        return self._entries[matching_entries_count:]

    def referenced_delete_files(self) -> list[ManifestEntry]:
        self._index_if_needed()
        return self._entries

    def is_empty(self) -> bool:
        self._index_if_needed()
        return len(self._entries) > 0


def _find_start_index(seqs: list[int], seq: int) -> int:
    pos: int = bisect_left(seqs, seq)
    if pos != len(seqs) and seqs[pos] == seqs:
        return pos
    return -1


def _get_sequence_number_or_raise(entry: ManifestEntry) -> int:
    if seq := entry.sequence_number:
        return seq
    raise ValidationException("ManifestEntry does not have a sequence number")


class DeleteFileIndex:
    """
    An index of delete files by sequence number.

    Use forDataFile(int, DataFile) or forEntry(ManifestEntry) to get the delete files to apply to a given data file
    """

    _global_deletes: _EqualityDeletes
    _pos_deletes_by_path: dict[str, _PositionDeletes]
    _pos_deletes_by_partition: PartitionMap[_PositionDeletes]
    _eq_deletes_by_partition: PartitionMap[_EqualityDeletes]
    _has_eq_deletes: bool
    _has_pos_deletes: bool
    _is_empty: bool

    def __init__(
        self,
        delete_entries: list[ManifestEntry],
        spec_by_id: dict[int, PartitionSpec],
        min_sequence_number: int = INITIAL_SEQUENCE_NUMBER,
    ) -> None:
        global_deletes: _EqualityDeletes = _EqualityDeletes()
        eq_deletes_by_partition: PartitionMap[_EqualityDeletes] = PartitionMap(spec_by_id)
        pos_deletes_by_partition: PartitionMap[_PositionDeletes] = PartitionMap(spec_by_id)
        pos_deletes_by_path: dict[str, _PositionDeletes] = {}

        for entry in delete_entries:
            if entry.sequence_number is None:
                continue

            if entry.sequence_number <= min_sequence_number:
                continue

            file: DataFile = entry.data_file
            content: DataFileContent = file.content

            if content == DataFileContent.POSITION_DELETES:
                self._add_pos_deletes(pos_deletes_by_path, pos_deletes_by_partition, entry)
            elif content == DataFileContent.EQUALITY_DELETES:
                self._add_eq_deletes(global_deletes, eq_deletes_by_partition, spec_by_id, entry)
            else:
                raise NotImplementedError(f"Unsupported content: {file.content}")

        # Set global variables for the class
        self._spec_by_id = spec_by_id
        self._global_deletes = global_deletes
        self._eq_deletes_by_partition = eq_deletes_by_partition
        self._pos_deletes_by_partition = pos_deletes_by_partition
        self._pos_deletes_by_path = pos_deletes_by_path

        self._has_eq_deletes = global_deletes.is_empty() or len(eq_deletes_by_partition) > 0
        self._has_pos_deletes = len(pos_deletes_by_partition) > 0 or len(pos_deletes_by_path) > 0
        self._is_empty = not self._has_eq_deletes and not self._has_pos_deletes

    def _add_pos_deletes(
        self,
        pos_deletes_by_path: dict[str, _PositionDeletes],
        pos_deletes_by_partition: PartitionMap[_PositionDeletes],
        entry: ManifestEntry,
    ) -> None:
        deletes: _PositionDeletes = _PositionDeletes()

        # TODO: Fallback method to get file_path from lower_bounds
        if file_path := entry.data_file.file_path:
            if file_path not in pos_deletes_by_path:
                pos_deletes_by_path[file_path] = deletes
            else:
                deletes = pos_deletes_by_path[file_path]
        else:
            spec_id: int = entry.data_file.spec_id
            partition: Record = entry.data_file.partition
            pos_deletes_by_partition.compute_if_absent(spec_id, partition, lambda: deletes)

        deletes.add_entry(entry)

    def _add_eq_deletes(
        self,
        global_deletes: _EqualityDeletes,
        eq_deletes_by_partition: PartitionMap[_EqualityDeletes],
        spec_by_id: dict[int, PartitionSpec],
        entry: ManifestEntry,
    ) -> None:
        deletes: _EqualityDeletes = _EqualityDeletes()

        if spec := spec_by_id.get(entry.data_file.spec_id):
            if spec.is_unpartitioned():
                deletes = global_deletes
            else:
                spec_id = spec.spec_id
                partition = entry.data_file.partition
                eq_deletes_by_partition.compute_if_absent(spec_id, partition, lambda: deletes)

            deletes.add_entry(spec, entry)

    def is_empty(self) -> bool:
        return self._is_empty

    def has_equality_deletes(self) -> bool:
        return self._has_eq_deletes

    def has_position_deletes(self) -> bool:
        return self._has_pos_deletes

    def for_entry(self, entry: ManifestEntry) -> list[ManifestEntry]:
        sequence_number = _get_sequence_number_or_raise(entry)
        return self.for_data_file(sequence_number, entry)

    def for_data_file(self, sequence_number: int, entry: ManifestEntry) -> list[ManifestEntry]:
        if self.is_empty():
            return []

        global_deletes = self._global_deletes.filter(sequence_number, entry)
        pos_path_deletes = self._pos_deletes_by_path[entry.data_file.file_path].filter(sequence_number)

        spec_id = entry.data_file.spec_id
        partition = entry.data_file.partition

        eq_deletes_by_partition: list[ManifestEntry] = []
        if eq_deletes := self._eq_deletes_by_partition.get(spec_id, partition):
            eq_deletes_by_partition = eq_deletes.filter(sequence_number, entry)

        pos_deletes_by_partition: list[ManifestEntry] = []
        if pos_deletes := self._pos_deletes_by_partition.get(spec_id, partition):
            pos_deletes_by_partition = pos_deletes.filter(sequence_number)

        return global_deletes + eq_deletes_by_partition + pos_path_deletes + pos_deletes_by_partition

    def referenced_delete_files(self) -> list[DataFile]:
        entries: list[ManifestEntry] = []

        for deletes in self._pos_deletes_by_partition.values():
            entries.extend(deletes.referenced_delete_files())

        for deletes in self._pos_deletes_by_path.values():
            entries.extend(deletes.referenced_delete_files())

        return [entry.data_file for entry in entries]


def _validation_history(
    table: Table,
    from_snapshot: Snapshot,
    to_snapshot: Snapshot,
    matching_operations: set[Operation],
    manifest_content_filter: ManifestContent,
) -> tuple[list[ManifestFile], set[int]]:
    """Return newly added manifests and snapshot IDs between the starting snapshot and parent snapshot.

    Args:
        table: Table to get the history from
        from_snapshot: Parent snapshot to get the history from
        to_snapshot: Starting snapshot
        matching_operations: Operations to match on
        manifest_content_filter: Manifest content type to filter

    Raises:
        ValidationException: If no matching snapshot is found or only one snapshot is found

    Returns:
        List of manifest files and set of snapshots ID's matching conditions
    """
    manifests_files: list[ManifestFile] = []
    snapshots: set[int] = set()

    last_snapshot = None
    for snapshot in ancestors_between(from_snapshot, to_snapshot, table.metadata):
        last_snapshot = snapshot
        summary = snapshot.summary
        if summary is None:
            raise ValidationException(f"No summary found for snapshot {snapshot}!")
        if summary.operation not in matching_operations:
            continue

        snapshots.add(snapshot.snapshot_id)
        # TODO: Maybe do the IO in a separate thread at some point, and collect at the bottom (we can easily merge the sets
        manifests_files.extend(
            [
                manifest
                for manifest in snapshot.manifests(table.io)
                if manifest.added_snapshot_id == snapshot.snapshot_id and manifest.content == manifest_content_filter
            ]
        )

    if last_snapshot is not None and last_snapshot.snapshot_id != from_snapshot.snapshot_id:
        raise ValidationException("No matching snapshot found.")

    return manifests_files, snapshots


def _filter_manifest_entries(
    entry: ManifestEntry,
    snapshot_ids: set[int],
    data_filter: Optional[BooleanExpression],
    partition_set: Optional[dict[int, set[Record]]],
    entry_status: Optional[ManifestEntryStatus],
    schema: Schema,
) -> bool:
    """Filter manifest entries based on data filter and partition set.

    Args:
        entry: Manifest entry to filter
        snapshot_ids: set of snapshot ids to match data files
        data_filter: Optional filter to match data files
        partition_set: Optional set of partitions to match data files
        entry_status: Optional status to match data files
        schema: schema for filtering

    Returns:
        True if the entry should be included, False otherwise
    """
    if entry.snapshot_id not in snapshot_ids:
        return False

    if entry_status is not None and entry.status != entry_status:
        return False

    if data_filter is not None:
        evaluator = _InclusiveMetricsEvaluator(schema, data_filter)
        if evaluator.eval(entry.data_file) is ROWS_CANNOT_MATCH:
            return False

    if partition_set is not None:
        partition = entry.data_file.partition
        spec_id = entry.data_file.spec_id
        if spec_id not in partition_set or partition not in partition_set[spec_id]:
            return False

    return True


def _deleted_data_files(
    table: Table,
    starting_snapshot: Snapshot,
    data_filter: Optional[BooleanExpression],
    partition_set: Optional[dict[int, set[Record]]],
    parent_snapshot: Optional[Snapshot],
) -> Iterator[ManifestEntry]:
    """Find deleted data files matching a filter since a starting snapshot.

    Args:
        table: Table to validate
        starting_snapshot: Snapshot current at the start of the operation
        data_filter: Expression used to find deleted data files
        partition_set: dict of {spec_id: set[partition]} to filter on
        parent_snapshot: Ending snapshot on the branch being validated

    Returns:
        List of conflicting manifest-entries
    """
    # if there is no current table state, no files have been deleted
    if parent_snapshot is None:
        return

    manifests, snapshot_ids = _validation_history(
        table,
        parent_snapshot,
        starting_snapshot,
        VALIDATE_DATA_FILES_EXIST_OPERATIONS,
        ManifestContent.DATA,
    )

    for manifest in manifests:
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
            if _filter_manifest_entries(
                entry, snapshot_ids, data_filter, partition_set, ManifestEntryStatus.DELETED, table.schema()
            ):
                yield entry


def _build_delete_file_index(
    table: Table,
    snapshot_ids: set[int],
    delete_manifests: list[ManifestFile],
    min_sequence_number: int,
    specs_by_id: dict[int, PartitionSpec],
    partition_set: Optional[dict[int, set[Record]]],
    data_filter: Optional[BooleanExpression],
) -> DeleteFileIndex:
    """Filter manifest entries to build a DeleteFileIndex.

    Args:
        table: Table to filter manifest from
        snapshot_ids: Snapshots ids from the table
        delete_manifests: Manifest to fetch the entries from
        min_sequence_number: Sequence number from the starting snapshot
        specs_by_id: dict of {spec_id: PartitionSpec} to build DeleteFileIndex
        partition_set: dict of {spec_id: set[partition]} to filter on
        data_filter: Expression used to find data files

    Returns:
        DeleteFileIndex
    """
    delete_entries = []

    for manifest in delete_manifests:
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
            if _filter_manifest_entries(
                entry, snapshot_ids, data_filter, partition_set, ManifestEntryStatus.ADDED, table.schema()
            ):
                delete_entries.append(entry)

    return DeleteFileIndex(delete_entries=delete_entries, spec_by_id=specs_by_id, min_sequence_number=min_sequence_number)


def _starting_sequence_number(table: Table, starting_snapshot: Optional[Snapshot]) -> int:
    """Find the starting sequence number from a snapshot.

    Args:
        table: Table to find snapshot from
        starting_snapshot: Snapshot from where to start looking

    Returns
        Sequence number as int
    """
    if starting_snapshot is not None:
        if snapshot := table.snapshot_by_id(starting_snapshot.snapshot_id):
            if seq := snapshot.sequence_number:
                return seq
    return INITIAL_SEQUENCE_NUMBER


def _added_delete_files(
    table: Table,
    starting_snapshot: Snapshot,
    data_filter: Optional[BooleanExpression],
    partition_set: Optional[dict[int, set[Record]]],
    parent_snapshot: Optional[Snapshot],
) -> DeleteFileIndex:
    """Return matching delete files have been added to the table since a starting snapshot.

    Args:
        table: Table to validate
        starting_snapshot: Snapshot current at the start of the operation
        data_filter: Expression used to find deleted data files
        partition_set: dict of {spec_id: set[partition]} to filter on
        parent_snapshot: Ending snapshot on the branch being validated

    Returns:
        DeleteFileIndex
    """
    if parent_snapshot is None or table.format_version < 2:
        return DeleteFileIndex([], {}, 0)

    manifests, snapshot_ids = _validation_history(
        table, parent_snapshot, starting_snapshot, VALIDATE_ADDED_DELETE_FILES_OPERATIONS, ManifestContent.DELETES
    )

    starting_sequence_number = _starting_sequence_number(table, starting_snapshot)
    return _build_delete_file_index(
        table=table,
        snapshot_ids=snapshot_ids,
        delete_manifests=manifests,
        min_sequence_number=starting_sequence_number,
        specs_by_id={},
        partition_set=partition_set,
        data_filter=data_filter,
    )


def _validate_deleted_data_files(
    table: Table,
    starting_snapshot: Snapshot,
    data_filter: Optional[BooleanExpression],
    parent_snapshot: Snapshot,
) -> None:
    """Validate that no files matching a filter have been deleted from the table since a starting snapshot.

    Args:
        table: Table to validate
        starting_snapshot: Snapshot current at the start of the operation
        data_filter: Expression used to find deleted data files
        parent_snapshot: Ending snapshot on the branch being validated

    """
    conflicting_entries = _deleted_data_files(table, starting_snapshot, data_filter, None, parent_snapshot)
    if any(conflicting_entries):
        conflicting_snapshots = {entry.snapshot_id for entry in conflicting_entries}
        raise ValidationException(f"Deleted data files were found matching the filter for snapshots {conflicting_snapshots}!")


def _added_data_files(
    table: Table,
    starting_snapshot: Snapshot,
    data_filter: Optional[BooleanExpression],
    partition_set: Optional[dict[int, set[Record]]],
    parent_snapshot: Optional[Snapshot],
) -> Iterator[ManifestEntry]:
    """Return manifest entries for data files added between the starting snapshot and parent snapshot.

    Args:
        table: Table to get the history from
        starting_snapshot: Starting snapshot to get the history from
        data_filter: Optional filter to match data files
        partition_set: Optional set of partitions to match data files
        parent_snapshot: Parent snapshot to get the history from

    Returns:
        Iterator of manifest entries for added data files matching the conditions
    """
    if parent_snapshot is None:
        return

    manifests, snapshot_ids = _validation_history(
        table,
        parent_snapshot,
        starting_snapshot,
        VALIDATE_ADDED_DATA_FILES_OPERATIONS,
        ManifestContent.DATA,
    )

    for manifest in manifests:
        for entry in manifest.fetch_manifest_entry(table.io):
            if _filter_manifest_entries(entry, snapshot_ids, data_filter, partition_set, None, table.schema()):
                yield entry


def _validate_added_data_files(
    table: Table,
    starting_snapshot: Snapshot,
    data_filter: Optional[BooleanExpression],
    parent_snapshot: Optional[Snapshot],
) -> None:
    """Validate that no files matching a filter have been added to the table since a starting snapshot.

    Args:
        table: Table to validate
        starting_snapshot: Snapshot current at the start of the operation
        data_filter: Expression used to find added data files
        parent_snapshot: Ending snapshot on the branch being validated

    """
    conflicting_entries = _added_data_files(table, starting_snapshot, data_filter, None, parent_snapshot)
    if any(conflicting_entries):
        conflicting_snapshots = {entry.snapshot_id for entry in conflicting_entries if entry.snapshot_id is not None}
        raise ValidationException(f"Added data files were found matching the filter for snapshots {conflicting_snapshots}!")


def _validate_no_new_delete_files(
    table: Table,
    starting_snapshot: Snapshot,
    data_filter: Optional[BooleanExpression],
    partition_set: Optional[dict[int, set[Record]]],
    parent_snapshot: Snapshot,
) -> None:
    """Validate no new delete files matching a filter have been added to the table since starting a snapshot.

    Args:
        table: Table to validate
        starting_snapshot: Snapshot current at the start of the operation
        data_filter: Expression used to find new conflicting delete files
        parent_snapshot: Ending snapshot on the branch being deleted

    """
    deletes = _added_delete_files(table, starting_snapshot, data_filter, partition_set, parent_snapshot)
    if not deletes.is_empty():
        conflicting_delete_files = [file.file_path for file in deletes.referenced_delete_files()]
        raise ValidationException(
            f"Found new conflicting delete files that can apply to records matching {data_filter}:{conflicting_delete_files}"
        )
