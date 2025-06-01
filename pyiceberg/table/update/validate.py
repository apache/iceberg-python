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
from typing import Callable, Iterator, Optional

from pyiceberg.exceptions import ValidationException
from pyiceberg.expressions import BooleanExpression
from pyiceberg.expressions.visitors import ROWS_CANNOT_MATCH, _InclusiveMetricsEvaluator
from pyiceberg.manifest import DataFile, ManifestContent, ManifestEntry, ManifestEntryStatus, ManifestFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot, ancestors_between
from pyiceberg.typedef import Record

VALIDATE_DATA_FILES_EXIST_OPERATIONS = {Operation.OVERWRITE, Operation.REPLACE, Operation.DELETE}


class ManifestEntryEvaluator:
    """Evaluate manifests against given conditions."""

    def __init__(
        self,
        manifests: list[ManifestFile],
        table: Table,
        snapshot_ids: Optional[set[int]] = None,
        status: Optional[ManifestEntryStatus] = None,
        data_filter: Optional[BooleanExpression] = None,
        partition_set: Optional[dict[int, set[Record]]] = None,
    ):
        self.manifests = manifests
        self.table = table
        self.snapshot_ids = snapshot_ids if snapshot_ids is not None else []
        self.status = status
        self.partition_set = partition_set

        if data_filter is not None:
            self.metrics_evaluator: Optional[Callable[[DataFile], bool]] = _InclusiveMetricsEvaluator(
                self.table.schema(), data_filter
            ).eval
        else:
            self.metrics_evaluator = None

    def _evaluate_snapshot_ids(self, entry: ManifestEntry) -> bool:
        """Check if the entry's snapshot ID is in the filter set."""
        if entry.snapshot_id in self.snapshot_ids:
            return True
        else:
            return False

    def _evaluate_status(self, entry: ManifestEntry) -> bool:
        """Check if the entry's status matches the filter."""
        if self.status is None or entry.status == self.status:
            return True
        else:
            return False

    def _evaluate_data_filter(self, entry: ManifestEntry) -> bool:
        """Check if the entry's data file matches the data filter."""
        if self.metrics_evaluator is None:
            return True
        if self.metrics_evaluator(entry.data_file) is ROWS_CANNOT_MATCH:
            return False
        return True

    def _evaluate_partition_set(self, entry: ManifestEntry) -> bool:
        """Check if the entry's partition matches the partition set."""
        if self.partition_set is None:
            return True
        spec_id = entry.data_file.spec_id
        partition = entry.data_file.partition
        if spec_id not in self.partition_set or partition not in self.partition_set[spec_id]:
            return False
        return True

    def evaluate(self) -> Iterator[ManifestEntry]:
        """Evaluate the manifests against the given conditions."""
        for manifest in self.manifests:
            for entry in manifest.fetch_manifest_entry(self.table.io, discard_deleted=False):
                if not self._evaluate_snapshot_ids(entry):
                    continue

                if not self._evaluate_status(entry):
                    continue

                if not self._evaluate_data_filter(entry):
                    continue

                if not self._evaluate_partition_set(entry):
                    continue

                yield entry


def validation_history(
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

    manifests, snapshot_ids = validation_history(
        table,
        parent_snapshot,
        starting_snapshot,
        VALIDATE_DATA_FILES_EXIST_OPERATIONS,
        ManifestContent.DATA,
    )

    manifest_evaluator = ManifestEntryEvaluator(
        manifests,
        table,
        snapshot_ids,
        ManifestEntryStatus.DELETED,
        data_filter,
        partition_set,
    )

    yield from manifest_evaluator.evaluate()


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
