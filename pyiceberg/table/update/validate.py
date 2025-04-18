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
from pyiceberg.manifest import ManifestContent, ManifestFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot, ancestors_between


class ValidationException(Exception):
    """Raised when validation fails."""


def validation_history(
    table: Table,
    starting_snapshot: Snapshot,
    matching_operations: set[Operation],
    manifest_content_filter: ManifestContent,
    parent_snapshot: Snapshot,
) -> tuple[list[ManifestFile], set[Snapshot]]:
    """Return newly added manifests and snapshot IDs between the starting snapshot and parent snapshot.

    Args:
        table: Table to get the history from
        starting_snapshot: Starting snapshot
        matching_operations: Operations to match on
        manifest_content_filter: Manifest content type to filter
        parent_snapshot: Parent snapshot to get the history from

    Raises:
        ValidationException: If no matching snapshot is found or only one snapshot is found

    Returns:
        List of manifest files and set of snapshots matching conditions
    """
    manifests_files: list[ManifestFile] = []
    snapshots: set[Snapshot] = set()

    last_snapshot = None
    for snapshot in ancestors_between(starting_snapshot, parent_snapshot, table.metadata):
        last_snapshot = snapshot
        summary = snapshot.summary
        if summary is None:
            continue
        if summary.operation in matching_operations:
            snapshots.add(snapshot)
            manifests_files.extend(
                [
                    manifest
                    for manifest in snapshot.manifests(table.io, manifest_content_filter)
                    if manifest.added_snapshot_id == snapshot.snapshot_id
                ]
            )

    if last_snapshot is None or last_snapshot.snapshot_id == starting_snapshot.snapshot_id:
        raise ValidationException("No matching snapshot found.")

    return manifests_files, snapshots
