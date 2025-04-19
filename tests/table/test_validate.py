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
# pylint:disable=redefined-outer-name,eval-used
from typing import cast
from unittest.mock import patch

from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestContent, ManifestFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot
from pyiceberg.table.update.validate import deleted_data_files, validation_history


def test_validation_history(table_v2_with_extensive_snapshots: Table) -> None:
    """Test the validation history function."""
    mock_manifests = {}

    for i, snapshot in enumerate(table_v2_with_extensive_snapshots.snapshots()):
        mock_manifest = ManifestFile(
            manifest_path=f"foo/bar/{i}",
            manifest_length=1,
            partition_spec_id=1,
            content=ManifestContent.DATA if i % 2 == 0 else ManifestContent.DELETES,
            sequence_number=1,
            min_sequence_number=1,
            added_snapshot_id=snapshot.snapshot_id,
        )

        # Store the manifest for this specific snapshot
        mock_manifests[snapshot.snapshot_id] = [mock_manifest]

    expected_manifest_data_counts = len([m for m in mock_manifests.values() if m[0].content == ManifestContent.DATA]) - 1

    oldest_snapshot = table_v2_with_extensive_snapshots.snapshots()[0]
    newest_snapshot = cast(Snapshot, table_v2_with_extensive_snapshots.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    with patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect):
        manifests, snapshots = validation_history(
            table_v2_with_extensive_snapshots,
            newest_snapshot,
            oldest_snapshot,
            {Operation.APPEND},
            ManifestContent.DATA,
        )

        assert len(manifests) == expected_manifest_data_counts


def test_deleted_data_files(
    table_v2_with_extensive_snapshots: Table,
) -> None:
    mock_manifests = {}

    for i, snapshot in enumerate(table_v2_with_extensive_snapshots.snapshots()):
        mock_manifest = ManifestFile(
            manifest_path=f"foo/bar/{i}",
            manifest_length=1,
            partition_spec_id=1,
            content=ManifestContent.DATA if i % 2 == 0 else ManifestContent.DELETES,
            sequence_number=1,
            min_sequence_number=1,
            added_snapshot_id=snapshot.snapshot_id,
        )

        # Store the manifest for this specific snapshot
        mock_manifests[snapshot.snapshot_id] = [mock_manifest]

    oldest_snapshot = table_v2_with_extensive_snapshots.snapshots()[0]
    newest_snapshot = cast(Snapshot, table_v2_with_extensive_snapshots.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    # every snapshot is an append, so we should get nothing!
    with patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect):
        result = list(
            deleted_data_files(
                table=table_v2_with_extensive_snapshots,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
                partition_set=None,
            )
        )

        assert result == []
