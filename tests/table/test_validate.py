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

import pytest

from pyiceberg.exceptions import ValidationException
from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestContent, ManifestFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot
from pyiceberg.table.update.validate import validation_history


@pytest.fixture
def table_v2_with_extensive_snapshots_and_manifests(
    table_v2_with_extensive_snapshots: Table,
) -> tuple[Table, dict[int, list[ManifestFile]]]:
    """Fixture to create a table with extensive snapshots and manifests."""
    mock_manifests = {}

    for i, snapshot in enumerate(table_v2_with_extensive_snapshots.snapshots()):
        mock_manifest = ManifestFile.from_args(
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

    return table_v2_with_extensive_snapshots, mock_manifests


def test_validation_history(table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]]) -> None:
    """Test the validation history function."""
    table, mock_manifests = table_v2_with_extensive_snapshots_and_manifests

    expected_manifest_data_counts = len([m for m in mock_manifests.values() if m[0].content == ManifestContent.DATA])

    oldest_snapshot = table.snapshots()[0]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    with patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect):
        manifests, snapshots = validation_history(
            table,
            newest_snapshot,
            oldest_snapshot,
            {Operation.APPEND},
            ManifestContent.DATA,
        )

        assert len(manifests) == expected_manifest_data_counts


def test_validation_history_fails_on_snapshot_with_no_summary(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
) -> None:
    """Test the validation history function fails on snapshot with no summary."""
    table, _ = table_v2_with_extensive_snapshots_and_manifests
    oldest_snapshot = table.snapshots()[0]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    # Create a snapshot with no summary
    snapshot_with_no_summary = Snapshot(
        snapshot_id="1234",
        parent_id="5678",
        timestamp_ms=0,
        operation=Operation.APPEND,
        summary=None,
        manifest_list="foo/bar",
    )
    with patch("pyiceberg.table.update.validate.ancestors_between", return_value=[snapshot_with_no_summary]):
        with pytest.raises(ValidationException):
            validation_history(
                table,
                newest_snapshot,
                oldest_snapshot,
                {Operation.APPEND},
                ManifestContent.DATA,
            )


def test_validation_history_fails_on_from_snapshot_not_matching_last_snapshot(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
) -> None:
    """Test the validation history function fails when from_snapshot doesn't match last_snapshot."""
    table, mock_manifests = table_v2_with_extensive_snapshots_and_manifests

    oldest_snapshot = table.snapshots()[0]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    missing_oldest_snapshot = table.snapshots()[1:]

    with patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect):
        with patch("pyiceberg.table.update.validate.ancestors_between", return_value=missing_oldest_snapshot):
            with pytest.raises(ValidationException):
                validation_history(
                    table,
                    newest_snapshot,
                    oldest_snapshot,
                    {Operation.APPEND},
                    ManifestContent.DATA,
                )
