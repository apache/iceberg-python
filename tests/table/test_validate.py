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
from pyiceberg.manifest import ManifestContent, ManifestEntry, ManifestEntryStatus, ManifestFile
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update.validate import (
    _added_data_files,
    _deleted_data_files,
    _validate_added_data_files,
    _validate_deleted_data_files,
    _validation_history,
)


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
        manifests, snapshots = _validation_history(
            table,
            oldest_snapshot,
            newest_snapshot,
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
            _validation_history(
                table,
                oldest_snapshot,
                newest_snapshot,
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
                _validation_history(
                    table,
                    oldest_snapshot,
                    newest_snapshot,
                    {Operation.APPEND},
                    ManifestContent.DATA,
                )


def test_deleted_data_files(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
) -> None:
    table, mock_manifests = table_v2_with_extensive_snapshots_and_manifests

    oldest_snapshot = table.snapshots()[0]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    # every snapshot is an append, so we should get nothing!
    with patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect):
        result = list(
            _deleted_data_files(
                table=table,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
                partition_set=None,
            )
        )

        assert result == []

    # modify second to last snapshot to be a delete
    snapshots = table.snapshots()
    altered_snapshot = snapshots[-2]
    altered_snapshot = altered_snapshot.model_copy(update={"summary": Summary(operation=Operation.DELETE)})
    snapshots[-2] = altered_snapshot

    table.metadata = table.metadata.model_copy(
        update={"snapshots": snapshots},
    )

    my_entry = ManifestEntry.from_args(
        status=ManifestEntryStatus.DELETED,
        snapshot_id=altered_snapshot.snapshot_id,
    )

    with (
        patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect),
        patch("pyiceberg.manifest.ManifestFile.fetch_manifest_entry", return_value=[my_entry]),
    ):
        result = list(
            _deleted_data_files(
                table=table,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
                partition_set=None,
            )
        )

        assert result == [my_entry]


def test_validate_deleted_data_files_raises_on_conflict(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
) -> None:
    table, _ = table_v2_with_extensive_snapshots_and_manifests
    oldest_snapshot = table.snapshots()[0]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    class DummyEntry:
        snapshot_id = 123

    with patch("pyiceberg.table.update.validate._deleted_data_files", return_value=[DummyEntry()]):
        with pytest.raises(ValidationException):
            _validate_deleted_data_files(
                table=table,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
            )


@pytest.mark.parametrize("operation", [Operation.APPEND, Operation.OVERWRITE])
def test_validate_added_data_files_conflicting_count(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
    operation: Operation,
) -> None:
    table, mock_manifests = table_v2_with_extensive_snapshots_and_manifests

    snapshot_history = 100
    snapshots = table.snapshots()
    for i in range(1, snapshot_history + 1):
        altered_snapshot = snapshots[-i]
        altered_snapshot = altered_snapshot.model_copy(update={"summary": Summary(operation=operation)})
        snapshots[-i] = altered_snapshot

    table.metadata = table.metadata.model_copy(
        update={"snapshots": snapshots},
    )

    oldest_snapshot = table.snapshots()[-snapshot_history]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    def mock_fetch_manifest_entry(self: ManifestFile, io: FileIO, discard_deleted: bool = True) -> list[ManifestEntry]:
        return [
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=self.added_snapshot_id,
            )
        ]

    with (
        patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect),
        patch("pyiceberg.manifest.ManifestFile.fetch_manifest_entry", new=mock_fetch_manifest_entry),
    ):
        result = list(
            _added_data_files(
                table=table,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
                partition_set=None,
            )
        )

        # since we only look at the ManifestContent.Data files
        assert len(result) == snapshot_history / 2


@pytest.mark.parametrize("operation", [Operation.DELETE, Operation.REPLACE])
def test_validate_added_data_files_non_conflicting_count(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
    operation: Operation,
) -> None:
    table, mock_manifests = table_v2_with_extensive_snapshots_and_manifests

    snapshot_history = 100
    snapshots = table.snapshots()
    for i in range(1, snapshot_history + 1):
        altered_snapshot = snapshots[-i]
        altered_snapshot = altered_snapshot.model_copy(update={"summary": Summary(operation=operation)})
        snapshots[-i] = altered_snapshot

    table.metadata = table.metadata.model_copy(
        update={"snapshots": snapshots},
    )

    oldest_snapshot = table.snapshots()[-snapshot_history]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    def mock_read_manifest_side_effect(self: Snapshot, io: FileIO) -> list[ManifestFile]:
        """Mock the manifests method to use the snapshot_id for lookup."""
        snapshot_id = self.snapshot_id
        if snapshot_id in mock_manifests:
            return mock_manifests[snapshot_id]
        return []

    def mock_fetch_manifest_entry(self: ManifestFile, io: FileIO, discard_deleted: bool = True) -> list[ManifestEntry]:
        return [
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=self.added_snapshot_id,
            )
        ]

    with (
        patch("pyiceberg.table.snapshots.Snapshot.manifests", new=mock_read_manifest_side_effect),
        patch("pyiceberg.manifest.ManifestFile.fetch_manifest_entry", new=mock_fetch_manifest_entry),
    ):
        result = list(
            _added_data_files(
                table=table,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
                partition_set=None,
            )
        )

        assert len(result) == 0


def test_validate_added_data_files_raises_on_conflict(
    table_v2_with_extensive_snapshots_and_manifests: tuple[Table, dict[int, list[ManifestFile]]],
) -> None:
    table, _ = table_v2_with_extensive_snapshots_and_manifests
    oldest_snapshot = table.snapshots()[0]
    newest_snapshot = cast(Snapshot, table.current_snapshot())

    class DummyEntry:
        snapshot_id = 123

    with patch("pyiceberg.table.update.validate._added_data_files", return_value=[DummyEntry()]):
        with pytest.raises(ValidationException):
            _validate_added_data_files(
                table=table,
                starting_snapshot=newest_snapshot,
                data_filter=None,
                parent_snapshot=oldest_snapshot,
            )
