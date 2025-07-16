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

import logging
from typing import TYPE_CHECKING, List, Optional, Set

from pyiceberg.manifest import DataFile, ManifestFile
from pyiceberg.utils.concurrent import ThreadPoolExecutor  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata


class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

    def expire_snapshot_by_id(self, snapshot_id: int) -> None:
        """Expire a single snapshot by its ID.

        Args:
            snapshot_id: The ID of the snapshot to expire.

        Raises:
            ValueError: If the snapshot does not exist or is protected.
        """
        with self.tbl.transaction() as txn:
            # Check if snapshot exists
            if not any(snapshot.snapshot_id == snapshot_id for snapshot in txn.table_metadata.snapshots):
                raise ValueError(f"Snapshot with ID {snapshot_id} does not exist.")

            # Check if snapshot is protected
            protected_ids = self._get_protected_snapshot_ids(txn.table_metadata)
            if snapshot_id in protected_ids:
                raise ValueError(f"Snapshot with ID {snapshot_id} is protected and cannot be expired.")

            # Remove the snapshot
            from pyiceberg.table.update import RemoveSnapshotsUpdate

            txn._apply((RemoveSnapshotsUpdate(snapshot_ids=[snapshot_id]),))

    def _expire_snapshots_by_ids(self, snapshot_ids: List[int]) -> None:
        """Expire multiple snapshots by their IDs.

        Args:
            snapshot_ids: List of snapshot IDs to expire.

        Raises:
            ValueError: If any snapshot does not exist or is protected.
        """
        with self.tbl.transaction() as txn:
            protected_ids = self._get_protected_snapshot_ids(txn.table_metadata)

            # Validate all snapshots before expiring any
            for snapshot_id in snapshot_ids:
                if txn.table_metadata.snapshot_by_id(snapshot_id) is None:
                    raise ValueError(f"Snapshot with ID {snapshot_id} does not exist.")
                if snapshot_id in protected_ids:
                    raise ValueError(f"Snapshot with ID {snapshot_id} is protected and cannot be expired.")

            # Remove all snapshots
            from pyiceberg.table.update import RemoveSnapshotsUpdate

            txn._apply((RemoveSnapshotsUpdate(snapshot_ids=snapshot_ids),))

    def expire_snapshots_older_than(self, timestamp_ms: int) -> None:
        """Expire all unprotected snapshots with a timestamp older than a given value.

        Args:
            timestamp_ms: Only snapshots with timestamp_ms < this value will be expired.
        """
        # First check if there are any snapshots to expire to avoid unnecessary transactions
        protected_ids = self._get_protected_snapshot_ids(self.tbl.metadata)
        snapshots_to_expire: List[int] = []

        for snapshot in self.tbl.metadata.snapshots:
            if snapshot.timestamp_ms < timestamp_ms and snapshot.snapshot_id not in protected_ids:
                snapshots_to_expire.append(snapshot.snapshot_id)

        if snapshots_to_expire:
            self._expire_snapshots_by_ids(snapshots_to_expire)

    def expire_snapshots_older_than_with_retention(
        self, timestamp_ms: int, retain_last_n: Optional[int] = None, min_snapshots_to_keep: Optional[int] = None
    ) -> None:
        """Expire all unprotected snapshots with a timestamp older than a given value, with retention strategies.

        Args:
            timestamp_ms: Only snapshots with timestamp_ms < this value will be expired.
            retain_last_n: Always keep the last N snapshots regardless of age.
            min_snapshots_to_keep: Minimum number of snapshots to keep in total.
        """
        snapshots_to_expire = self._get_snapshots_to_expire_with_retention(
            timestamp_ms=timestamp_ms, retain_last_n=retain_last_n, min_snapshots_to_keep=min_snapshots_to_keep
        )

        if snapshots_to_expire:
            self._expire_snapshots_by_ids(snapshots_to_expire)

    def retain_last_n_snapshots(self, n: int) -> None:
        """Keep only the last N snapshots, expiring all others.

        Args:
            n: Number of most recent snapshots to keep.

        Raises:
            ValueError: If n is less than 1.
        """
        if n < 1:
            raise ValueError("Number of snapshots to retain must be at least 1")

        protected_ids = self._get_protected_snapshot_ids(self.tbl.metadata)

        # Sort snapshots by timestamp (most recent first)
        sorted_snapshots = sorted(self.tbl.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)

        # Keep the last N snapshots and all protected ones
        snapshots_to_keep = set()
        snapshots_to_keep.update(protected_ids)

        # Add the N most recent snapshots
        for i, snapshot in enumerate(sorted_snapshots):
            if i < n:
                snapshots_to_keep.add(snapshot.snapshot_id)

        # Find snapshots to expire
        snapshots_to_expire: List[int] = []
        for snapshot in self.tbl.metadata.snapshots:
            if snapshot.snapshot_id not in snapshots_to_keep:
                snapshots_to_expire.append(snapshot.snapshot_id)

        if snapshots_to_expire:
            self._expire_snapshots_by_ids(snapshots_to_expire)

    def _get_snapshots_to_expire_with_retention(
        self, timestamp_ms: Optional[int] = None, retain_last_n: Optional[int] = None, min_snapshots_to_keep: Optional[int] = None
    ) -> List[int]:
        """Get snapshots to expire considering retention strategies.

        Args:
            timestamp_ms: Only snapshots with timestamp_ms < this value will be considered for expiration.
            retain_last_n: Always keep the last N snapshots regardless of age.
            min_snapshots_to_keep: Minimum number of snapshots to keep in total.

        Returns:
            List of snapshot IDs to expire.
        """
        protected_ids = self._get_protected_snapshot_ids(self.tbl.metadata)

        # Sort snapshots by timestamp (most recent first)
        sorted_snapshots = sorted(self.tbl.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)

        # Start with all snapshots that could be expired
        candidates_for_expiration = []
        snapshots_to_keep = set(protected_ids)

        # Apply retain_last_n constraint
        if retain_last_n is not None:
            for i, snapshot in enumerate(sorted_snapshots):
                if i < retain_last_n:
                    snapshots_to_keep.add(snapshot.snapshot_id)

        # Apply timestamp constraint
        for snapshot in self.tbl.metadata.snapshots:
            if snapshot.snapshot_id not in snapshots_to_keep and (timestamp_ms is None or snapshot.timestamp_ms < timestamp_ms):
                candidates_for_expiration.append(snapshot)

        # Sort candidates by timestamp (oldest first) for potential expiration
        candidates_for_expiration.sort(key=lambda s: s.timestamp_ms)

        # Apply min_snapshots_to_keep constraint
        total_snapshots = len(self.tbl.metadata.snapshots)
        snapshots_to_expire: List[int] = []

        for candidate in candidates_for_expiration:
            # Check if expiring this snapshot would violate min_snapshots_to_keep
            remaining_after_expiration = total_snapshots - len(snapshots_to_expire) - 1

            if min_snapshots_to_keep is None or remaining_after_expiration >= min_snapshots_to_keep:
                snapshots_to_expire.append(candidate.snapshot_id)
            else:
                # Stop expiring to maintain minimum count
                break

        return snapshots_to_expire

    def expire_snapshots_with_retention_policy(
        self, timestamp_ms: Optional[int] = None, retain_last_n: Optional[int] = None, min_snapshots_to_keep: Optional[int] = None
    ) -> None:
        """Comprehensive snapshot expiration with multiple retention strategies.

        This method provides a unified interface for snapshot expiration with various
        retention policies to ensure operational resilience while allowing space reclamation.

        The method will use table properties as defaults if they are set:
        - history.expire.max-snapshot-age-ms: Default for timestamp_ms if not provided
        - history.expire.min-snapshots-to-keep: Default for min_snapshots_to_keep if not provided
        - history.expire.max-ref-age-ms: Used for ref expiration (branches/tags)

        Args:
            timestamp_ms: Only snapshots with timestamp_ms < this value will be considered for expiration.
                         If None, will use history.expire.max-snapshot-age-ms table property if set.
            retain_last_n: Always keep the last N snapshots regardless of age.
                          Useful when regular snapshot creation occurs and users want to keep
                          the last few for rollback purposes.
            min_snapshots_to_keep: Minimum number of snapshots to keep in total.
                                 Acts as a guardrail to prevent aggressive expiration logic.
                                 If None, will use history.expire.min-snapshots-to-keep table property if set.

        Returns:
            List of snapshot IDs that were expired.

        Raises:
            ValueError: If retain_last_n or min_snapshots_to_keep is less than 1.

        Examples:
            # Use table property defaults
            maintenance.expire_snapshots_with_retention_policy()

            # Override defaults with explicit values
            maintenance.expire_snapshots_with_retention_policy(
                timestamp_ms=1234567890000,
                retain_last_n=10,
                min_snapshots_to_keep=5
            )
        """
        # Get default values from table properties
        default_max_age, default_min_snapshots, _ = self._get_expiration_properties()

        # Use defaults from table properties if not explicitly provided
        if timestamp_ms is None:
            timestamp_ms = default_max_age

        if min_snapshots_to_keep is None:
            min_snapshots_to_keep = default_min_snapshots

        # If no expiration criteria are provided, don't expire anything
        if timestamp_ms is None and retain_last_n is None and min_snapshots_to_keep is None:
            return

        if retain_last_n is not None and retain_last_n < 1:
            raise ValueError("retain_last_n must be at least 1")

        if min_snapshots_to_keep is not None and min_snapshots_to_keep < 1:
            raise ValueError("min_snapshots_to_keep must be at least 1")

        snapshots_to_expire = self._get_snapshots_to_expire_with_retention(
            timestamp_ms=timestamp_ms, retain_last_n=retain_last_n, min_snapshots_to_keep=min_snapshots_to_keep
        )

        if snapshots_to_expire:
            self._expire_snapshots_by_ids(snapshots_to_expire)

    def _get_protected_snapshot_ids(self, table_metadata: Optional[TableMetadata] = None) -> Set[int]:
        """Get the IDs of protected snapshots.

        These are the HEAD snapshots of all branches and all tagged snapshots.
        These ids are to be excluded from expiration.

        Args:
            table_metadata: Optional table metadata to check for protected snapshots.
                          If not provided, uses the table's current metadata.

        Returns:
            Set of protected snapshot IDs to exclude from expiration.
        """
        # Prefer provided metadata, fall back to current table metadata
        metadata = table_metadata or self.tbl.metadata
        refs = metadata.refs if metadata else {}
        return {ref.snapshot_id for ref in refs.values()}

    def _get_all_datafiles(self) -> List[DataFile]:
        """Collect all DataFiles in the current snapshot only."""
        datafiles: List[DataFile] = []

        current_snapshot = self.tbl.current_snapshot()
        if not current_snapshot:
            return datafiles

        def process_manifest(manifest: ManifestFile) -> list[DataFile]:
            found: list[DataFile] = []
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io, discard_deleted=True):
                if hasattr(entry, "data_file"):
                    found.append(entry.data_file)
            return found

        # Scan only the current snapshot's manifests
        manifests = current_snapshot.manifests(io=self.tbl.io)
        with ThreadPoolExecutor() as executor:
            results = executor.map(process_manifest, manifests)
            for res in results:
                datafiles.extend(res)

        return datafiles

    def _get_expiration_properties(self) -> tuple[Optional[int], Optional[int], Optional[int]]:
        """Get the default expiration properties from table properties.

        Returns:
            Tuple of (max_snapshot_age_ms, min_snapshots_to_keep, max_ref_age_ms)
        """
        properties = self.tbl.properties

        max_snapshot_age_ms = properties.get("history.expire.max-snapshot-age-ms")
        max_snapshot_age = int(max_snapshot_age_ms) if max_snapshot_age_ms is not None else None

        min_snapshots = properties.get("history.expire.min-snapshots-to-keep")
        min_snapshots_to_keep = int(min_snapshots) if min_snapshots is not None else None

        max_ref_age = properties.get("history.expire.max-ref-age-ms")
        max_ref_age_ms = int(max_ref_age) if max_ref_age is not None else None

        return max_snapshot_age, min_snapshots_to_keep, max_ref_age_ms

    def deduplicate_data_files(self) -> List[DataFile]:
        """
        Remove duplicate data files from an Iceberg table.

        Returns:
            List of removed DataFile objects.
        """
        import os
        from collections import defaultdict

        removed: List[DataFile] = []

        # Get the current snapshot
        current_snapshot = self.tbl.current_snapshot()
        if not current_snapshot:
            return removed

        # Collect all manifest entries from the current snapshot
        all_entries = []
        for manifest in current_snapshot.manifests(io=self.tbl.io):
            entries = list(manifest.fetch_manifest_entry(io=self.tbl.io, discard_deleted=True))
            all_entries.extend(entries)

        # Group entries by file name
        file_groups = defaultdict(list)
        for entry in all_entries:
            file_name = os.path.basename(entry.data_file.file_path)
            file_groups[file_name].append(entry)

        # Find duplicate entries to remove
        has_duplicates = False
        files_to_remove = []
        files_to_keep = []

        for _file_name, entries in file_groups.items():
            if len(entries) > 1:
                # Keep the first entry, remove the rest
                files_to_keep.append(entries[0].data_file)
                for duplicate_entry in entries[1:]:
                    files_to_remove.append(duplicate_entry.data_file)
                    removed.append(duplicate_entry.data_file)
                    has_duplicates = True
            else:
                # No duplicates, keep the entry
                files_to_keep.append(entries[0].data_file)

        # Only create a new snapshot if we actually have duplicates to remove
        if has_duplicates:
            with self.tbl.transaction() as txn:
                with txn.update_snapshot().overwrite() as overwrite_snapshot:
                    # First, explicitly delete all the duplicate files
                    for file_to_remove in files_to_remove:
                        overwrite_snapshot.delete_data_file(file_to_remove)

                    # Then add back only the files that should be kept
                    for file_to_keep in files_to_keep:
                        overwrite_snapshot.append_data_file(file_to_keep)

            # Refresh the table to reflect the changes
            self.tbl = self.tbl.refresh()

        return removed