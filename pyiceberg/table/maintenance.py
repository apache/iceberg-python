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
from typing import TYPE_CHECKING, Any, List, Optional, Set, Union

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

        try:
            import pyarrow as pa  # noqa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For metadata operations PyArrow needs to be installed") from e

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

    def expire_snapshots_by_ids(self, snapshot_ids: List[int]) -> None:
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
            with self.tbl.transaction() as txn:
                self.expire_snapshots_by_ids(snapshots_to_expire)

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
            with self.tbl.transaction() as txn:
                self.expire_snapshots_by_ids(snapshots_to_expire)

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
            with self.tbl.transaction() as txn:
                self.expire_snapshots_by_ids(snapshots_to_expire)

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
    ) -> List[int]:
        """Comprehensive snapshot expiration with multiple retention strategies.

        This method provides a unified interface for snapshot expiration with various
        retention policies to ensure operational resilience while allowing space reclamation.

        Args:
            timestamp_ms: Only snapshots with timestamp_ms < this value will be considered for expiration.
                         If None, all snapshots are candidates (subject to other constraints).
            retain_last_n: Always keep the last N snapshots regardless of age.
                          Useful when regular snapshot creation occurs and users want to keep
                          the last few for rollback purposes.
            min_snapshots_to_keep: Minimum number of snapshots to keep in total.
                                 Acts as a guardrail to prevent aggressive expiration logic
                                 from removing too many snapshots.

        Returns:
            List of snapshot IDs that were expired.

        Raises:
            ValueError: If retain_last_n or min_snapshots_to_keep is less than 1.

        Examples:
            # Keep last 5 snapshots regardless of age
            maintenance.expire_snapshots_with_retention_policy(retain_last_n=5)

            # Expire snapshots older than timestamp but keep at least 3 total
            maintenance.expire_snapshots_with_retention_policy(
                timestamp_ms=1234567890000,
                min_snapshots_to_keep=3
            )

            # Combined policy: expire old snapshots but keep last 10 and at least 5 total
            maintenance.expire_snapshots_with_retention_policy(
                timestamp_ms=1234567890000,
                retain_last_n=10,
                min_snapshots_to_keep=5
            )
        """
        if retain_last_n is not None and retain_last_n < 1:
            raise ValueError("retain_last_n must be at least 1")

        if min_snapshots_to_keep is not None and min_snapshots_to_keep < 1:
            raise ValueError("min_snapshots_to_keep must be at least 1")

        snapshots_to_expire = self._get_snapshots_to_expire_with_retention(
            timestamp_ms=timestamp_ms, retain_last_n=retain_last_n, min_snapshots_to_keep=min_snapshots_to_keep
        )

        if snapshots_to_expire:
            with self.tbl.transaction() as txn:
                from pyiceberg.table.update import RemoveSnapshotsUpdate

                txn._apply((RemoveSnapshotsUpdate(snapshot_ids=snapshots_to_expire),))

        return snapshots_to_expire

    def _get_protected_snapshot_ids(self, table_metadata: TableMetadata) -> Set[int]:
        """Get the IDs of protected snapshots.

        These are the HEAD snapshots of all branches and all tagged snapshots.
        These ids are to be excluded from expiration.

        Args:
            table_metadata: The table metadata to check for protected snapshots.

        Returns:
            Set of protected snapshot IDs to exclude from expiration.
        """
        from pyiceberg.table.refs import SnapshotRefType

        protected_ids: Set[int] = set()
        for ref in table_metadata.refs.values():
            if ref.snapshot_ref_type in [SnapshotRefType.TAG, SnapshotRefType.BRANCH]:
                protected_ids.add(ref.snapshot_id)
        return protected_ids

    def _get_all_datafiles(self) -> List[DataFile]:
        """Collect all DataFiles in the table, scanning all partitions."""
        datafiles: List[DataFile] = []

        def process_manifest(manifest: ManifestFile) -> list[DataFile]:
            found: list[DataFile] = []
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io):
                if hasattr(entry, "data_file"):
                    found.append(entry.data_file)
            return found

        # Scan all snapshots
        manifests = []
        for snapshot in self.tbl.snapshots():
            manifests.extend(snapshot.manifests(io=self.tbl.io))
        with ThreadPoolExecutor() as executor:
            results = executor.map(process_manifest, manifests)
            for res in results:
                datafiles.extend(res)

        return datafiles

    def _get_all_datafiles_with_context(self) -> List[tuple[DataFile, str, int]]:
        """Collect all DataFiles in the table, scanning all partitions, with manifest context."""
        datafiles: List[tuple[DataFile, str, int]] = []

        def process_manifest(manifest: ManifestFile) -> list[tuple[DataFile, str, int]]:
            found: list[tuple[DataFile, str, int]] = []
            for idx, entry in enumerate(manifest.fetch_manifest_entry(io=self.tbl.io)):
                if hasattr(entry, "data_file"):
                    found.append((entry.data_file, getattr(manifest, 'manifest_path', str(manifest)), idx))
            return found

        # Scan all snapshots
        manifests = []
        for snapshot in self.tbl.snapshots():
            manifests.extend(snapshot.manifests(io=self.tbl.io))
        with ThreadPoolExecutor() as executor:
            results = executor.map(process_manifest, manifests)
            for res in results:
                datafiles.extend(res)

        return datafiles

    def _detect_duplicates(self, all_datafiles_with_context: List[tuple[DataFile, str, int]]) -> List[DataFile]:
        """Detect duplicate data files based on file name and extension."""
        seen = {}
        processed_entries = set()
        duplicates = []

        for df, manifest_path, entry_idx in all_datafiles_with_context:
            # Extract file name and extension
            file_name_with_extension = df.file_path.split("/")[-1]
            entry_key = (manifest_path, entry_idx)

            if file_name_with_extension in seen:
                if entry_key not in processed_entries:
                    duplicates.append(df)
                    processed_entries.add(entry_key)
            else:
                seen[file_name_with_extension] = (df, manifest_path, entry_idx)

        return duplicates

    def deduplicate_data_files(self) -> List[DataFile]:
        """
        Remove duplicate data files from an Iceberg table.

        Returns:
            List of removed DataFile objects.
        """
        removed: List[DataFile] = []

        # Collect all data files
        all_datafiles_with_context = self._get_all_datafiles_with_context()

        # Detect duplicates
        duplicates = self._detect_duplicates(all_datafiles_with_context)

        # Remove the DataFiles
        for df in duplicates:
            self.tbl.transaction().update_snapshot().overwrite().delete_data_file(df)
            removed.append(df)

        return removed

    def _detect_duplicates(self, all_datafiles_with_context: List[tuple[DataFile, str, int]]) -> List[DataFile]:
        """Detect duplicate data files based on file path and partition."""
        seen = {}
        processed_entries = set()
        duplicates = []

        for df, manifest_path, entry_idx in all_datafiles_with_context:
            partition: dict[str, Any] = df.partition.to_dict() if hasattr(df.partition, "to_dict") else {}
            key = (df.file_path, tuple(sorted(partition.items())) if partition else ())
            entry_key = (manifest_path, entry_idx)

            if key in seen:
                if entry_key not in processed_entries:
                    duplicates.append(df)
                    processed_entries.add(entry_key)
            else:
                seen[key] = (df, manifest_path, entry_idx)

        return duplicates
