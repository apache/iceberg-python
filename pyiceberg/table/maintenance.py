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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import DataFile, ManifestFile
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.utils.concurrent import ThreadPoolExecutor  # type: ignore[attr-defined]
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

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
        current_snapshot = self.tbl.current_snapshot()
        if not current_snapshot:
            return []

        def process_manifest(manifest: ManifestFile) -> List[DataFile]:
            """Process a single manifest file and return its data files."""
            datafiles = []
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io, discard_deleted=True):
                if hasattr(entry, "data_file"):
                    datafiles.append(entry.data_file)
            return datafiles

        # Get all manifests for the current snapshot
        manifests = current_snapshot.manifests(io=self.tbl.io)
        
        # Use ThreadPoolExecutor for parallel processing of manifests
        with ThreadPoolExecutor() as executor:
            # Submit all manifest processing tasks
            future_results = list(executor.map(process_manifest, manifests))
            
            # Flatten results efficiently
            datafiles = []
            for result in future_results:
                datafiles.extend(result)

        return datafiles

    def _get_all_datafiles_across_snapshots(self) -> List[DataFile]:
        """Collect all DataFiles across ALL snapshots."""
        datafiles: List[DataFile] = []
        all_snapshots = self.tbl.metadata.snapshots

        def process_manifest(manifest: ManifestFile) -> list[DataFile]:
            found: list[DataFile] = []
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io, discard_deleted=True):
                if hasattr(entry, "data_file"):
                    found.append(entry.data_file)
            return found

        # Scan all snapshots' manifests
        for snapshot in all_snapshots:
            manifests = snapshot.manifests(io=self.tbl.io)
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
        Remove duplicate data files from the current snapshot of an Iceberg table.

        This method identifies DataFile entries in the current snapshot that reference
        the same file path and removes the duplicate references, keeping only one
        reference per unique file path.

        Returns:
            List of removed DataFile objects.
        """
        from collections import defaultdict

        removed: List[DataFile] = []

        # Get all data files from current snapshot only - we can only modify the current snapshot
        current_datafiles = self._get_all_datafiles()
        if not current_datafiles:
            return removed

        # Group data files by file path (full path, not just basename)
        file_groups = defaultdict(list)
        for data_file in current_datafiles:
            file_groups[data_file.file_path].append(data_file)

        # Find duplicate files to remove and unique files to keep
        has_duplicates = False
        unique_files_to_keep = []

        for _file_path, file_list in file_groups.items():
            if len(file_list) > 1:
                # Keep the first occurrence, mark the rest as duplicates to remove
                unique_files_to_keep.append(file_list[0])
                for duplicate_file in file_list[1:]:
                    removed.append(duplicate_file)
                    has_duplicates = True
            else:
                # No duplicates, keep the file
                unique_files_to_keep.append(file_list[0])

        # Only create a new snapshot if we actually have duplicates to remove
        if has_duplicates:
            # Use overwrite correctly: first delete existing files, then append unique ones
            transaction = self.tbl.transaction()
            with transaction.update_snapshot().overwrite() as overwrite_files:
                # First, delete ALL current files
                for data_file in current_datafiles:
                    overwrite_files.delete_data_file(data_file)

                # Then append only the unique files we want to keep
                for data_file in unique_files_to_keep:
                    overwrite_files.append_data_file(data_file)

            transaction.commit_transaction()

        return removed

    def rebuild_current_snapshot(
        self,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
        **retry_kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Rebuild the current snapshot with unique data files.

        This method creates a new snapshot containing all unique data files from the current snapshot,
        effectively deduplicating any duplicate file references while preserving all unique data.

        Args:
            snapshot_properties: Properties to apply to the new snapshot.
            **retry_kwargs: Additional retry configuration parameters passed to the retry decorator.
                          Supported parameters: stop, wait, retry, reraise, etc.

        Returns:
            Dict containing operation summary with keys:
            - 'rebuilt': bool - Whether a rebuild was actually performed
            - 'total_files': int - Total number of data files processed
            - 'unique_files': int - Number of unique files after deduplication
            - 'duplicates_removed': int - Number of duplicate file references removed

        Raises:
            CommitFailedException: If the commit fails after all retries are exhausted.
        """
        # Configure retry parameters with defaults
        retry_config = {
            'stop': stop_after_attempt(3),
            'wait': wait_exponential(multiplier=1, min=2, max=10),
            'retry': retry_if_exception_type(CommitFailedException),
            'reraise': True,
        }
        # Override defaults with any provided retry_kwargs
        retry_config.update(retry_kwargs)

        # Get initial data files and track snapshot ID for optimization
        current_snapshot = self.tbl.current_snapshot()
        last_snapshot_id = current_snapshot.snapshot_id if current_snapshot else None
        current_datafiles = self._get_all_datafiles()
        
        # Early exit: Check if there are actually duplicates before proceeding
        file_path_counts: Dict[str, int] = {}
        for data_file in current_datafiles:
            file_path_counts[data_file.file_path] = file_path_counts.get(data_file.file_path, 0) + 1
        
        # If no duplicates exist, skip the rebuild entirely
        has_duplicates = any(count > 1 for count in file_path_counts.values())
        if not has_duplicates:
            return {
                'rebuilt': False,
                'total_files': len(current_datafiles),
                'unique_files': len(current_datafiles),
                'duplicates_removed': 0
            }
        
        # Only create distinct_datafiles dict if we have duplicates
        distinct_datafiles = {data_file.file_path: data_file for data_file in current_datafiles}
        duplicates_removed = len(current_datafiles) - len(distinct_datafiles)

        @retry(**retry_config)
        def attempt_rebuild_snapshot():
            nonlocal last_snapshot_id, current_datafiles, distinct_datafiles
            
            # Refresh table state on each attempt
            self.tbl.refresh()
            
            # Only re-fetch data files if snapshot ID has changed
            current_snapshot = self.tbl.current_snapshot()
            current_snapshot_id = current_snapshot.snapshot_id if current_snapshot else None
            
            if current_snapshot_id != last_snapshot_id:
                current_datafiles = self._get_all_datafiles()
                
                # Re-check for duplicates after refresh
                file_path_counts = {}
                for data_file in current_datafiles:
                    file_path_counts[data_file.file_path] = file_path_counts.get(data_file.file_path, 0) + 1
                
                # Early exit if no duplicates after refresh
                has_duplicates = any(count > 1 for count in file_path_counts.values())
                if not has_duplicates:
                    return {
                        'rebuilt': False,
                        'total_files': len(current_datafiles),
                        'unique_files': len(current_datafiles),
                        'duplicates_removed': 0
                    }
                
                distinct_datafiles = {data_file.file_path: data_file for data_file in current_datafiles}
                last_snapshot_id = current_snapshot_id

            transaction = self.tbl.transaction()
            with transaction.update_snapshot(snapshot_properties=snapshot_properties).overwrite() as overwrite_files:
                # Batch delete all current files first
                for data_file in current_datafiles:
                    overwrite_files.delete_data_file(data_file)

                # Batch append all unique files
                for data_file in distinct_datafiles.values():
                    overwrite_files.append_data_file(data_file)

            transaction.commit_transaction()

        attempt_rebuild_snapshot()
        
        return {
            'rebuilt': True,
            'total_files': len(current_datafiles),
            'unique_files': len(distinct_datafiles),
            'duplicates_removed': duplicates_removed
        }


                