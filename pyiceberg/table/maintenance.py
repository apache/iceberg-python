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
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, List, Optional, Set, Union

from pyiceberg.manifest import DataFile
from pyiceberg.utils.concurrent import ThreadPoolExecutor

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
            if txn.table_metadata.snapshot_by_id(snapshot_id) is None:
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
        snapshots_to_expire = []

        for snapshot in self.tbl.metadata.snapshots:
            if snapshot.timestamp_ms < timestamp_ms and snapshot.snapshot_id not in protected_ids:
                snapshots_to_expire.append(snapshot.snapshot_id)

        if snapshots_to_expire:
            with self.tbl.transaction() as txn:
                from pyiceberg.table.update import RemoveSnapshotsUpdate

                txn._apply((RemoveSnapshotsUpdate(snapshot_ids=snapshots_to_expire),))

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

    def _get_all_datafiles(
        self,
        scan_all_snapshots: bool = False,
        target_file_path: Optional[str] = None,
        parallel: bool = True,
    ) -> List[DataFile]:
        """
        Collect all DataFiles in the table, optionally filtering by file path.
        """
        datafiles: List[DataFile] = []

        def process_manifest(manifest) -> list[DataFile]:
            found: list[DataFile] = []
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io):
                if hasattr(entry, "data_file"):
                    df = entry.data_file
                    if target_file_path is None or df.file_path == target_file_path:
                        found.append(df)
            return found

        if scan_all_snapshots:
            manifests = []
            for snapshot in self.tbl.snapshots():
                manifests.extend(snapshot.manifests(io=self.tbl.io))
            if parallel:
                with ThreadPoolExecutor() as executor:
                    results = executor.map(process_manifest, manifests)
                    for res in results:
                        datafiles.extend(res)
            else:
                for manifest in manifests:
                    datafiles.extend(process_manifest(manifest))
        else:
            # Only current snapshot
            for chunk in self.tbl.inspect.data_files().to_pylist():
                file_path = chunk.get("file_path")
                partition = chunk.get("partition", {})
                if target_file_path is None or file_path == target_file_path:
                    datafiles.append(DataFile(file_path=file_path, partition=partition))
        return datafiles

    def deduplicate_data_files(
        self,
        scan_all_partitions: bool = True,
        scan_all_snapshots: bool = False,
        to_remove: Optional[List[Union[DataFile, str]]] = None,
        parallel: bool = True,
    ) -> List[DataFile]:
        """
        Remove duplicate data files from an Iceberg table.

        Args:
            scan_all_partitions: If True, scan all partitions for duplicates (uses file_path+partition as key).
            scan_all_snapshots: If True, scan all snapshots for duplicates, otherwise only current snapshot.
            to_remove: List of DataFile objects or file path strings to remove. If None, auto-detect duplicates.
            parallel: If True, parallelize manifest traversal.

        Returns:
            List of removed DataFile objects.
        """
        removed: List[DataFile] = []

        # Determine what to remove
        if to_remove is None:
            # Auto-detect duplicates
            all_datafiles = self._get_all_datafiles(scan_all_snapshots=scan_all_snapshots, parallel=parallel)
            seen = {}
            duplicates = []
            for df in all_datafiles:
                partition = dict(df.partition) if hasattr(df.partition, "items") else df.partition
                if scan_all_partitions:
                    key = (df.file_path, tuple(sorted(partition.items())) if partition else None)
                else:
                    key = df.file_path
                if key in seen:
                    duplicates.append(df)
                else:
                    seen[key] = df
            to_remove = duplicates

        # Normalize to DataFile objects
        normalized_to_remove: List[DataFile] = []
        all_datafiles = self._get_all_datafiles(scan_all_snapshots=scan_all_snapshots, parallel=parallel)
        for item in to_remove or []:
            if isinstance(item, DataFile):
                normalized_to_remove.append(item)
            elif isinstance(item, str):
                # Remove all DataFiles with this file_path
                for df in all_datafiles:
                    if df.file_path == item:
                        normalized_to_remove.append(df)
            else:
                raise ValueError(f"Unsupported type in to_remove: {type(item)}")

        # Remove the DataFiles
        for df in normalized_to_remove:
            self.tbl.transaction().update_snapshot().overwrite().delete_data_file(df).commit()
            removed.append(df)

        return removed
