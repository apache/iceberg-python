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
from typing import TYPE_CHECKING

from pyiceberg.table.statistics import PartitionStatisticsFile

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.table.update.snapshot import ExpireSnapshots


class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

    def compute_partition_statistics(self, snapshot_id: int | None = None) -> PartitionStatisticsFile | None:
        """Compute the partition statistics of a snapshot, write them to a file and register the file in the table metadata.

        The statistics are computed from the manifests of the snapshot. A partition statistics file that is already
        registered for the snapshot is returned as is, and nothing is written.

        Args:
            snapshot_id: The ID of the snapshot to compute the statistics for. Defaults to the current snapshot.

        Returns:
            The registered partition statistics file, or None if the table has no snapshot or the snapshot has no
            manifest entries.

        Raises:
            ValueError: If the table is not partitioned, the snapshot does not exist, the table's default file format
                has no writer, or historical partition fields have conflicting output names.
            NotImplementedError: If the table is format version 3 or later.
            ArrowNotImplementedError: If the unified partition type contains a UUID field.
        """
        from pyiceberg.table.partition_stats import compute_and_write_partition_statistics_file

        if snapshot_id is None:
            if (current_snapshot := self.tbl.current_snapshot()) is None:
                return None
            snapshot_id = current_snapshot.snapshot_id

        # A registered file can outlive its snapshot, so the snapshot is checked before the file is returned
        if self.tbl.snapshot_by_id(snapshot_id) is None:
            raise ValueError(f"Snapshot not found: {snapshot_id}")

        registered = next((file for file in self.tbl.metadata.partition_statistics if file.snapshot_id == snapshot_id), None)
        if registered is not None:
            return registered

        statistics_file = compute_and_write_partition_statistics_file(self.tbl, snapshot_id)
        if statistics_file is not None:
            self.tbl.update_partition_statistics().set_partition_statistics(
                snapshot_id=statistics_file.snapshot_id,
                statistics_path=statistics_file.statistics_path,
                file_size_in_bytes=statistics_file.file_size_in_bytes,
            ).commit()
        return statistics_file

    def expire_snapshots(self) -> ExpireSnapshots:
        """Return an ExpireSnapshots builder for snapshot expiration operations.

        Returns:
            ExpireSnapshots builder for configuring and executing snapshot expiration.
        """
        from pyiceberg.table import Transaction
        from pyiceberg.table.update.snapshot import ExpireSnapshots

        return ExpireSnapshots(transaction=Transaction(self.tbl, autocommit=True))
