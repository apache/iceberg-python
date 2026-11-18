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
from typing import TYPE_CHECKING

from pyiceberg.table.statistics import PartitionStatisticsFile
from pyiceberg.table.update import (
    RemovePartitionStatisticsUpdate,
    SetPartitionStatisticsUpdate,
    TableUpdate,
    UpdatesAndRequirements,
    UpdateTableMetadata,
)

if TYPE_CHECKING:
    from pyiceberg.table import Transaction


class UpdatePartitionStatistics(UpdateTableMetadata["UpdatePartitionStatistics"]):
    """
    Run partition statistics management operations using APIs.

    APIs include set_partition_statistics and remove_partition_statistics operations.

    Use table.update_partition_statistics().<operation>().commit() to run a specific operation.
    Use table.update_partition_statistics().<operation-one>().<operation-two>().commit() to run multiple operations.

    Pending changes are applied on commit. Sets are applied before removals, so a removal wins over a set
    for the same snapshot, regardless of the order of the calls.

    We can also use context managers to make more changes. For example:

    with table.update_partition_statistics() as update:
        update.set_partition_statistics(snapshot_id=1, statistics_path="s3://bucket/stats.parquet", file_size_in_bytes=124)
        update.remove_partition_statistics(snapshot_id=2)
    """

    _sets: dict[int, SetPartitionStatisticsUpdate]
    _removals: dict[int, RemovePartitionStatisticsUpdate]

    def __init__(self, transaction: "Transaction") -> None:
        super().__init__(transaction)
        self._sets = {}
        self._removals = {}

    def set_partition_statistics(
        self, snapshot_id: int, statistics_path: str, file_size_in_bytes: int
    ) -> "UpdatePartitionStatistics":
        """Set the partition statistics file of a snapshot, replacing the file already set for that snapshot.

        Args:
            snapshot_id: The ID of the snapshot that the statistics file describes.
            statistics_path: The location of the partition statistics file.
            file_size_in_bytes: The size of the partition statistics file in bytes.

        Returns:
            This for method chaining.
        """
        partition_statistics_file = PartitionStatisticsFile(
            snapshot_id=snapshot_id, statistics_path=statistics_path, file_size_in_bytes=file_size_in_bytes
        )
        self._sets[snapshot_id] = SetPartitionStatisticsUpdate(partition_statistics=partition_statistics_file)

        return self

    def remove_partition_statistics(self, snapshot_id: int) -> "UpdatePartitionStatistics":
        """Remove the partition statistics file of a snapshot. Removing a snapshot that has no file is a no-op.

        Args:
            snapshot_id: The ID of the snapshot whose partition statistics file is removed.

        Returns:
            This for method chaining.
        """
        self._removals[snapshot_id] = RemovePartitionStatisticsUpdate(snapshot_id=snapshot_id)

        return self

    def _commit(self) -> UpdatesAndRequirements:
        # Sets come before removals, as in Java's SetPartitionStatistics, so a removal wins over a set
        updates: tuple[TableUpdate, ...] = (*self._sets.values(), *self._removals.values())
        return updates, ()
