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

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.table.update.snapshot import ExpireSnapshots


class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

    def expire_snapshots(self) -> ExpireSnapshots:
        """Return an ExpireSnapshots builder for snapshot expiration operations.

        Returns:
            ExpireSnapshots builder for configuring and executing snapshot expiration.
        """
        from pyiceberg.table import Transaction
        from pyiceberg.table.update.snapshot import ExpireSnapshots

        return ExpireSnapshots(transaction=Transaction(self.tbl, autocommit=True))

    def compact(self) -> None:
        """Compact the table's data and delete files by reading and overwriting the entire table.

        Note: This is a full-table compaction that leverages Arrow for binpacking.
        It currently reads the entire table into memory via `.to_arrow()`.

        This reads all existing data into memory and writes it back out using the
        target file size settings (write.target-file-size-bytes), atomically
        dropping the old files and replacing them with fewer, larger files.
        """
        # Read the current table state into memory
        arrow_table = self.tbl.scan().to_arrow()

        # Guard: if the table is completely empty, there's nothing to compact.
        # Doing an overwrite with an empty table would result in deleting everything.
        if arrow_table.num_rows == 0:
            logger.info("Table contains no rows, skipping compaction.")
            return

        # Replace existing files with new compacted files
        with self.tbl.transaction() as txn:
            files_to_delete = [task.file for task in self.tbl.scan().plan_files()]
            txn.replace(
                df=arrow_table,
                files_to_delete=files_to_delete,
                snapshot_properties={"snapshot-type": "replace", "replace-operation": "compaction"},
            )
