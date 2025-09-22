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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import TYPE_CHECKING, Optional, Set, cast
from urllib.parse import urlparse

from pyiceberg.utils.concurrent import ExecutorFactory

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from pyiceberg.table import Table
    from pyiceberg.table.update.snapshot import ExpireSnapshots


@dataclass(kw_only=True)
class RemoveOrphansResult:
    deleted_files: Set[str]
    failed_deletions: Set[str]


class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

    def _orphaned_files(self, location: str, older_than: Optional[timedelta] = None) -> Set[str]:
        """Get all files which are not referenced in any metadata files of an Iceberg table and can thus be considered "orphaned".

        Args:
            location: The location to check for orphaned files.
            older_than: The time period to check for orphaned files. Defaults to 3 days.

        Returns:
            A set of orphaned file paths.
        """
        from pyiceberg.io.fsspec import FsspecFileIO

        if older_than is None:
            older_than = timedelta(0)
        as_of = datetime.now(timezone.utc) - older_than

        if isinstance(self.tbl.io, FsspecFileIO):
            logger.info("Determined io to be FsspecFileIO.")
            uri = urlparse(location)
            fs = self.tbl.io.get_fs(uri.scheme)
            all_files = [f for f in fs.glob(f"{location}/**") if fs.isfile(f) and fs.modified(f) < as_of]
        else:
            logger.info("Determined io to be PyArrowFileIO.")
            try:
                import pyarrow as pa  # noqa: F401
            except ModuleNotFoundError as e:
                raise ModuleNotFoundError(
                    "For deleting orphaned files with a PyArrowFileIO, PyArrow needs to be installed"
                ) from e

            from pyarrow.fs import FileSelector, FileType

            from pyiceberg.io.pyarrow import PyArrowFileIO

            self.tbl.io = cast(PyArrowFileIO, self.tbl.io)

            scheme, _, _ = PyArrowFileIO.parse_location(location)
            fs = self.tbl.io.fs_by_scheme(scheme, None)
            _, _, path = self.tbl.io.parse_location(location)
            selector = FileSelector(path, recursive=True)
            all_files = [f.path for f in fs.get_file_info(selector) if f.type == FileType.File and f.mtime < as_of]

        all_known_files = self.tbl.inspect._all_known_files()
        flat_known_files: set[str] = reduce(set.union, all_known_files.values(), set())

        orphaned_files = set(all_files).difference(flat_known_files)

        return orphaned_files

    def remove_orphaned_files(self, older_than: Optional[timedelta] = None, dry_run: bool = False) -> RemoveOrphansResult:
        """Remove files which are not referenced in any metadata files of an Iceberg table and can thus be considered "orphaned".

        Args:
            older_than: The time period to check for orphaned files. Defaults to 3 days.
            dry_run: If True, only log the files that would be deleted. Defaults to False.
        """
        location = self.tbl.location()
        orphaned_files = self._orphaned_files(location, older_than)
        logger.info(f"Found {len(orphaned_files)} orphaned files at {location}!")
        deleted_files = set()
        failed_to_delete_files = set()

        def _delete(file: str) -> None:
            # don't error if the file doesn't exist
            # still catch ctrl-c, etc.
            try:
                self.tbl.io.delete(file)
                deleted_files.add(file)
            except Exception:
                failed_to_delete_files.add(file)

        if orphaned_files:
            if dry_run:
                logger.info(f"(Dry Run) Deleted {len(orphaned_files)} orphaned files at {location}!")
            else:
                executor = ExecutorFactory.get_or_create()
                deletes = executor.map(_delete, orphaned_files)
                # exhaust
                list(deletes)
                logger.info(f"Deleted {len(deleted_files)} orphaned files at {location}!")
                if failed_to_delete_files:
                    logger.warning(f"Failed to delete {len(failed_to_delete_files)} orphaned files at {location}!")
        else:
            logger.info(f"No orphaned files found at {location}!")

        return RemoveOrphansResult(deleted_files=deleted_files, failed_deletions=failed_to_delete_files)

    def expire_snapshots(self) -> ExpireSnapshots:
        """Return an ExpireSnapshots builder for snapshot expiration operations.

        Returns:
            ExpireSnapshots builder for configuring and executing snapshot expiration.
        """
        from pyiceberg.table import Transaction
        from pyiceberg.table.update.snapshot import ExpireSnapshots

        return ExpireSnapshots(transaction=Transaction(self.tbl, autocommit=True))
