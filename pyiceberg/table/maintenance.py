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
from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import TYPE_CHECKING, Set

from pyiceberg.utils.concurrent import ExecutorFactory

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from pyiceberg.table import Table


class MaintenanceTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

        try:
            import pyarrow as pa  # noqa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For metadata operations PyArrow needs to be installed") from e

    def _orphaned_files(self, location: str, older_than: timedelta = timedelta(days=3)) -> Set[str]:
        """Get all files which are not referenced in any metadata files of an Iceberg table and can thus be considered "orphaned".

        Args:
            location: The location to check for orphaned files.
            older_than: The time period to check for orphaned files. Defaults to 3 days.

        Returns:
            A set of orphaned file paths.
        """
        try:
            import pyarrow as pa  # noqa: F401
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For deleting orphaned files PyArrow needs to be installed") from e

        from pyarrow.fs import FileSelector, FileType

        from pyiceberg.io.pyarrow import PyArrowFileIO

        all_known_files = self.tbl.inspect._all_known_files()
        flat_known_files: set[str] = reduce(set.union, all_known_files.values(), set())

        scheme, _, _ = PyArrowFileIO.parse_location(location)
        pyarrow_io = PyArrowFileIO()
        fs = pyarrow_io.fs_by_scheme(scheme, None)

        _, _, path = pyarrow_io.parse_location(location)
        selector = FileSelector(path, recursive=True)
        # filter to just files as it may return directories, and filter on time
        as_of = datetime.now(timezone.utc) - older_than
        all_files = [f.path for f in fs.get_file_info(selector) if f.type == FileType.File and f.mtime < as_of]

        orphaned_files = set(all_files).difference(flat_known_files)

        return orphaned_files

    def remove_orphaned_files(self, older_than: timedelta = timedelta(days=3), dry_run: bool = False) -> None:
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
                logger.info(f"Files:\n{deleted_files}")
                if failed_to_delete_files:
                    logger.warning(f"Failed to delete {len(failed_to_delete_files)} orphaned files at {location}!")
                    logger.warning(f"Files:\n{failed_to_delete_files}")
        else:
            logger.info(f"No orphaned files found at {location}!")
