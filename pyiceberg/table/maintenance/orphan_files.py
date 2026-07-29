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
"""Action that removes files from storage that are not reachable from table metadata.

Lists the table's storage location, computes the set of files referenced by any valid
snapshot or metadata file, and deletes the difference.

Only acts on files older than 3 days by default.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable
from concurrent.futures import as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.properties import property_as_bool

if TYPE_CHECKING:
    from pyiceberg.io import FileEntry
    from pyiceberg.table import Table

logger = logging.getLogger(__name__)


class PrefixMismatchMode(str, Enum):
    """How to treat listed files whose URI scheme or authority differs from the referenced file.

    Files may match a referenced path component-for-component but be served through a different
    scheme (s3 vs s3a) or endpoint authority. Use ``equal_schemes`` / ``equal_authorities`` to
    declare equivalences; this mode chooses what to do with anything that remains ambiguous.
    """

    ERROR = "ERROR"
    IGNORE = "IGNORE"
    DELETE = "DELETE"


@dataclass(frozen=True)
class RemoveOrphanFilesResult:
    """Outcome of a RemoveOrphanFiles execution."""

    orphan_file_locations: list[str] = field(default_factory=list)
    deleted_files: list[str] = field(default_factory=list)
    failed_to_delete: list[str] = field(default_factory=list)
    total_bytes: int = 0


_DEFAULT_OLDER_THAN = timedelta(days=3)
_DEFAULT_EQUAL_SCHEMES = {"s3a": "s3", "s3n": "s3"}


class RemoveOrphanFiles:
    r"""Builder for the remove-orphan-files action.

    Usage::

        result = table.maintenance.remove_orphan_files() \
            .older_than(datetime.now(tz=timezone.utc) - timedelta(days=7)) \
            .execute()
    """

    _table: Table
    _location: str | None
    _older_than_ms: int
    _dry_run: bool
    _delete_with: Callable[[str], None] | None
    _max_concurrency: int | None
    _prefix_mismatch_mode: PrefixMismatchMode
    _equal_schemes: dict[str, str]
    _equal_authorities: dict[str, str]
    _compare_to_file_list: Iterable[tuple[str, datetime]] | None

    def __init__(self, table: Table) -> None:
        self._table = table
        self._location = None
        self._older_than_ms = _now_ms() - int(_DEFAULT_OLDER_THAN.total_seconds() * 1000)
        self._dry_run = False
        self._delete_with = None
        self._max_concurrency = None
        self._prefix_mismatch_mode = PrefixMismatchMode.ERROR
        self._equal_schemes = dict(_DEFAULT_EQUAL_SCHEMES)
        self._equal_authorities = {}
        self._compare_to_file_list = None

    def location(self, location: str) -> RemoveOrphanFiles:
        """Restrict the scan to a specific location. Defaults to the table's root location."""
        self._location = location
        return self

    def older_than(self, value: datetime | timedelta) -> RemoveOrphanFiles:
        """Only consider files modified strictly before this point.

        Accepts either an absolute datetime or a timedelta interpreted as "files older
        than this much" relative to now. Defaults to 3 days ago.
        """
        if isinstance(value, timedelta):
            self._older_than_ms = _now_ms() - int(value.total_seconds() * 1000)
        else:
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            self._older_than_ms = int(value.timestamp() * 1000)
        return self

    def dry_run(self, enabled: bool = True) -> RemoveOrphanFiles:
        """When enabled, identify orphans but do not delete them."""
        self._dry_run = enabled
        return self

    def delete_with(self, delete_func: Callable[[str], None]) -> RemoveOrphanFiles:
        """Use a custom deleter instead of FileIO.delete.

        Useful for dry runs that collect orphans, or for routing deletes through a
        different sink.
        """
        self._delete_with = delete_func
        return self

    def max_concurrency(self, max_workers: int) -> RemoveOrphanFiles:
        """Override the worker count for manifest reads and deletes."""
        if max_workers <= 0:
            raise ValueError(f"max_concurrency must be positive, got {max_workers}")
        self._max_concurrency = max_workers
        return self

    def prefix_mismatch_mode(self, mode: PrefixMismatchMode) -> RemoveOrphanFiles:
        """Set how to handle scheme/authority mismatches between listed and referenced files."""
        self._prefix_mismatch_mode = mode
        return self

    def equal_schemes(self, schemes: dict[str, str]) -> RemoveOrphanFiles:
        """Declare schemes that should be considered equivalent.

        Keys may be comma-separated lists of schemes that map to the canonical value, e.g.
        ``{"s3a,s3n": "s3"}``. Extends (not replaces) the default mapping.
        """
        self._equal_schemes = dict(_DEFAULT_EQUAL_SCHEMES)
        self._equal_schemes.update(_flatten_mapping(schemes))
        return self

    def equal_authorities(self, authorities: dict[str, str]) -> RemoveOrphanFiles:
        """Declare authorities (host[:port]) that should be considered equivalent.

        Keys may be comma-separated lists.
        """
        self._equal_authorities = _flatten_mapping(authorities)
        return self

    def compare_to_file_list(self, files: Iterable[tuple[str, datetime]]) -> RemoveOrphanFiles:
        """Skip the storage listing step and use the provided ``(path, last_modified)`` pairs.

        Useful when a caller has already enumerated storage (e.g. from an external inventory).
        The same ``location`` and ``older_than`` filters still apply.
        """
        self._compare_to_file_list = files
        return self

    def execute(self) -> RemoveOrphanFilesResult:
        """Run the action and return the result."""
        properties = self._table.metadata.properties
        if not property_as_bool(properties, "gc.enabled", True):
            raise ValueError(
                "Cannot remove orphan files: gc.enabled is false on this table "
                "(deleting files may corrupt other tables that reference them)"
            )

        scan_location = self._location or self._table.metadata.location

        referenced = self._collect_referenced_files()

        candidates = self._collect_candidate_files(scan_location)
        orphans, conflicts = _find_orphans(
            candidates,
            referenced,
            self._equal_schemes,
            self._equal_authorities,
            self._prefix_mismatch_mode,
        )

        if conflicts and self._prefix_mismatch_mode == PrefixMismatchMode.ERROR:
            raise ValueError(
                "Unable to determine whether certain files are orphan. Metadata references "
                "files that match listed files except for authority/scheme. Resolve by passing "
                "equal_schemes() / equal_authorities(), or set prefix_mismatch_mode to IGNORE or "
                f"DELETE. Conflicting prefixes: {sorted(conflicts)}"
            )

        total_bytes = sum(size for _, size in orphans)
        orphan_locations = [path for path, _ in orphans]

        if self._dry_run:
            return RemoveOrphanFilesResult(
                orphan_file_locations=orphan_locations,
                deleted_files=[],
                failed_to_delete=[],
                total_bytes=total_bytes,
            )

        deleted, failed = self._delete_files(orphan_locations)
        return RemoveOrphanFilesResult(
            orphan_file_locations=orphan_locations,
            deleted_files=deleted,
            failed_to_delete=failed,
            total_bytes=total_bytes,
        )

    def _collect_referenced_files(self) -> set[str]:
        """Build the full set of file paths reachable from the table's current metadata."""
        table = self._table
        metadata = table.metadata
        io = table.io

        referenced: set[str] = set()
        referenced.add(table.metadata_location)
        for entry in metadata.metadata_log:
            referenced.add(entry.metadata_file)
        for stat in metadata.statistics:
            referenced.add(stat.statistics_path)
        for pstat in metadata.partition_statistics:
            referenced.add(pstat.statistics_path)

        snapshots = list(metadata.snapshots)
        if not snapshots:
            return referenced

        executor = ExecutorFactory.get_or_create()

        for snapshot in snapshots:
            if snapshot.manifest_list:
                referenced.add(snapshot.manifest_list)

        manifest_lists = list(executor.map(lambda s: s.manifests(io), snapshots))
        unique_manifests = {m.manifest_path: m for ms in manifest_lists for m in ms}
        for path in unique_manifests:
            referenced.add(path)

        entries_iter = executor.map(
            lambda m: m.fetch_manifest_entry(io=io, discard_deleted=False),
            list(unique_manifests.values()),
        )
        for entries in entries_iter:
            for entry in entries:
                referenced.add(entry.data_file.file_path)

        return referenced

    def _collect_candidate_files(self, scan_location: str) -> list[tuple[str, int]]:
        """List files to consider for deletion, applying the ``older_than`` filter."""
        location_prefix = self._location
        cutoff_ms = self._older_than_ms

        def keep(path: str, last_modified_ms: int | None) -> bool:
            if location_prefix is not None and not path.startswith(location_prefix):
                return False
            if last_modified_ms is None:
                return True
            return last_modified_ms < cutoff_ms

        results: list[tuple[str, int]] = []

        if self._compare_to_file_list is not None:
            for path, ts in self._compare_to_file_list:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if keep(path, int(ts.timestamp() * 1000)):
                    results.append((path, 0))
            return results

        for entry in self._iter_listing(scan_location):
            entry_ts_ms = int(entry.last_modified.timestamp() * 1000) if entry.last_modified else None
            if keep(entry.location, entry_ts_ms):
                results.append((entry.location, entry.size))
        return results

    def _iter_listing(self, scan_location: str) -> Iterable[FileEntry]:
        try:
            return self._table.io.list_prefix(scan_location)
        except NotImplementedError as e:
            raise NotImplementedError(
                f"FileIO {type(self._table.io).__name__} does not implement list_prefix; "
                "pass compare_to_file_list() or use a FileIO that supports listing."
            ) from e

    def _delete_files(self, orphan_paths: list[str]) -> tuple[list[str], list[str]]:
        if not orphan_paths:
            return [], []

        deleter = self._delete_with or self._table.io.delete

        if self._max_concurrency == 1 or len(orphan_paths) == 1:
            deleted: list[str] = []
            failed: list[str] = []
            for path in orphan_paths:
                try:
                    deleter(path)
                    deleted.append(path)
                except Exception as e:
                    logger.warning("Failed to delete orphan file %s: %s", path, e)
                    failed.append(path)
            return deleted, failed

        executor = ExecutorFactory.get_or_create()
        futures = {executor.submit(deleter, path): path for path in orphan_paths}
        deleted = []
        failed = []
        for fut in as_completed(futures):
            path = futures[fut]
            try:
                fut.result()
                deleted.append(path)
            except Exception as e:
                logger.warning("Failed to delete orphan file %s: %s", path, e)
                failed.append(path)
        return deleted, failed


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _flatten_mapping(mapping: dict[str, str]) -> dict[str, str]:
    """Expand comma-separated keys, e.g. ``{"s3a,s3n": "s3"}`` → ``{"s3a": "s3", "s3n": "s3"}``."""
    out: dict[str, str] = {}
    for keys, value in mapping.items():
        for key in keys.split(","):
            key = key.strip()
            if key:
                out[key] = value.strip()
    return out


def _find_orphans(
    candidates: list[tuple[str, int]],
    referenced: Iterable[str],
    equal_schemes: dict[str, str],
    equal_authorities: dict[str, str],
    mode: PrefixMismatchMode,
) -> tuple[list[tuple[str, int]], set[tuple[str, str]]]:
    """Return (orphans, prefix-mismatch conflicts) for the given candidate/referenced sets."""
    referenced_set = referenced if isinstance(referenced, set) else set(referenced)
    normalized_referenced = {_normalize(p, equal_schemes, equal_authorities): p for p in referenced_set}

    orphans: list[tuple[str, int]] = []
    conflicts: set[tuple[str, str]] = set()
    for path, size in candidates:
        if path in referenced_set:
            continue
        normalized = _normalize(path, equal_schemes, equal_authorities)
        if normalized in normalized_referenced:
            referenced_original = normalized_referenced[normalized]
            conflict = _conflict(referenced_original, path, equal_schemes, equal_authorities)
            if conflict is not None:
                if mode == PrefixMismatchMode.DELETE:
                    orphans.append((path, size))
                else:
                    conflicts.add(conflict)
            continue
        orphans.append((path, size))
    return orphans, conflicts


_REPEATED_SLASH = re.compile(r"/+")


def _normalize(path: str, equal_schemes: dict[str, str], equal_authorities: dict[str, str]) -> str:
    """Reduce a path to its canonical form for set membership comparisons.

    Drops scheme and authority so two references pointing at the same logical object
    (e.g. ``s3://b/x`` vs ``s3a://b/x``) compare equal. Collapses runs of slashes so
    ``file:///a///b`` matches ``file:///a/b``. Scheme/authority mismatches are surfaced
    separately by ``_conflict`` so the caller can report what differed.
    """
    parsed = urlparse(path)
    body = parsed.path if parsed.scheme else path
    return _REPEATED_SLASH.sub("/", body) if body else path


def _conflict(
    referenced_path: str,
    candidate_path: str,
    equal_schemes: dict[str, str],
    equal_authorities: dict[str, str],
) -> tuple[str, str] | None:
    """Return a (referenced_prefix, candidate_prefix) pair if their scheme or authority differs."""
    ref = urlparse(referenced_path)
    cand = urlparse(candidate_path)
    ref_scheme = equal_schemes.get(ref.scheme, ref.scheme)
    cand_scheme = equal_schemes.get(cand.scheme, cand.scheme)
    ref_auth = equal_authorities.get(ref.netloc, ref.netloc)
    cand_auth = equal_authorities.get(cand.netloc, cand.netloc)
    if ref_scheme == cand_scheme and ref_auth == cand_auth:
        return None
    return (f"{ref_scheme}://{ref_auth}", f"{cand_scheme}://{cand_auth}")
