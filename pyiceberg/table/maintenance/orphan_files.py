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

import logging
import re
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from pyiceberg.exceptions import ValidationException
from pyiceberg.io import _is_local_path
from pyiceberg.table import TableProperties
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.properties import property_as_bool

if TYPE_CHECKING:
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
_HIDDEN_PATH_PREFIXES = ("_", ".")


class RemoveOrphanFiles:
    r"""Builder for the remove-orphan-files action.

    Usage::

        result = table.maintenance.remove_orphan_files() \
            .older_than(datetime.now(tz=timezone.utc) - timedelta(days=7)) \
            .execute()
    """

    _table: "Table"
    _location: str | None
    _older_than_ms: int
    _dry_run: bool
    _delete_with: Callable[[str], None] | None
    _prefix_mismatch_mode: PrefixMismatchMode
    _equal_schemes: dict[str, str]
    _equal_authorities: dict[str, str]
    _compare_to_file_list: Iterable[tuple[str, datetime]] | None

    def __init__(self, table: "Table") -> None:
        self._table = table
        self._location = None
        self._older_than_ms = _now_ms() - int(_DEFAULT_OLDER_THAN.total_seconds() * 1000)
        self._dry_run = False
        self._delete_with = None
        self._prefix_mismatch_mode = PrefixMismatchMode.ERROR
        self._equal_schemes = dict(_DEFAULT_EQUAL_SCHEMES)
        self._equal_authorities = {}
        self._compare_to_file_list = None

    def location(self, location: str) -> "RemoveOrphanFiles":
        """Restrict the scan to a specific location. Defaults to the table's root location."""
        self._location = location
        return self

    def older_than(self, value: datetime | timedelta) -> "RemoveOrphanFiles":
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

    def dry_run(self, enabled: bool = True) -> "RemoveOrphanFiles":
        """When enabled, identify orphans but do not delete them."""
        self._dry_run = enabled
        return self

    def delete_with(self, delete_func: Callable[[str], None]) -> "RemoveOrphanFiles":
        """Use a custom deleter instead of FileIO.delete.

        Useful for dry runs that collect orphans, or for routing deletes through a
        different sink.
        """
        self._delete_with = delete_func
        return self

    def prefix_mismatch_mode(self, mode: PrefixMismatchMode) -> "RemoveOrphanFiles":
        """Set how to handle scheme/authority mismatches between listed and referenced files."""
        self._prefix_mismatch_mode = mode
        return self

    def equal_schemes(self, schemes: dict[str, str]) -> "RemoveOrphanFiles":
        """Declare schemes that should be considered equivalent.

        Keys may be comma-separated lists of schemes that map to the canonical value, e.g.
        ``{"s3a,s3n": "s3"}``. Extends (not replaces) the default mapping.
        """
        self._equal_schemes = dict(_DEFAULT_EQUAL_SCHEMES)
        self._equal_schemes.update(_flatten_mapping(schemes))
        return self

    def equal_authorities(self, authorities: dict[str, str]) -> "RemoveOrphanFiles":
        """Declare authorities (host[:port]) that should be considered equivalent.

        Keys may be comma-separated lists.
        """
        self._equal_authorities = _flatten_mapping(authorities)
        return self

    def compare_to_file_list(self, files: Iterable[tuple[str, datetime]]) -> "RemoveOrphanFiles":
        """Skip the storage listing step and use the provided ``(path, last_modified)`` pairs.

        Useful when a caller has already enumerated storage (e.g. from an external inventory).
        The same ``location`` and ``older_than`` filters still apply.
        """
        self._compare_to_file_list = files
        return self

    def execute(self) -> RemoveOrphanFilesResult:
        """Run the action and return the result."""
        if not property_as_bool(self._table.metadata.properties, TableProperties.GC_ENABLED, TableProperties.GC_ENABLED_DEFAULT):
            raise ValidationException(
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
            raise ValidationException(
                "Unable to determine whether certain files are orphan. Metadata references "
                "files that match listed files except for authority/scheme. Resolve by passing "
                "equal_schemes() / equal_authorities(), or set prefix_mismatch_mode to IGNORE or "
                f"DELETE. Conflicting authorities/schemes: {sorted(conflicts)}"
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
        """Build the full set of file paths reachable from the table's metadata."""
        metadata = self._table.metadata
        inspect = self._table.inspect

        referenced: set[str] = set(inspect.metadata_log_entries().column("file").to_pylist())
        referenced.update(stat.statistics_path for stat in metadata.statistics)
        referenced.update(pstat.statistics_path for pstat in metadata.partition_statistics)
        referenced.update(snapshot.manifest_list for snapshot in metadata.snapshots if snapshot.manifest_list)
        referenced.update(inspect.all_manifests().column("path").to_pylist())
        referenced.update(inspect.all_files().column("file_path").to_pylist())
        return referenced

    def _collect_candidate_files(self, scan_location: str) -> list[tuple[str, int]]:
        """List files to consider for deletion, applying the ``older_than`` filter."""
        cutoff_ms = self._older_than_ms
        results: list[tuple[str, int]] = []

        if self._compare_to_file_list is not None:
            for path, ts in self._compare_to_file_list:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if path.startswith(scan_location) and int(ts.timestamp() * 1000) < cutoff_ms:
                    results.append((path, 0))
            return results

        hidden_partition_prefixes = _hidden_partition_prefixes(self._table)
        for entry in self._table.io.list_prefix(scan_location):
            # Without a modification time there is no way to tell a leftover from a file that a
            # concurrent writer is about to commit, so leave it alone.
            if entry.last_modified is None or int(entry.last_modified.timestamp() * 1000) >= cutoff_ms:
                continue
            if _is_hidden(entry.location, scan_location, hidden_partition_prefixes):
                continue
            results.append((entry.location, entry.size))
        return results

    def _delete_files(self, orphan_paths: list[str]) -> tuple[list[str], list[str]]:
        """Delete the given files, returning the paths that were deleted and the ones that failed."""
        deleter = self._delete_with or self._table.io.delete
        executor = ExecutorFactory.get_or_create()
        futures = {executor.submit(deleter, path): path for path in orphan_paths}

        deleted: list[str] = []
        failed: list[str] = []
        for future in as_completed(futures):
            path = futures[future]
            try:
                future.result()
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


def _hidden_partition_prefixes(table: "Table") -> tuple[str, ...]:
    """Return the directory prefixes of partition fields that would otherwise look hidden.

    A table partitioned by a field named ``_c2`` writes to directories like ``_c2_trunc=AA``,
    which must survive the hidden-path filter.
    """
    return tuple(
        f"{partition_field.name}="
        for spec in table.metadata.partition_specs
        for partition_field in spec.fields
        if partition_field.name.startswith(_HIDDEN_PATH_PREFIXES)
    )


def _is_hidden(location: str, scan_location: str, hidden_partition_prefixes: tuple[str, ...]) -> bool:
    """Whether any path component below the scanned location is a hidden file or directory."""
    path = _uri_path(location)
    root = _uri_path(scan_location)
    if not path.startswith(root):
        return False
    relative = path[len(root) :]
    return any(
        component.startswith(_HIDDEN_PATH_PREFIXES) and not component.startswith(hidden_partition_prefixes)
        for component in relative.split("/")
    )


@dataclass(frozen=True)
class _FileURI:
    """A file location split into the components that decide whether two locations are the same."""

    scheme: str
    authority: str
    path: str

    def component_match(self, other: "_FileURI") -> bool:
        # An absent component on the referenced side matches anything, so metadata that stores
        # bare paths does not conflict with a listing that reports full URIs.
        return _component_match(self.scheme, other.scheme) and _component_match(self.authority, other.authority)


def _component_match(referenced: str, candidate: str) -> bool:
    return not referenced or referenced.lower() == candidate.lower()


def _find_orphans(
    candidates: list[tuple[str, int]],
    referenced: Iterable[str],
    equal_schemes: dict[str, str],
    equal_authorities: dict[str, str],
    mode: PrefixMismatchMode,
) -> tuple[list[tuple[str, int]], set[tuple[str, str]]]:
    """Return (orphans, prefix-mismatch conflicts) for the given candidate/referenced sets."""
    referenced_by_path: dict[str, list[_FileURI]] = {}
    for path in referenced:
        uri = _file_uri(path, equal_schemes, equal_authorities)
        referenced_by_path.setdefault(uri.path, []).append(uri)

    orphans: list[tuple[str, int]] = []
    conflicts: set[tuple[str, str]] = set()
    for path, size in candidates:
        candidate = _file_uri(path, equal_schemes, equal_authorities)
        matches = referenced_by_path.get(candidate.path)
        if not matches:
            orphans.append((path, size))
        elif any(match.component_match(candidate) for match in matches):
            continue
        elif mode == PrefixMismatchMode.DELETE:
            orphans.append((path, size))
        else:
            conflicts.update(_conflicts(matches, candidate))
    return orphans, conflicts


def _conflicts(referenced: list["_FileURI"], candidate: "_FileURI") -> Iterator[tuple[str, str]]:
    for match in referenced:
        if not _component_match(match.scheme, candidate.scheme):
            yield (match.scheme, candidate.scheme)
        if not _component_match(match.authority, candidate.authority):
            yield (match.authority, candidate.authority)


_REPEATED_SLASH = re.compile(r"/+")


def _file_uri(location: str, equal_schemes: dict[str, str], equal_authorities: dict[str, str]) -> "_FileURI":
    """Split a location into scheme, authority and path, canonicalizing each component.

    Equivalent schemes and authorities are collapsed onto their canonical value, and runs of
    slashes in the path are collapsed so ``file:///a///b`` matches ``file:///a/b``.
    """
    scheme, authority, path = _split_location(location)
    return _FileURI(
        scheme=equal_schemes.get(scheme, scheme),
        authority=equal_authorities.get(authority, authority),
        path=_REPEATED_SLASH.sub("/", path),
    )


def _uri_path(location: str) -> str:
    return _REPEATED_SLASH.sub("/", _split_location(location)[2])


def _split_location(location: str) -> tuple[str, str, str]:
    """Split a location into its scheme, authority and path, treating local paths as scheme-less."""
    # On Windows a drive letter parses as a URI scheme, so local paths are left whole.
    if _is_local_path(location):
        return "", "", location
    parsed = urlparse(location)
    if not parsed.scheme:
        return "", "", location
    return parsed.scheme, parsed.netloc, parsed.path
