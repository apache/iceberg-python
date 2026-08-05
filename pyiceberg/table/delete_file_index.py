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

from bisect import bisect_left

from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import INITIAL_SEQUENCE_NUMBER, POSITIONAL_DELETE_SCHEMA, DataFile, ManifestEntry
from pyiceberg.typedef import Record

PATH_FIELD_ID = 2147483546


class PositionDeletes:
    """Collects position delete files and indexes them by sequence number."""

    __slots__ = ("_buffer", "_seqs", "_files")

    def __init__(self) -> None:
        self._buffer: list[tuple[DataFile, int]] | None = []
        self._seqs: list[int] = []
        self._files: list[tuple[DataFile, int]] = []

    def add(self, delete_file: DataFile, seq_num: int) -> None:
        if self._buffer is None:
            raise ValueError("Cannot add files after indexing")
        self._buffer.append((delete_file, seq_num))

    def _ensure_indexed(self) -> None:
        if self._buffer is not None:
            self._files = sorted(self._buffer, key=lambda file: file[1])
            self._seqs = [seq for _, seq in self._files]
            self._buffer = None

    def filter_by_seq(self, seq: int) -> list[DataFile]:
        self._ensure_indexed()
        if not self._files:
            return []
        start_idx = bisect_left(self._seqs, seq)
        return [delete_file for delete_file, _ in self._files[start_idx:]]

    def filter_by_seq_paired(self, seq: int) -> list[tuple[DataFile, int]]:
        self._ensure_indexed()
        if not self._files:
            return []
        start_idx = bisect_left(self._seqs, seq)
        return self._files[start_idx:]

    def referenced_delete_files(self) -> list[DataFile]:
        self._ensure_indexed()
        return [data_file for data_file, _ in self._files]


def _has_path_bounds(delete_file: DataFile) -> bool:
    lower = delete_file.lower_bounds
    upper = delete_file.upper_bounds
    if not lower or not upper:
        return False

    return PATH_FIELD_ID in lower and PATH_FIELD_ID in upper


def _applies_to_data_file(delete_file: DataFile, data_file: DataFile) -> bool:
    if not _has_path_bounds(delete_file):
        return True

    evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_file.file_path))
    return evaluator.eval(delete_file)


def _referenced_data_file_path(delete_file: DataFile) -> str | None:
    """Return the path, if the path bounds evaluate to the same location."""
    lower_bounds = delete_file.lower_bounds
    upper_bounds = delete_file.upper_bounds

    if not lower_bounds or not upper_bounds:
        return None

    lower = lower_bounds.get(PATH_FIELD_ID)
    upper = upper_bounds.get(PATH_FIELD_ID)

    if lower and upper and lower == upper:
        try:
            return lower.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            pass

    return None


def _partition_key(spec_id: int, partition: Record | None) -> tuple[int, Record]:
    if partition:
        return spec_id, partition
    return spec_id, Record()  # unpartitioned handling


class DeleteFileIndex:
    """Indexes position delete files by partition and by exact data file path."""

    def __init__(self) -> None:
        self._by_partition: dict[tuple[int, Record], PositionDeletes] = {}
        self._by_path: dict[str, PositionDeletes] = {}

    def is_empty(self) -> bool:
        return not self._by_partition and not self._by_path

    def add_delete_file(self, manifest_entry: ManifestEntry, partition_key: Record | None = None) -> None:
        delete_file = manifest_entry.data_file
        seq = manifest_entry.sequence_number or INITIAL_SEQUENCE_NUMBER
        target_path = _referenced_data_file_path(delete_file)

        if target_path:
            deletes = self._by_path.setdefault(target_path, PositionDeletes())
            deletes.add(delete_file, seq)
        else:
            key = _partition_key(delete_file.spec_id or 0, partition_key)
            deletes = self._by_partition.setdefault(key, PositionDeletes())
            deletes.add(delete_file, seq)

    def for_data_file(self, seq_num: int, data_file: DataFile, partition_key: Record | None = None) -> set[DataFile]:
        """Find all delete files that apply to a given data file.

        Applies Iceberg spec sequence number gating:
        - Position deletes: apply when delete.seq >= data.seq
        - Equality deletes: apply when delete.seq > data.seq (strictly greater)

        This distinction is mandated by the Iceberg spec (§5.5.2): an equality
        delete written in the same snapshot as the data file does NOT apply to
        that data file (it targets only previously-committed data).
        """
        if self.is_empty():
            return set()

        from pyiceberg.manifest import DataFileContent

        deletes: set[DataFile] = set()
        spec_id = data_file.spec_id or 0

        key = _partition_key(spec_id, partition_key)
        partition_deletes = self._by_partition.get(key)
        if partition_deletes:
            for delete_file, delete_seq in partition_deletes.filter_by_seq_paired(seq_num):
                if _applies_to_data_file(delete_file, data_file):
                    # Equality deletes require strictly greater sequence number
                    if delete_file.content == DataFileContent.EQUALITY_DELETES and delete_seq <= seq_num:
                        continue
                    deletes.add(delete_file)

        path_deletes = self._by_path.get(data_file.file_path)
        if path_deletes:
            for delete_file, delete_seq in path_deletes.filter_by_seq_paired(seq_num):
                # Equality deletes require strictly greater sequence number
                if delete_file.content == DataFileContent.EQUALITY_DELETES and delete_seq <= seq_num:
                    continue
                deletes.add(delete_file)

        return deletes

    def referenced_delete_files(self) -> list[DataFile]:
        data_files: list[DataFile] = []

        for deletes in self._by_partition.values():
            data_files.extend(deletes.referenced_delete_files())

        for deletes in self._by_path.values():
            data_files.extend(deletes.referenced_delete_files())

        return data_files
