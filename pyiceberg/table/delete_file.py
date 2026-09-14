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

from collections.abc import Iterable, Iterator, MutableSet
from dataclasses import dataclass
from typing import Any

from pyiceberg.manifest import DataFile


@dataclass(frozen=True, slots=True)
class DeleteFileKey:
    """Identity of a delete file, including its referenced content range."""

    file_path: str
    content_offset: int | None
    content_size_in_bytes: int | None

    @classmethod
    def from_file(cls, delete_file: DataFile) -> DeleteFileKey:
        """Create a key from a delete file."""
        return cls(
            file_path=delete_file.file_path,
            content_offset=delete_file.content_offset,
            content_size_in_bytes=delete_file.content_size_in_bytes,
        )


class DeleteFileSet(MutableSet[DataFile]):
    """Set-like delete-file collection keyed by location and content range."""

    _files: dict[DeleteFileKey, DataFile]

    def __init__(self, delete_files: Iterable[DataFile] = ()) -> None:
        self._files = {}
        for delete_file in delete_files:
            self.add(delete_file)

    def __contains__(self, delete_file: object) -> bool:
        """Return whether the delete file is present."""
        return isinstance(delete_file, DataFile) and DeleteFileKey.from_file(delete_file) in self._files

    def __iter__(self) -> Iterator[DataFile]:
        """Return an iterator over delete files."""
        return iter(self._files.values())

    def __len__(self) -> int:
        """Return the number of delete files."""
        return len(self._files)

    def add(self, delete_file: DataFile) -> None:
        self._files.setdefault(DeleteFileKey.from_file(delete_file), delete_file)

    def discard(self, delete_file: DataFile) -> None:
        self._files.pop(DeleteFileKey.from_file(delete_file), None)

    def update(self, delete_files: Iterable[DataFile]) -> None:
        for delete_file in delete_files:
            self.add(delete_file)

    def __repr__(self) -> str:
        """Return a string representation of the delete file set."""
        return f"{type(self).__name__}({list(self)!r})"

    def __eq__(self, other: Any) -> bool:
        """Compare delete file sets by delete file identity."""
        if isinstance(other, DeleteFileSet):
            return self._files.keys() == other._files.keys()

        if not isinstance(other, Iterable):
            return False

        other_keys: set[DeleteFileKey] = set()
        other_count = 0
        for delete_file in other:
            if not isinstance(delete_file, DataFile):
                return False
            other_keys.add(DeleteFileKey.from_file(delete_file))
            other_count += 1

        return len(other_keys) == other_count and set(self._files) == other_keys
