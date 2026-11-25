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
"""A bounded in-memory cache for the bytes of immutable metadata files."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable


class ContentCache:
    """Cache file contents by location, bounded by total size and by entry age.

    Args:
        max_total_bytes: The maximum number of bytes to keep across all entries.
        max_content_length: Entries larger than this are not cached.
        expiration_interval_ms: Entries older than this are treated as absent.
        clock: Returns the current time in seconds, replaced in tests.
    """

    def __init__(
        self,
        max_total_bytes: int,
        max_content_length: int,
        expiration_interval_ms: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_total_bytes = max_total_bytes
        self.max_content_length = max_content_length
        self.expiration_interval_seconds = expiration_interval_ms / 1000
        self._clock = clock
        self._entries: OrderedDict[str, tuple[bytes, float]] = OrderedDict()
        self._total_bytes = 0

    def get(self, location: str) -> bytes | None:
        """Return the cached bytes for a location, or None when absent or expired."""
        entry = self._entries.get(location)
        if entry is None:
            return None

        data, inserted_at = entry
        if self._clock() - inserted_at >= self.expiration_interval_seconds:
            self._remove(location)
            return None

        self._entries.move_to_end(location)
        return data

    def put(self, location: str, data: bytes) -> None:
        """Cache the bytes for a location, evicting the least recently used entries when needed."""
        if len(data) > self.max_content_length or len(data) > self.max_total_bytes or self.max_content_length <= 0:
            return

        self._remove(location)
        self._purge_expired()
        while self._entries and self._total_bytes + len(data) > self.max_total_bytes:
            _, (evicted, _) = self._entries.popitem(last=False)
            self._total_bytes -= len(evicted)

        self._entries[location] = (data, self._clock())
        self._total_bytes += len(data)

    def clear(self) -> None:
        """Remove every entry."""
        self._entries.clear()
        self._total_bytes = 0

    def _remove(self, location: str) -> None:
        entry = self._entries.pop(location, None)
        if entry is not None:
            self._total_bytes -= len(entry[0])

    def _purge_expired(self) -> None:
        now = self._clock()
        for location in [
            location
            for location, (_, inserted_at) in self._entries.items()
            if now - inserted_at >= self.expiration_interval_seconds
        ]:
            self._remove(location)
