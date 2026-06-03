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

"""Lazy-loading pagination utilities."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import SupportsIndex, TypeVar

T = TypeVar("T")


class PaginationList(list[T]):
    """A list that lazily fetches subsequent pages from a paginated API.

    The first page is pre-loaded on construction.  Subsequent pages are only
    fetched when the caller iterates past items already in memory.  Operations
    that require the complete result set — ``len()``, ``in``, slicing,
    ``repr()`` — trigger a full fetch of all remaining pages.

    Args:
        first_page: Items from the first API response.
        next_page_token: Pagination token returned with the first response,
            or ``None`` if no further pages exist.
        fetch_next_page: Callable that accepts a page token and returns a
            tuple of ``(items, next_page_token_or_None)``.
    """

    def __init__(
        self,
        first_page: list[T],
        next_page_token: str | None,
        fetch_next_page: Callable[[str], tuple[list[T], str | None]],
    ) -> None:
        super().__init__(first_page)
        self._next_page_token = next_page_token
        self._fetch_next_page = fetch_next_page

    # ------------------------------------------------------------------
    # Internal helpers — use list's own methods to avoid infinite loops.
    # ------------------------------------------------------------------

    def _fetch_all(self) -> None:
        """Fetch all remaining pages into the list."""
        while self._next_page_token:
            items, self._next_page_token = self._fetch_next_page(self._next_page_token)
            list.extend(self, items)

    def _fetch_through_index(self, idx: int) -> None:
        """Fetch pages until the list contains at least *idx + 1* items."""
        while list.__len__(self) <= idx and self._next_page_token:
            items, self._next_page_token = self._fetch_next_page(self._next_page_token)
            list.extend(self, items)

    # ------------------------------------------------------------------
    # Lazy iteration
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[T]:
        """Iterate lazily, fetching pages only as the caller advances."""
        idx = 0
        while True:
            if idx < list.__len__(self):
                yield list.__getitem__(self, idx)
                idx += 1
            elif self._next_page_token:
                items, self._next_page_token = self._fetch_next_page(self._next_page_token)
                list.extend(self, items)
            else:
                return

    # ------------------------------------------------------------------
    # Operations that require the complete result set
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        """Return the total number of items, fetching all pages first."""
        self._fetch_all()
        return list.__len__(self)

    def __contains__(self, item: object) -> bool:
        """Return True if item is present, fetching all pages first."""
        self._fetch_all()
        return list.__contains__(self, item)

    def __repr__(self) -> str:
        """Return string representation after fetching all pages."""
        self._fetch_all()
        return f"PaginationList({list.__repr__(self)})"

    def __eq__(self, other: object) -> bool:
        """Compare equality after fetching all pages."""
        self._fetch_all()
        return list.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        """Compare inequality after fetching all pages."""
        return not self.__eq__(other)

    # ------------------------------------------------------------------
    # Index / slice access
    # ------------------------------------------------------------------

    def __getitem__(self, idx: SupportsIndex | slice) -> T | list[T]:
        """Fetch pages as needed before returning the requested item(s)."""
        if isinstance(idx, slice):
            self._fetch_all()
        else:
            i = idx.__index__()
            if i < 0:
                self._fetch_all()
            else:
                self._fetch_through_index(i)
        return list.__getitem__(self, idx)
