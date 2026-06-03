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

from collections.abc import Callable

from pyiceberg.utils.pagination import PaginationList


def _make_pages(pages: list[list[int]]) -> tuple[list[int], str | None, list[list[int]]]:
    """Build (first_page, first_token, remaining_pages) for tests."""
    tokens = [f"tok{i}" for i in range(len(pages))]
    first = pages[0]
    first_token = tokens[1] if len(pages) > 1 else None
    remaining = pages[1:]
    return first, first_token, remaining


def _make_fetch(remaining: list[list[int]], tokens: list[str | None]) -> Callable[[str], tuple[list[int], str | None]]:
    """Return a fetch callback that yields successive pages."""
    idx = 0

    def fetch(token: str) -> tuple[list[int], str | None]:
        nonlocal idx
        items = remaining[idx]
        next_tok = tokens[idx]
        idx += 1
        return items, next_tok

    return fetch


def _simple_pagination_list(
    pages: list[list[int]],
) -> tuple[PaginationList[int], list[int]]:
    """Build a PaginationList and return it with the full expected items."""
    all_items = [item for page in pages for item in page]
    next_tokens = [f"tok{i}" for i in range(1, len(pages))] + [None]

    call_count = 0

    def fetch(token: str) -> tuple[list[int], str | None]:
        nonlocal call_count
        # Pages are accessed in order starting from page index 1.
        page_idx = int(token.replace("tok", ""))
        call_count += 1
        return pages[page_idx], next_tokens[page_idx]

    first_token = "tok1" if len(pages) > 1 else None
    pl = PaginationList(pages[0], first_token, fetch)
    return pl, all_items


class TestPaginationListSinglePage:
    """Single-page list behaves like a plain list."""

    def test_iteration(self) -> None:
        pl, expected = _simple_pagination_list([[1, 2, 3]])
        assert list(pl) == expected

    def test_len(self) -> None:
        pl, expected = _simple_pagination_list([[1, 2, 3]])
        assert len(pl) == len(expected)

    def test_contains(self) -> None:
        pl, _ = _simple_pagination_list([[1, 2, 3]])
        assert 2 in pl
        assert 99 not in pl

    def test_getitem(self) -> None:
        pl, _ = _simple_pagination_list([[10, 20, 30]])
        assert pl[0] == 10
        assert pl[2] == 30
        assert pl[-1] == 30

    def test_slice(self) -> None:
        pl, _ = _simple_pagination_list([[1, 2, 3, 4]])
        assert pl[1:3] == [2, 3]

    def test_is_list_subclass(self) -> None:
        pl, _ = _simple_pagination_list([[1, 2]])
        assert isinstance(pl, list)


class TestPaginationListMultiPage:
    """Multi-page list lazily fetches subsequent pages."""

    def test_iteration_fetches_all(self) -> None:
        pl, expected = _simple_pagination_list([[1, 2], [3, 4], [5]])
        assert list(pl) == expected

    def test_partial_iteration_stops_early(self) -> None:
        """Iterating over just the first page should not trigger any fetch."""
        fetched = []

        def fetch(token: str) -> tuple[list[int], str | None]:
            fetched.append(token)
            return [3, 4], None

        pl: PaginationList[int] = PaginationList([1, 2], "tok1", fetch)

        # Only consume the first two items (already in first page).
        result = []
        for item in pl:
            result.append(item)
            if item == 2:
                break

        assert result == [1, 2]
        assert fetched == [], "No fetch should have occurred for first-page items"

    def test_iteration_triggers_fetch_when_needed(self) -> None:
        """Consuming past the first page triggers exactly one fetch per page."""
        fetched_tokens: list[str] = []

        def fetch(token: str) -> tuple[list[int], str | None]:
            fetched_tokens.append(token)
            if token == "tok1":
                return [3], "tok2"
            return [4, 5], None

        pl: PaginationList[int] = PaginationList([1, 2], "tok1", fetch)
        assert list(pl) == [1, 2, 3, 4, 5]
        assert fetched_tokens == ["tok1", "tok2"]

    def test_len_fetches_all(self) -> None:
        pl, expected = _simple_pagination_list([[1, 2], [3]])
        assert len(pl) == len(expected)

    def test_contains_fetches_all(self) -> None:
        pl, _ = _simple_pagination_list([[1, 2], [3, 4]])
        assert 4 in pl
        assert 99 not in pl

    def test_getitem_fetches_lazily(self) -> None:
        fetched: list[str] = []

        def fetch(token: str) -> tuple[list[int], str | None]:
            fetched.append(token)
            if token == "tok1":
                return [3, 4], "tok2"
            return [5], None

        pl: PaginationList[int] = PaginationList([1, 2], "tok1", fetch)

        # First page: no fetch needed.
        assert pl[0] == 1
        assert fetched == []

        # Index 2 is in the second page: one fetch.
        assert pl[2] == 3
        assert fetched == ["tok1"]

        # Index 4 is in the third page.
        assert pl[4] == 5
        assert fetched == ["tok1", "tok2"]

    def test_negative_index_fetches_all(self) -> None:
        pl, _ = _simple_pagination_list([[1, 2], [3, 4, 5]])
        assert pl[-1] == 5

    def test_slice_fetches_all(self) -> None:
        pl, _ = _simple_pagination_list([[1, 2], [3, 4]])
        assert pl[1:3] == [2, 3]

    def test_repr_fetches_all(self) -> None:
        pl, _ = _simple_pagination_list([[1], [2]])
        r = repr(pl)
        assert "1" in r and "2" in r

    def test_empty_first_page(self) -> None:
        pl, expected = _simple_pagination_list([[], [1, 2]])
        assert list(pl) == expected

    def test_equality_with_plain_list(self) -> None:
        pl, expected = _simple_pagination_list([[1, 2], [3]])
        assert pl == expected


class TestPaginationListPerformance:
    """Verify that PaginationList does not eagerly fetch all pages for len()
    on a single-page list, and that multi-page len() fetches all pages exactly once."""

    def test_len_single_page_makes_no_extra_fetches(self) -> None:
        """len() on a single-page PaginationList should not call the fetch callback."""
        fetched: list[str] = []

        def fetch(token: str) -> tuple[list[int], str | None]:
            fetched.append(token)
            return [], None

        pl: PaginationList[int] = PaginationList([1, 2, 3], None, fetch)
        assert len(pl) == 3
        assert fetched == [], "No fetch should occur for a single-page list"

    def test_len_multi_page_fetches_all_pages_once(self) -> None:
        """len() on a multi-page PaginationList fetches remaining pages exactly once."""
        fetch_count = 0

        def fetch(token: str) -> tuple[list[int], str | None]:
            nonlocal fetch_count
            fetch_count += 1
            if token == "p2":
                return [3, 4], "p3"
            return [5], None

        pl: PaginationList[int] = PaginationList([1, 2], "p2", fetch)
        assert len(pl) == 5
        assert fetch_count == 2, "Should fetch pages 2 and 3 exactly once each"

        # Second len() call must not trigger further fetches
        fetch_count = 0
        assert len(pl) == 5
        assert fetch_count == 0, "Second len() should use cached data"
