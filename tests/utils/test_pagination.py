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

import pytest

from pyiceberg.typedef import PaginationList


def _simple_pagination_list(
    pages: list[list[int]],
) -> tuple[PaginationList[int], list[int]]:
    all_items = [item for page in pages for item in page]
    next_tokens = [f"tok{i}" for i in range(1, len(pages))] + [None]

    call_count = 0

    def fetch(token: str) -> tuple[list[int], str | None]:
        nonlocal call_count
        page_idx = int(token.replace("tok", ""))
        call_count += 1
        return pages[page_idx], next_tokens[page_idx]

    first_token = "tok1" if len(pages) > 1 else None
    pl = PaginationList(pages[0], first_token, fetch)
    return pl, all_items


# ---------------------------------------------------------------------------
# Single-page: behaves like a plain list
# ---------------------------------------------------------------------------


def test_single_page_iteration() -> None:
    pl, expected = _simple_pagination_list([[1, 2, 3]])
    assert list(pl) == expected


def test_single_page_len() -> None:
    pl, expected = _simple_pagination_list([[1, 2, 3]])
    assert len(pl) == len(expected)


def test_single_page_contains() -> None:
    pl, _ = _simple_pagination_list([[1, 2, 3]])
    assert 2 in pl
    assert 99 not in pl


def test_single_page_getitem() -> None:
    pl, _ = _simple_pagination_list([[10, 20, 30]])
    assert pl[0] == 10
    assert pl[2] == 30
    assert pl[-1] == 30


def test_single_page_slice() -> None:
    pl, _ = _simple_pagination_list([[1, 2, 3, 4]])
    assert pl[1:3] == [2, 3]


def test_single_page_is_list_subclass() -> None:
    pl, _ = _simple_pagination_list([[1, 2]])
    assert isinstance(pl, list)


def test_single_page_count() -> None:
    pl, _ = _simple_pagination_list([[1, 2, 2, 3]])
    assert pl.count(2) == 2


def test_single_page_index() -> None:
    pl, _ = _simple_pagination_list([[10, 20, 30]])
    assert pl.index(30) == 2


def test_single_page_reversed() -> None:
    pl, _ = _simple_pagination_list([[1, 2, 3]])
    assert list(reversed(pl)) == [3, 2, 1]


def test_single_page_copy() -> None:
    pl, expected = _simple_pagination_list([[1, 2, 3]])
    copied = pl.copy()
    assert copied == expected
    assert type(copied) is list


def test_single_page_add() -> None:
    pl, _ = _simple_pagination_list([[1, 2]])
    assert pl + [3] == [1, 2, 3]


def test_single_page_radd() -> None:
    pl, _ = _simple_pagination_list([[2, 3]])
    assert [1] + pl == [1, 2, 3]


def test_single_page_mul() -> None:
    pl, _ = _simple_pagination_list([[1, 2]])
    assert pl * 2 == [1, 2, 1, 2]


def test_single_page_rmul() -> None:
    pl, _ = _simple_pagination_list([[1, 2]])
    assert 2 * pl == [1, 2, 1, 2]


# ---------------------------------------------------------------------------
# Multi-page: lazily fetches subsequent pages
# ---------------------------------------------------------------------------


def test_multi_page_iteration_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4], [5]])
    assert list(pl) == expected


def test_multi_page_partial_iteration_stops_early() -> None:
    fetched = []

    def fetch(token: str) -> tuple[list[int], str | None]:
        fetched.append(token)
        return [3, 4], None

    pl: PaginationList[int] = PaginationList([1, 2], "tok1", fetch)

    result = []
    for item in pl:
        result.append(item)
        if item == 2:
            break

    assert result == [1, 2]
    assert fetched == [], "No fetch should have occurred for first-page items"


def test_multi_page_iteration_triggers_fetch_when_needed() -> None:
    fetched_tokens: list[str] = []

    def fetch(token: str) -> tuple[list[int], str | None]:
        fetched_tokens.append(token)
        if token == "tok1":
            return [3], "tok2"
        return [4, 5], None

    pl: PaginationList[int] = PaginationList([1, 2], "tok1", fetch)
    assert list(pl) == [1, 2, 3, 4, 5]
    assert fetched_tokens == ["tok1", "tok2"]


def test_multi_page_len_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3]])
    assert len(pl) == len(expected)


def test_multi_page_contains_fetches_all() -> None:
    pl, _ = _simple_pagination_list([[1, 2], [3, 4]])
    assert 4 in pl
    assert 99 not in pl


def test_multi_page_getitem_fetches_lazily() -> None:
    fetched: list[str] = []

    def fetch(token: str) -> tuple[list[int], str | None]:
        fetched.append(token)
        if token == "tok1":
            return [3, 4], "tok2"
        return [5], None

    pl: PaginationList[int] = PaginationList([1, 2], "tok1", fetch)

    assert pl[0] == 1
    assert fetched == []

    assert pl[2] == 3
    assert fetched == ["tok1"]

    assert pl[4] == 5
    assert fetched == ["tok1", "tok2"]


def test_multi_page_negative_index_fetches_all() -> None:
    pl, _ = _simple_pagination_list([[1, 2], [3, 4, 5]])
    assert pl[-1] == 5


def test_multi_page_slice_fetches_all() -> None:
    pl, _ = _simple_pagination_list([[1, 2], [3, 4]])
    assert pl[1:3] == [2, 3]


def test_multi_page_repr_fetches_all() -> None:
    pl, _ = _simple_pagination_list([[1], [2]])
    r = repr(pl)
    assert "1" in r and "2" in r


def test_multi_page_empty_first_page() -> None:
    pl, expected = _simple_pagination_list([[], [1, 2]])
    assert list(pl) == expected


def test_multi_page_equality_with_plain_list() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3]])
    assert pl == expected


def test_multi_page_count_fetches_all() -> None:
    pl, _ = _simple_pagination_list([[1, 2], [2, 3]])
    assert pl.count(2) == 2


def test_multi_page_index_fetches_all() -> None:
    pl, _ = _simple_pagination_list([[1, 2], [3, 4]])
    assert pl.index(4) == 3


def test_multi_page_index_raises_for_missing_value() -> None:
    pl, _ = _simple_pagination_list([[1, 2], [3, 4]])
    with pytest.raises(ValueError):
        pl.index(99)


def test_multi_page_reversed_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4]])
    assert list(reversed(pl)) == list(reversed(expected))


def test_multi_page_copy_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4]])
    copied = pl.copy()
    assert copied == expected
    assert type(copied) is list


def test_multi_page_add_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4]])
    assert pl + [5] == expected + [5]


def test_multi_page_radd_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4]])
    assert [0] + pl == [0] + expected


def test_multi_page_mul_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4]])
    assert pl * 2 == expected * 2


def test_multi_page_rmul_fetches_all() -> None:
    pl, expected = _simple_pagination_list([[1, 2], [3, 4]])
    assert 2 * pl == 2 * expected


# ---------------------------------------------------------------------------
# Performance: len() should not eagerly fetch all pages for a single-page list
# ---------------------------------------------------------------------------


def test_performance_len_single_page_makes_no_extra_fetches() -> None:
    fetched: list[str] = []

    def fetch(token: str) -> tuple[list[int], str | None]:
        fetched.append(token)
        return [], None

    pl: PaginationList[int] = PaginationList([1, 2, 3], None, fetch)
    assert len(pl) == 3
    assert fetched == [], "No fetch should occur for a single-page list"


def test_performance_len_multi_page_fetches_all_pages_once() -> None:
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

    fetch_count = 0
    assert len(pl) == 5
    assert fetch_count == 0, "Second len() should use cached data"
