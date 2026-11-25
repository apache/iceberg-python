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

from pyiceberg.typedef import PaginationList


def _pagination_list(pages: list[list[int]], fetched: list[str] | None = None) -> PaginationList[int]:
    fetched_tokens = fetched if fetched is not None else []
    next_tokens = [f"page{i}" for i in range(1, len(pages))] + [None]

    def fetch(token: str) -> tuple[list[int], str | None]:
        fetched_tokens.append(token)
        page = int(token.removeprefix("page"))
        return pages[page], next_tokens[page]

    next_page_token = next_tokens[0] if len(pages) > 1 else None
    return PaginationList(pages[0], next_page_token, fetch)


def test_single_page_behaves_like_list() -> None:
    pagination = _pagination_list([[10, 20, 30]])

    assert isinstance(pagination, list)
    assert list(pagination) == [10, 20, 30]
    assert len(pagination) == 3
    assert pagination[0] == 10
    assert pagination[-1] == 30
    assert pagination[1:] == [20, 30]
    assert 20 in pagination
    assert 99 not in pagination
    assert pagination.count(20) == 1
    assert pagination.index(30) == 2
    assert list(reversed(pagination)) == [30, 20, 10]
    assert pagination.copy() == [10, 20, 30]
    assert pagination + [40] == [10, 20, 30, 40]
    assert [0] + pagination == [0, 10, 20, 30]
    assert pagination * 2 == [10, 20, 30, 10, 20, 30]
    assert 2 * pagination == [10, 20, 30, 10, 20, 30]
    assert pagination == [10, 20, 30]
    assert pagination != [10, 20]
    assert repr(pagination) == "PaginationList([10, 20, 30])"


def test_iteration_fetches_pages_lazily() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3], [4, 5]], fetched)

    assert list(pagination) == [1, 2, 3, 4, 5]
    assert fetched == ["page1", "page2"]


def test_iteration_stops_without_fetching_remaining_pages() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3, 4]], fetched)

    seen = []
    for item in pagination:
        seen.append(item)
        if item == 2:
            break

    assert seen == [1, 2]
    assert fetched == []


def test_index_fetches_only_up_to_the_requested_item() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3, 4], [5]], fetched)

    assert pagination[2] == 3
    assert fetched == ["page1"]


def test_negative_index_fetches_all_pages() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3, 4], [5]], fetched)

    assert pagination[-1] == 5
    assert fetched == ["page1", "page2"]


def test_len_fetches_all_pages() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3, 4], [5]], fetched)

    assert len(pagination) == 5
    assert fetched == ["page1", "page2"]


def test_contains_fetches_all_pages() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3, 4]], fetched)

    assert 4 in pagination
    assert fetched == ["page1"]


def test_slice_fetches_all_pages() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[1, 2], [3, 4], [5]], fetched)

    assert pagination[1:4] == [2, 3, 4]
    assert fetched == ["page1", "page2"]


def test_sort_and_reverse_fetch_all_pages() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[3, 1], [2]], fetched)

    pagination.sort()
    assert pagination == [1, 2, 3]
    assert fetched == ["page1"]

    pagination.reverse()
    assert pagination == [3, 2, 1]


def test_empty_first_page_fetches_on_iteration() -> None:
    fetched: list[str] = []
    pagination = _pagination_list([[], [1, 2]], fetched)

    assert list(pagination) == [1, 2]
    assert fetched == ["page1"]
