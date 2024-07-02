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
# pylint:disable=redefined-outer-name
from typing import List, Tuple

import pyarrow as pa
import pytest

from pyiceberg.utils.arrow_sorting import PyArrowSortOptions, get_sort_indices_arrow_table


@pytest.fixture
def example_arrow_table_for_sort() -> pa.Table:
    return pa.table({
        "column1": [5, None, 3, 1, 1, None, 3],
        "column2": ["b", "a", None, "c", "c", "d", "m"],
        "column3": [10.5, None, 5.1, None, 2.5, 7.3, 3.3],
    })


@pytest.mark.parametrize(
    "sort_keys, expected",
    [
        (
            [
                ("column1", PyArrowSortOptions("ascending", "at_end")),
                ("column2", PyArrowSortOptions("ascending", "at_start")),
                ("column3", PyArrowSortOptions("descending", "at_end")),
            ],
            [4, 3, 2, 6, 0, 1, 5],
        )
    ],
)
def test_get_sort_indices_arrow_table(
    example_arrow_table_for_sort: pa.Table, sort_keys: List[Tuple[str, PyArrowSortOptions]], expected: List[int]
) -> None:
    sorted_indices = get_sort_indices_arrow_table(example_arrow_table_for_sort, sort_keys)
    assert sorted_indices == expected, "Table sort not in expected form"


@pytest.mark.parametrize("sort_keys, expected", [([("column1", PyArrowSortOptions())], [3, 4, 2, 6, 0, 1, 5])])
def test_stability_get_sort_indices_arrow_table(
    example_arrow_table_for_sort: pa.Table, sort_keys: List[Tuple[str, PyArrowSortOptions]], expected: pa.Table
) -> None:
    sorted_indices = get_sort_indices_arrow_table(example_arrow_table_for_sort, sort_keys)
    assert sorted_indices == expected, "Arrow Table sort is not stable"
