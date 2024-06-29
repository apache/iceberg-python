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

from pyiceberg.table.sorting import NullOrder, SortDirection, SortField


class PyArrowSortOptions:
    sort_direction: str
    null_order: str

    def __init__(self, sort_direction: str = "ascending", null_order: str = "at_end"):
        if sort_direction not in ("ascending", "descending"):
            raise ValueError('Sort Direction should be one of ["ascending","descending"]')
        if null_order not in ("at_start", "at_end"):
            raise ValueError('Sort Null Order should be one of ["at_start","at_end"]')

        self.sort_direction = sort_direction
        self.null_order = null_order


def convert_sort_field_to_pyarrow_sort_options(sort_field: SortField) -> PyArrowSortOptions:
    pyarrow_sort_direction = {SortDirection.ASC: "ascending", SortDirection.DESC: "descending"}
    pyarrow_null_ordering = {NullOrder.NULLS_LAST: "at_end", NullOrder.NULLS_FIRST: "at_start"}
    return PyArrowSortOptions(
        pyarrow_sort_direction.get(sort_field.direction, "ascending"),
        pyarrow_null_ordering.get(sort_field.null_order, "at_end"),
    )


def get_sort_indices_arrow_table(tbl: pa.Table, sort_seq: List[Tuple[str, PyArrowSortOptions]]) -> List[int]:
    import pyarrow as pa

    index_column_name = "__idx__pyarrow_sort__"
    cols = set(tbl.column_names)

    while index_column_name in cols:
        index_column_name = f"{index_column_name}_1"

    table: pa.Table = tbl.add_column(0, index_column_name, [list(range(len(tbl)))])

    for col_name, _ in sort_seq:
        if col_name not in cols:
            raise ValueError(
                f"{col_name} not found in arrow table. Expected one of [{','.join([col_name for col_name, _ in cols])}]"
            )

    for col_name, sort_options in sort_seq[::-1]:
        table = table.take(
            pa.compute.sort_indices(
                table, sort_keys=[(col_name, sort_options.sort_direction)], null_placement=sort_options.null_order
            )
        )

    return table[index_column_name].to_pylist()
