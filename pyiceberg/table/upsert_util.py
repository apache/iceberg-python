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
import functools
import operator

import pyarrow as pa
from pyarrow import Table as pyarrow_table
from pyarrow import compute as pc

from pyiceberg.expressions import (
    AlwaysFalse,
    BooleanExpression,
    EqualTo,
    In,
    Or,
)


def create_match_filter(df: pyarrow_table, join_cols: list[str]) -> BooleanExpression:
    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])

    if len(join_cols) == 1:
        return In(join_cols[0], unique_keys[0].to_pylist())
    else:
        filters = [
            functools.reduce(operator.and_, [EqualTo(col, row[col]) for col in join_cols]) for row in unique_keys.to_pylist()
        ]

        if len(filters) == 0:
            return AlwaysFalse()
        elif len(filters) == 1:
            return filters[0]
        else:
            return Or(*filters)


def has_duplicate_rows(df: pyarrow_table, join_cols: list[str]) -> bool:
    """Check for duplicate rows in a PyArrow table based on the join columns."""
    return len(df.select(join_cols).group_by(join_cols).aggregate([([], "count_all")]).filter(pc.field("count_all") > 1)) > 0


# How many values of a column are turned into Python objects at a time, when PyArrow cannot
# compare them itself
_PYTHON_COMPARISON_SLICE = 10_000


def _get_changed_struct_mask(source_column: pa.ChunkedArray, target_column: pa.ChunkedArray) -> pa.ChunkedArray:
    """Compare two struct columns field by field, which PyArrow can do even though it cannot compare the structs."""
    # `struct_field` carries the null of the struct into its fields, so the fields of two null
    # structs compare equal and only the struct itself decides for those rows
    changed = pc.not_equal(pc.is_null(source_column), pc.is_null(target_column))

    for field in source_column.type:
        changed = pc.or_(
            changed,
            _get_changed_mask(pc.struct_field(source_column, field.name), pc.struct_field(target_column, field.name)),
        )

    return changed


def _get_changed_mask(source_column: pa.ChunkedArray, target_column: pa.ChunkedArray) -> pa.ChunkedArray:
    """Return a boolean mask that flags the positions where the two columns differ, treating two nulls as equal."""
    try:
        differs = pc.not_equal(source_column, target_column)
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
        # PyArrow cannot compare columns with complex types
        # See: https://github.com/apache/arrow/issues/35785
        if pa.types.is_struct(source_column.type) and source_column.type == target_column.type:
            return _get_changed_struct_mask(source_column, target_column)

        # Two columns PyArrow refuses to compare may still hold the same values in another type:
        # a naive timestamp against a zoned one, or the string of a dataframe against the
        # large_string a scan reads. Comparing those in Python would call every row changed, on
        # every run, and would leave a struct out of the comparison by field above. The types have
        # to differ for this to make progress, the cast leaves them equal and the next round
        # settles it one way or the other
        if source_column.type != target_column.type:
            try:
                return _get_changed_mask(source_column.cast(target_column.type), target_column)
            except pa.ArrowException:
                # Whatever PyArrow makes of the cast, the comparison in Python below still holds
                pass

        # A list or a map is left to be compared in Python, value by value. A slice at a time,
        # so that the objects of a whole column are never held at once
        return pa.chunked_array(
            [
                [
                    source_val != target_val
                    for source_val, target_val in zip(
                        source_column.slice(offset, _PYTHON_COMPARISON_SLICE).to_pylist(),
                        target_column.slice(offset, _PYTHON_COMPARISON_SLICE).to_pylist(),
                        strict=True,
                    )
                ]
                for offset in range(0, len(source_column), _PYTHON_COMPARISON_SLICE)
            ]
            or [[]],
            type=pa.bool_(),
        )

    # `not_equal` is null as soon as either side is null, and a null differs from a value
    # but not from another null
    return pc.fill_null(differs, pc.not_equal(pc.is_null(source_column), pc.is_null(target_column)))


def get_rows_to_update(source_table: pa.Table, target_table: pa.Table, join_cols: list[str]) -> pa.Table:
    """
    Return a table with rows that need to be updated in the target table based on the join columns.

    The table is joined on the identifier columns, and then checked if there are any updated rows.
    Those are selected and everything is renamed correctly.
    """
    source_columns, target_columns = set(source_table.column_names), set(target_table.column_names)
    if source_columns != target_columns:
        raise ValueError(
            f"Source table's field names are not matching the target's field names, "
            f"missing: {sorted(target_columns - source_columns)}, "
            f"unexpected: {sorted(source_columns - target_columns)}"
        )

    # Kept in the order of the source rather than taken from a set difference, whose order
    # varies from one process to the next
    join_cols_set = set(join_cols)
    non_key_cols = [col for col in source_table.column_names if col not in join_cols_set]

    if has_duplicate_rows(target_table, join_cols):
        raise ValueError("Target table has duplicate rows, aborting upsert")

    if len(target_table) == 0:
        # When the target table is empty, there is nothing to update :)
        return source_table.schema.empty_table()

    SOURCE_INDEX_COLUMN_NAME = "__source_index"
    TARGET_INDEX_COLUMN_NAME = "__target_index"

    if SOURCE_INDEX_COLUMN_NAME in join_cols or TARGET_INDEX_COLUMN_NAME in join_cols:
        raise ValueError(
            f"{SOURCE_INDEX_COLUMN_NAME} and {TARGET_INDEX_COLUMN_NAME} are reserved for joining "
            f"DataFrames, and cannot be used as column names"
        ) from None

    # Step 1: Prepare source index with join keys and a marker index
    # Only the join columns are cast, so the width of the table does not weigh on the join
    # See: https://github.com/apache/arrow/issues/37542
    source_index = (
        source_table.select(join_cols)
        .cast(target_table.select(join_cols).schema)
        .append_column(SOURCE_INDEX_COLUMN_NAME, pa.array(range(len(source_table))))
    )

    # Step 2: Prepare target index with join keys and a marker
    target_index = target_table.select(join_cols).append_column(TARGET_INDEX_COLUMN_NAME, pa.array(range(len(target_table))))

    # Step 3: Perform an inner join to find which rows from source exist in target
    matching_indices = source_index.join(target_index, keys=join_cols, join_type="inner")

    if len(matching_indices) == 0:
        return source_table.schema.empty_table()

    source_indices = matching_indices[SOURCE_INDEX_COLUMN_NAME]
    target_indices = matching_indices[TARGET_INDEX_COLUMN_NAME]

    # Step 4: Compare the matched rows one column at a time. Comparing them cell by cell instead
    # would allocate a PyArrow scalar per cell, which does not fit in memory on a wide table.
    changed = pa.chunked_array([pa.repeat(False, len(matching_indices))])
    for col in non_key_cols:
        changed = pc.or_(
            changed,
            _get_changed_mask(source_table.column(col).take(source_indices), target_table.column(col).take(target_indices)),
        )
        # Once every matched row has changed, the columns that are left cannot add anything, and
        # asking is far cheaper than taking and comparing them
        if pc.all(changed).as_py():
            break

    # Step 5: Take rows from source table using the indices
    return source_table.take(source_indices.filter(changed))
