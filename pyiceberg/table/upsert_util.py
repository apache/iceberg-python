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


def _struct_values_differ(source_values: pa.ChunkedArray, target_values: pa.ChunkedArray) -> pa.ChunkedArray:
    """Compare two struct columns field by field, as PyArrow cannot compare the structs themselves."""
    # A null struct has null fields, so the struct itself decides those rows
    changed = pc.not_equal(pc.is_null(source_values), pc.is_null(target_values))
    for field in source_values.type:
        changed = pc.or_(
            changed,
            _values_differ(pc.struct_field(source_values, field.name), pc.struct_field(target_values, field.name)),
        )
    return changed


def _values_differ(
    source_values: pa.Array | pa.ChunkedArray, target_values: pa.Array | pa.ChunkedArray
) -> pa.Array | pa.ChunkedArray:
    """Return a mask of the positions where the values are not equal, treating two nulls as equal."""
    try:
        equal = pc.fill_null(pc.equal(source_values, target_values), False)
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid, pa.ArrowTypeError):
        # PyArrow has no comparison kernel for nested types
        if pa.types.is_struct(source_values.type) and source_values.type == target_values.type:
            return _struct_values_differ(source_values, target_values)

        # Different types: cast the source to the target type and compare again, e.g. a naive against
        # a zoned timestamp, or a struct of string against the struct of large_string a scan returns
        if source_values.type != target_values.type:
            return _values_differ(source_values.cast(target_values.type), target_values)

        source_list = source_values.to_pylist()
        target_list = target_values.to_pylist()
        return pa.array(
            [source_value != target_value for source_value, target_value in zip(source_list, target_list, strict=True)],
            type=pa.bool_(),
        )

    both_null = pc.and_(pc.is_null(source_values), pc.is_null(target_values))
    return pc.invert(pc.or_(equal, both_null))


def get_rows_to_update(source_table: pa.Table, target_table: pa.Table, join_cols: list[str]) -> pa.Table:
    """
    Return a table with rows that need to be updated in the target table based on the join columns.

    The table is joined on the identifier columns, and then checked if there are any updated rows.
    Those are selected and everything is renamed correctly.
    """
    all_columns = set(source_table.column_names)
    join_cols_set = set(join_cols)

    non_key_cols = list(all_columns - join_cols_set)

    if has_duplicate_rows(target_table, join_cols):
        raise ValueError("Target table has duplicate rows, aborting upsert")

    if len(target_table) == 0:
        # When the target table is empty, there is nothing to update :)
        return source_table.schema.empty_table()

    # We need to compare non_key_cols without a join as PyArrow
    # 1. Cannot do a join when non-join columns have complex types
    # 2. Cannot compare columns with complex types
    # See: https://github.com/apache/arrow/issues/35785
    SOURCE_INDEX_COLUMN_NAME = "__source_index"
    TARGET_INDEX_COLUMN_NAME = "__target_index"

    if SOURCE_INDEX_COLUMN_NAME in join_cols or TARGET_INDEX_COLUMN_NAME in join_cols:
        raise ValueError(
            f"{SOURCE_INDEX_COLUMN_NAME} and {TARGET_INDEX_COLUMN_NAME} are reserved for joining "
            f"DataFrames, and cannot be used as column names"
        ) from None

    # Step 1: Prepare source index with join keys and a marker index
    # Cast to target table schema, so we can do the join
    # See: https://github.com/apache/arrow/issues/37542
    source_index = (
        source_table.cast(target_table.schema)
        .select(join_cols_set)
        .append_column(SOURCE_INDEX_COLUMN_NAME, pa.array(range(len(source_table))))
    )

    # Step 2: Prepare target index with join keys and a marker
    target_index = target_table.select(join_cols_set).append_column(TARGET_INDEX_COLUMN_NAME, pa.array(range(len(target_table))))

    # Step 3: Perform an inner join to find which rows from source exist in target
    matching_indices = source_index.join(target_index, keys=list(join_cols_set), join_type="inner")

    # Step 4: Compare the matched rows column by column
    source_indices = matching_indices[SOURCE_INDEX_COLUMN_NAME]
    target_indices = matching_indices[TARGET_INDEX_COLUMN_NAME]

    changed: pa.Array | pa.ChunkedArray | None = None
    for key in non_key_cols:
        column_changed = _values_differ(
            source_table.column(key).take(source_indices),
            target_table.column(key).take(target_indices),
        )
        changed = column_changed if changed is None else pc.or_(changed, column_changed)

    # Step 5: Take rows from source table using the indices and cast to target schema
    if changed is not None and pc.any(changed).as_py():
        return source_table.take(pc.filter(source_indices, changed))
    else:
        return source_table.schema.empty_table()
