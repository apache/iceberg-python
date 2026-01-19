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
from typing import Union

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
    """
    Create an Iceberg BooleanExpression filter that exactly matches rows based on join columns.

    For single-column keys, uses an efficient In() predicate.
    For composite keys, creates Or(And(...), And(...), ...) for exact row matching.
    This function should be used when exact matching is required (e.g., overwrite, insert filtering).
    """
    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])

    if len(unique_keys) == 0:
        return AlwaysFalse()

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


def create_coarse_match_filter(df: pyarrow_table, join_cols: list[str]) -> BooleanExpression:
    """
    Create a coarse Iceberg BooleanExpression filter for initial row scanning.

    For single-column keys, uses an efficient In() predicate (exact match).
    For composite keys, uses In() per column as a coarse filter (AND of In() predicates),
    which may return false positives but is much more efficient than exact matching.

    This function should only be used for initial scans where exact matching happens
    downstream (e.g., in get_rows_to_update() via the join operation).
    """
    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])

    if len(unique_keys) == 0:
        return AlwaysFalse()

    if len(join_cols) == 1:
        return In(join_cols[0], unique_keys[0].to_pylist())
    else:
        # For composite keys: use In() per column as a coarse filter
        # This is more efficient than creating Or(And(...), And(...), ...) for each row
        # May include false positives, but fine-grained matching happens downstream
        column_filters = []
        for col in join_cols:
            unique_values = pc.unique(unique_keys[col]).to_pylist()
            column_filters.append(In(col, unique_values))
        return functools.reduce(operator.and_, column_filters)


def has_duplicate_rows(df: pyarrow_table, join_cols: list[str]) -> bool:
    """Check for duplicate rows in a PyArrow table based on the join columns."""
    return len(df.select(join_cols).group_by(join_cols).aggregate([([], "count_all")]).filter(pc.field("count_all") > 1)) > 0


def _compare_columns_vectorized(
    source_col: Union[pa.Array, pa.ChunkedArray], target_col: Union[pa.Array, pa.ChunkedArray]
) -> pa.Array:
    """
    Vectorized comparison of two columns, returning a boolean array where True means values differ.

    Handles struct types recursively by comparing each nested field.
    Handles null values correctly: null != non-null is True, null == null is True (no update needed).
    """
    col_type = source_col.type

    if pa.types.is_struct(col_type):
        # PyArrow cannot directly compare struct columns, so we recursively compare each field
        diff_masks = []
        for i, field in enumerate(col_type):
            src_field = pc.struct_field(source_col, [i])
            tgt_field = pc.struct_field(target_col, [i])
            field_diff = _compare_columns_vectorized(src_field, tgt_field)
            diff_masks.append(field_diff)

        if not diff_masks:
            # Empty struct - no fields to compare, so no differences
            return pa.array([False] * len(source_col), type=pa.bool_())

        return functools.reduce(pc.or_, diff_masks)

    elif pa.types.is_list(col_type) or pa.types.is_large_list(col_type) or pa.types.is_map(col_type):
        # For list/map types, fall back to Python comparison as PyArrow doesn't support vectorized comparison
        # This is still faster than the original row-by-row approach since we batch the conversion
        source_py = source_col.to_pylist()
        target_py = target_col.to_pylist()
        return pa.array([s != t for s, t in zip(source_py, target_py, strict=True)], type=pa.bool_())

    else:
        # For primitive types, use vectorized not_equal
        # Handle nulls: not_equal returns null when comparing with null
        # We need: null vs non-null = different (True), null vs null = same (False)
        diff = pc.not_equal(source_col, target_col)
        source_null = pc.is_null(source_col)
        target_null = pc.is_null(target_col)

        # XOR of null masks: True if exactly one is null (meaning they differ)
        null_diff = pc.xor(source_null, target_null)

        # Combine: different if values differ OR exactly one is null
        # Fill null comparison results with False (both non-null but comparison returned null shouldn't happen,
        # but if it does, treat as no difference)
        diff_filled = pc.fill_null(diff, False)
        return pc.or_(diff_filled, null_diff)


def get_rows_to_update(source_table: pa.Table, target_table: pa.Table, join_cols: list[str]) -> pa.Table:
    """
    Return a table with rows that need to be updated in the target table based on the join columns.

    Uses vectorized PyArrow operations for efficient comparison, avoiding row-by-row Python loops.
    The table is joined on the identifier columns, and then checked if there are any updated rows.
    """
    all_columns = set(source_table.column_names)
    join_cols_set = set(join_cols)

    non_key_cols = list(all_columns - join_cols_set)

    if has_duplicate_rows(target_table, join_cols):
        raise ValueError("Target table has duplicate rows, aborting upsert")

    if len(target_table) == 0:
        # When the target table is empty, there is nothing to update
        return source_table.schema.empty_table()

    if len(non_key_cols) == 0:
        # No non-key columns to compare, all matched rows are "updates" but with no changes
        return source_table.schema.empty_table()

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

    if len(matching_indices) == 0:
        # No matching rows found
        return source_table.schema.empty_table()

    # Step 4: Take matched rows in batch (vectorized - single operation)
    source_indices = matching_indices[SOURCE_INDEX_COLUMN_NAME]
    target_indices = matching_indices[TARGET_INDEX_COLUMN_NAME]

    matched_source = source_table.take(source_indices)
    matched_target = target_table.take(target_indices)

    # Step 5: Vectorized comparison per column
    diff_masks = []
    for col in non_key_cols:
        source_col = matched_source.column(col)
        target_col = matched_target.column(col)
        col_diff = _compare_columns_vectorized(source_col, target_col)
        diff_masks.append(col_diff)

    # Step 6: Combine masks with OR (any column different = needs update)
    combined_mask = functools.reduce(pc.or_, diff_masks)

    # Step 7: Filter to get indices of rows that need updating
    to_update_indices = pc.filter(source_indices, combined_mask)

    if len(to_update_indices) > 0:
        return source_table.take(to_update_indices)
    else:
        return source_table.schema.empty_table()
