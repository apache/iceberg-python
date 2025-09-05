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
from math import isnan
from typing import Any

import pyarrow as pa
from pyarrow import Table as pyarrow_table
from pyarrow import compute as pc

from pyiceberg.expressions import (
    AlwaysFalse,
    And,
    BooleanExpression,
    EqualTo,
    In,
    IsNaN,
    IsNull,
    Or,
)


def create_match_filter(df: pyarrow_table, join_cols: list[str]) -> BooleanExpression:
    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])
    filters = []

    if len(join_cols) == 1:
        column = join_cols[0]
        values = set(unique_keys[0].to_pylist())

        if None in values:
            filters.append(IsNull(column))
            values.remove(None)

        if nans := {v for v in values if isinstance(v, float) and isnan(v)}:
            filters.append(IsNaN(column))
            values -= nans

        filters.append(In(column, values))
    else:

        def equals(column: str, value: Any) -> BooleanExpression:
            if value is None:
                return IsNull(column)

            if isinstance(value, float) and isnan(value):
                return IsNaN(column)

            return EqualTo(column, value)

        filters = [And(*[equals(col, row[col]) for col in join_cols]) for row in unique_keys.to_pylist()]

    if len(filters) == 0:
        return AlwaysFalse()
    elif len(filters) == 1:
        return filters[0]
    else:
        return Or(*filters)


def has_duplicate_rows(df: pyarrow_table, join_cols: list[str]) -> bool:
    """Check for duplicate rows in a PyArrow table based on the join columns."""
    return len(df.select(join_cols).group_by(join_cols).aggregate([([], "count_all")]).filter(pc.field("count_all") > 1)) > 0


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

    # We need to compare non_key_cols in Python as PyArrow
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
    # PyArrow joins ignore null values, and we want null==null to hold, so we compute the join in Python.
    # This is equivalent to:
    # matching_indices = source_index.join(target_index, keys=list(join_cols_set), join_type="inner")
    source_indices = {tuple(row[col] for col in join_cols): row[SOURCE_INDEX_COLUMN_NAME] for row in source_index.to_pylist()}
    target_indices = {tuple(row[col] for col in join_cols): row[TARGET_INDEX_COLUMN_NAME] for row in target_index.to_pylist()}
    matching_indices = [(s, t) for key, s in source_indices.items() if (t := target_indices.get(key)) is not None]

    # Step 4: Compare all rows using Python
    to_update_indices = []
    for source_idx, target_idx in matching_indices:
        source_row = source_table.slice(source_idx, 1)
        target_row = target_table.slice(target_idx, 1)

        for key in non_key_cols:
            source_val = source_row.column(key)[0].as_py()
            target_val = target_row.column(key)[0].as_py()
            if source_val != target_val:
                to_update_indices.append(source_idx)
                break

    # Step 5: Take rows from source table using the indices and cast to target schema
    if to_update_indices:
        return source_table.take(to_update_indices)
    else:
        return source_table.schema.empty_table()
