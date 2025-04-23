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


def get_rows_to_update(source_table: pa.Table, target_table: pa.Table, join_cols: list[str]) -> pa.Table:
    """
    Return a table with rows that need to be updated in the target table based on the join columns.

    The table is joined on the identifier columns, and then checked if there are any updated rows.
    Those are selected and everything is renamed correctly.
    """
    join_cols_set = set(join_cols)

    if has_duplicate_rows(target_table, join_cols):
        raise ValueError("Target table has duplicate rows, aborting upsert")

    if len(target_table) == 0:
        # When the target table is empty, there is nothing to update :)
        return source_table.schema.empty_table()

    # When we are not able to compare (e.g. due to unsupported types),
    # fall back to selecting only rows in the source table that do NOT already exist in the target.
    # See: https://github.com/apache/arrow/issues/35785
    MARKER_COLUMN_NAME = "__from_target"
    INDEX_COLUMN_NAME = "__source_index"

    if MARKER_COLUMN_NAME in join_cols or INDEX_COLUMN_NAME in join_cols:
        raise ValueError(
            f"{MARKER_COLUMN_NAME} and {INDEX_COLUMN_NAME} are reserved for joining "
            f"DataFrames, and cannot be used as column names"
        ) from None

    # Step 1: Prepare source index with join keys and a marker index
    # Cast to target table schema, so we can do the join
    # See: https://github.com/apache/arrow/issues/37542
    source_index = (
        source_table.cast(target_table.schema)
        .select(join_cols_set)
        .append_column(INDEX_COLUMN_NAME, pa.array(range(len(source_table))))
    )

    # Step 2: Prepare target index with join keys and a marker
    target_index = target_table.select(join_cols_set).append_column(MARKER_COLUMN_NAME, pa.repeat(True, len(target_table)))

    # Step 3: Perform a left outer join to find which rows from source exist in target
    joined = source_index.join(target_index, keys=list(join_cols_set), join_type="left outer")

    # Step 4: Create indices for rows that do exist in the target i.e., where marker column is true after the join
    to_update_indices = joined.filter(pc.field(MARKER_COLUMN_NAME))[INDEX_COLUMN_NAME]

    # Step 5: Take rows from source table using the indices and cast to target schema
    return source_table.take(to_update_indices)
