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
from typing import Callable, List, TypeVar

import pyarrow as pa
from pyarrow import Table as pyarrow_table
from pyarrow import compute as pc

from pyiceberg.expressions import (
    AlwaysFalse,
    BooleanExpression,
    EqualTo,
    In,
)

T = TypeVar("T")


def build_balanced_tree(operator_: Callable[[T, T], T], items: List[T]) -> T:
    """
    Recursively constructs a balanced binary tree of expressions using the provided binary operator.

    This function is a safer and more scalable alternative to:
        reduce(operator_, items)

    Using reduce creates a deeply nested, unbalanced tree (e.g., operator_(a, operator_(b, operator_(c, ...)))),
    which grows linearly with the number of items. This can lead to RecursionError exceptions in Python
    when the number of expressions is large (e.g., >1000).

    In contrast, this function builds a balanced binary tree with logarithmic depth (O(log n)),
    helping avoid recursion issues and ensuring that expression trees remain stable, predictable,
    and safe to traverse — especially in tools like PyIceberg that operate on large logical trees.

    Parameters:
        operator_ (Callable[[T, T], T]): A binary operator function (e.g., pyiceberg.expressions.Or, And).
        items (List[T]): A list of expression objects to combine.

    Returns:
        T: An expression object representing the balanced combination of all input expressions.

    Raises:
        ValueError: If the input list is empty.
    """
    if not items:
        raise ValueError("No expressions to combine")
    if len(items) == 1:
        return items[0]
    mid = len(items) // 2
    left = build_balanced_tree(operator_, items[:mid])
    right = build_balanced_tree(operator_, items[mid:])
    return operator_(left, right)


def create_match_filter(df: pyarrow_table, join_cols: list[str]) -> BooleanExpression:
    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])

    if len(join_cols) == 1:
        return In(join_cols[0], unique_keys[0].to_pylist())
    else:
        filters = [
            functools.reduce(operator.and_, [EqualTo(col, row[col]) for col in join_cols]) for row in unique_keys.to_pylist()
        ]

        return AlwaysFalse() if len(filters) == 0 else build_balanced_tree(operator.or_, filters)


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
    non_key_cols = all_columns - join_cols_set

    if has_duplicate_rows(target_table, join_cols):
        raise ValueError("Target table has duplicate rows, aborting upsert")

    if len(target_table) == 0:
        # When the target table is empty, there is nothing to update :)
        return source_table.schema.empty_table()

    diff_expr = functools.reduce(operator.or_, [pc.field(f"{col}-lhs") != pc.field(f"{col}-rhs") for col in non_key_cols])

    return (
        source_table
        # We already know that the schema is compatible, this is to fix large_ types
        .cast(target_table.schema)
        .join(target_table, keys=list(join_cols_set), join_type="inner", left_suffix="-lhs", right_suffix="-rhs")
        .filter(diff_expr)
        .drop_columns([f"{col}-rhs" for col in non_key_cols])
        .rename_columns({f"{col}-lhs" if col not in join_cols else col: col for col in source_table.column_names})
        # Finally cast to the original schema since it doesn't carry nullability:
        # https://github.com/apache/arrow/issues/45557
    ).cast(target_table.schema)
