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
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    In,
    LessThanOrEqual,
    Or,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema


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


def augment_filter_with_partition_ranges(
    matched_predicate: BooleanExpression,
    df: pyarrow_table,
    schema: Schema,
    spec: PartitionSpec,
) -> BooleanExpression:
    """Return *matched_predicate* AND'd with ``[min, max]`` predicates on partition source columns.

    Iceberg's ``inclusive_projection`` projects each range through the
    partition transform (``hours``, ``days``, ``months``, ``years``,
    ``identity``, ``truncate``) when planning the scan, so
    ``DataScan.plan_files`` can prune manifests and data files that
    don't overlap the source's value range. Without this augmentation,
    tables whose partition spec sources from columns NOT in
    ``join_cols`` (a common pattern for append-only event logs
    partitioned by time but keyed by composite IDs) fall through to a
    full table scan on every upsert because the row filter built from
    ``join_cols`` alone projects to ``AlwaysTrue`` against the
    partition spec.

    Bucket and other non-monotonic transforms return ``None`` from
    their ``project`` method for inequalities, so the augmentation is
    safe — it either prunes or contributes ``AlwaysTrue`` (no harm).

    A partition source column is skipped from augmentation when:

    - It isn't present on ``df`` (no source value to bound).
    - It is entirely null in ``df`` (no meaningful min/max).
    - It contains any null in ``df`` (preserving correctness: a
      ``GreaterThanOrEqual(col, non_null_min)`` predicate would
      exclude destination rows whose partition value is ``NULL``,
      potentially missing a key match. Without partition pruning
      those NULL-partition rows are scanned normally.)

    When ``min == max`` for a column, an ``EqualTo`` predicate is
    emitted instead of the range pair — tighter, and lets exact
    partition pruning fire.

    Args:
        matched_predicate: The row filter built from ``join_cols``.
        df: Source data frame whose values bound the augmentation.
        schema: Iceberg schema, used to resolve partition source ids
            to column names.
        spec: Active partition spec.

    Returns:
        The augmented predicate, or *matched_predicate* unchanged
        when no partition source column qualifies.
    """
    if spec.is_unpartitioned():
        return matched_predicate

    df_columns = set(df.column_names)
    augmentations: list[BooleanExpression] = []

    # Iterate distinct source columns rather than partition fields —
    # multiple partition fields can share a source column (e.g.
    # ``bucket(8, id), truncate(4, id)``) but we only need to add the
    # source-column range once; ``inclusive_projection`` projects
    # through each partition field independently.
    seen_source_ids: set[int] = set()
    for field in spec.fields:
        if field.source_id in seen_source_ids:
            continue
        seen_source_ids.add(field.source_id)

        col_name = schema.find_field(field.source_id).name
        if col_name not in df_columns:
            continue

        col = df[col_name]
        if col.null_count > 0:
            # Mixing null with a bounded predicate would exclude
            # destination rows whose partition value is null,
            # potentially missing key matches. Skip pruning rather
            # than risk a correctness regression.
            continue

        col_min = pc.min(col).as_py()
        col_max = pc.max(col).as_py()
        if col_min is None or col_max is None:
            # Defensive — ``null_count == 0`` should imply both bounds
            # are non-null, but pyarrow's min/max can still return None
            # on empty columns.
            continue

        if col_min == col_max:
            augmentations.append(EqualTo(col_name, col_min))
        else:
            augmentations.append(GreaterThanOrEqual(col_name, col_min))
            augmentations.append(LessThanOrEqual(col_name, col_max))

    if not augmentations:
        return matched_predicate

    return functools.reduce(And, [matched_predicate, *augmentations])


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
    matching_indices = source_index.join(target_index, keys=list(join_cols_set), join_type="inner")

    # Step 4: Compare all rows using Python
    to_update_indices = []
    for source_idx, target_idx in zip(
        matching_indices[SOURCE_INDEX_COLUMN_NAME].to_pylist(),
        matching_indices[TARGET_INDEX_COLUMN_NAME].to_pylist(),
        strict=True,
    ):
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
