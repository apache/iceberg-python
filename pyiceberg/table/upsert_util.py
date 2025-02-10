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
from pyarrow import Table as pyarrow_table
import pyarrow as pa
from pyarrow import compute as pc

from pyiceberg.expressions import (
    BooleanExpression,
    And,
    EqualTo,
    Or,
    In,
)

def create_match_filter(df: pyarrow_table, join_cols: list) -> BooleanExpression:

    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])

    if len(join_cols) == 1:
        return In(join_cols[0], unique_keys[0].to_pylist())
    else:
        return Or(*[
            And(*[
                EqualTo(col, row[col])
                for col in join_cols
            ])
            for row in unique_keys.to_pylist()
        ])

def has_duplicate_rows(df: pyarrow_table, join_cols: list) -> bool:
    """
    This function checks if there are duplicate rows in in a pyarrow table based on the join columns.
    """
   
    return len(
        df.select(join_cols)
            .group_by(join_cols)
            .aggregate([([], "count_all")])
            .filter(pc.field("count_all") > 1)
    ) > 0

def get_rows_to_update(source_table: pa.Table, target_table: pa.Table, join_cols: list) -> pa.Table:
    
    """
        This function takes the source_table, trims it down to rows that match in both source and target.
        It then does a scan for the non-key columns to see if any are mis-aligned before returning the final row set to update
    """
    
    all_columns = set(source_table.column_names)
    join_cols_set = set(join_cols)

    non_key_cols = list(all_columns - join_cols_set)

    match_expr = None

    for col in join_cols:
        target_values = target_table.column(col).to_pylist()
        expr = pc.field(col).isin(target_values)

        if match_expr is None:
            match_expr = expr
        else:
            match_expr = match_expr & expr
    
    matching_source_rows = source_table.filter(match_expr)

    rows_to_update = []

    for index in range(matching_source_rows.num_rows):
        
        source_row = matching_source_rows.slice(index, 1)

        target_filter = None

        for col in join_cols:
            target_value = source_row.column(col)[0].as_py()  
            if target_filter is None:
                target_filter = pc.field(col) == target_value
            else:
                target_filter = target_filter & (pc.field(col) == target_value)

        matching_target_row = target_table.filter(target_filter)

        if matching_target_row.num_rows > 0:
            needs_update = False

            for non_key_col in non_key_cols:
                source_value = source_row.column(non_key_col)[0].as_py()
                target_value = matching_target_row.column(non_key_col)[0].as_py()

                if source_value != target_value:
                    needs_update = True
                    break 

            if needs_update:
                rows_to_update.append(source_row)

    if rows_to_update:
        rows_to_update_table = pa.concat_tables(rows_to_update)
    else:
        rows_to_update_table = pa.Table.from_arrays([], names=source_table.column_names)

    common_columns = set(source_table.column_names).intersection(set(target_table.column_names))
    rows_to_update_table = rows_to_update_table.select(list(common_columns))

    return rows_to_update_table

def get_rows_to_insert(source_table: pa.Table, target_table: pa.Table, join_cols: list) -> pa.Table:
  
    source_filter_expr = None

    for col in join_cols:

        target_values = target_table.column(col).to_pylist()
        expr = pc.field(col).isin(target_values)

        if source_filter_expr is None:
            source_filter_expr = expr
        else:
            source_filter_expr = source_filter_expr & expr
    
    non_matching_expr = ~source_filter_expr

    source_columns = set(source_table.column_names)
    target_columns = set(target_table.column_names)
  
    common_columns = source_columns.intersection(target_columns)

    non_matching_rows = source_table.filter(non_matching_expr).select(common_columns)

    return non_matching_rows