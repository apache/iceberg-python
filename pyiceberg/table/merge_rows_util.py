
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
from pyiceberg import table as pyiceberg_table

from pyiceberg.expressions import (
    BooleanExpression,
    And,
    EqualTo,
    Or,
    In,
)

def get_filter_list(df: pyarrow_table, join_cols: list) -> BooleanExpression:

    unique_keys = df.select(join_cols).group_by(join_cols).aggregate([])

    pred = None

    if len(join_cols) == 1:
        pred = In(join_cols[0], unique_keys[0].to_pylist())
    else:
        pred = Or(*[
            And(*[
                EqualTo(col, row[col])
                for col in join_cols
            ])
            for row in unique_keys.to_pylist()
        ])

    return pred


def get_table_column_list_pa(df: pyarrow_table) -> list:
    return set(col for col in df.column_names)

def get_table_column_list_iceberg(table: pyiceberg_table) -> list:
    return set(col for col in table.schema().column_names)

def dups_check_in_source(df: pyarrow_table, join_cols: list) -> bool:
    """
    This function checks if there are duplicate rows in the source table based on the join columns.
    It returns True if there are duplicate rows in the source table, otherwise it returns False.
    """
    # Check for duplicates in the source table
    source_dup_count = len(
        df.select(join_cols)
            .group_by(join_cols)
            .aggregate([([], "count_all")])
            .filter(pc.field("count_all") > 1)
    )
    
    return source_dup_count > 0


def do_join_columns_exist(source_df: pyarrow_table, target_iceberg_table: pyiceberg_table, join_cols: list) -> bool:
 
    """
    This function checks if the join columns exist in both the source and target tables.
    It returns a dictionary indicating which join columns are missing from each table.
    """
    missing_columns = {
        'source': [],
        'target': []
    }

    for col in join_cols:
        if col not in source_df.column_names:
            missing_columns['source'].append(col)
        if col not in target_iceberg_table.schema().column_names:
            missing_columns['target'].append(col)

    return missing_columns

def get_rows_to_update_sql(source_table_name: str, target_table_name: str
                           , join_cols: list
                           , source_cols_list: set
                           , target_cols_list: set) -> str:
    """
    This function returns the rows that need to be updated in the target table based on the source table. 
    It compares the source and target tables based on the join columns and returns the rows that have different values in the non-join columns.
    """

    # Determine non-join columns that exist in both tables
    
    non_join_cols = source_cols_list.intersection(target_cols_list) - set(join_cols)


    sql = f"""
    SELECT {', '.join([f"src.{col}" for col in join_cols])},
         {', '.join([f"src.{col}" for col in non_join_cols])}
    FROM {source_table_name} as src
        INNER JOIN {target_table_name} as tgt
            ON {' AND '.join([f"src.{col} = tgt.{col}" for col in join_cols])}
    EXCEPT DISTINCT
    SELECT {', '.join([f"tgt.{col}" for col in join_cols])},
         {', '.join([f"tgt.{col}" for col in non_join_cols])}
    FROM {target_table_name} as tgt
    """
    return sql


def get_rows_to_insert_sql(source_table_name: str, target_table_name: str
                           , join_cols: list
                           , source_cols_list: set
                           , target_cols_list: set) -> str:
 

    # Determine non-join columns that exist in both tables
    insert_cols = source_cols_list.intersection(target_cols_list) - set(join_cols)

    # Build the SQL query
    sql = f"""
    SELECT 
        {', '.join([f"src.{col}" for col in join_cols])},
        {', '.join([f"src.{col}" for col in insert_cols])}  
    FROM 
        {source_table_name} as src
    LEFT JOIN 
        {target_table_name} as tgt
    ON 
        {' AND '.join([f"src.{col} = tgt.{col}" for col in join_cols])}
    WHERE 
        tgt.{join_cols[0]} IS NULL  
    """
    return sql
