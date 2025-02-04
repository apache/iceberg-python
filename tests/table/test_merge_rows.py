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

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.catalog import Table as pyiceberg_table
import os
import shutil
import pyarrow as pa
from datetime import datetime

_TEST_NAMESPACE = "test_ns"

try:
    from datafusion import SessionContext
except ModuleNotFoundError as e:
    raise ModuleNotFoundError("For merge_rows, DataFusion needs to be installed") from e

def get_test_warehouse_path():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    return f"{curr_dir}/warehouse"

def get_sql_catalog(namespace: str) -> SqlCatalog:
    warehouse_path = get_test_warehouse_path()
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    catalog.create_namespace(namespace=namespace)
    return catalog

def purge_warehouse():
    warehouse_path = get_test_warehouse_path()

    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

def show_iceberg_table(table, ctx: SessionContext):
    import pyarrow.dataset as ds
    table_name = "target"
    if ctx.table_exist(table_name):
        ctx.deregister_table(table_name)
    ctx.register_dataset(table_name, ds.dataset(table.scan().to_arrow()))
    ctx.sql(f"SELECT * FROM {table_name} limit 5").show()

def show_df(df, ctx: SessionContext):
    import pyarrow.dataset as ds
    ctx.register_dataset("df", ds.dataset(df))
    ctx.sql("select * from df limit 10").show()

def gen_source_dataset(start_row: int, end_row: int, composite_key: bool, add_dup: bool, ctx: SessionContext):

    additional_columns = ", t.order_id + 1000 as order_line_id" if composite_key else ""

    dup_row = f"""
        UNION ALL
        (
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'B' as order_type
        from t
        limit 1
        )
    """ if add_dup else ""


    sql = f"""
        with t as (SELECT unnest(range({start_row},{end_row+1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'B' as order_type
        from t
        {dup_row}
    """

    df = ctx.sql(sql).to_arrow_table()

    return df

def gen_target_iceberg_table(start_row: int, end_row: int, composite_key: bool, ctx: SessionContext):

    additional_columns = ", t.order_id + 1000 as order_line_id" if composite_key else ""

    df = ctx.sql(f"""
        with t as (SELECT unnest(range({start_row},{end_row+1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'A' as order_type
        from t
    """).to_arrow_table()

    catalog = get_sql_catalog(_TEST_NAMESPACE)
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    return table

def test_merge_scenario_single_ins_upd():

    """
        tests a single insert and update
    """

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 2, False, ctx)
    source_df = gen_source_dataset(2, 3, False, False, ctx)
    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"
    purge_warehouse()

def test_merge_scenario_skip_upd_row():

    """
        tests a single insert and update; skips a row that does not need to be updated
    """

    ctx = SessionContext()

    df = ctx.sql(f"""
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """).to_arrow_table()

    catalog = get_sql_catalog(_TEST_NAMESPACE)
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    source_df = ctx.sql(f"""
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'B' as order_type  
        union all 
        select 3 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """).to_arrow_table()

    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_scenario_date_as_key():

    """
        tests a single insert and update; primary key is a date column
    """

    ctx = SessionContext()

    df = ctx.sql(f"""
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'A' as order_type
    """).to_arrow_table()

    catalog = get_sql_catalog(_TEST_NAMESPACE)
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    source_df = ctx.sql(f"""
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'B' as order_type  
        union all 
        select date '2021-01-03' as order_date, 'A' as order_type
    """).to_arrow_table()

    res = table.merge_rows(df=source_df, join_cols=["order_date"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_scenario_string_as_key():

    """
        tests a single insert and update; primary key is a string column
    """

    ctx = SessionContext()

    df = ctx.sql(f"""
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'A' as order_type
    """).to_arrow_table()

    catalog = get_sql_catalog(_TEST_NAMESPACE)
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    source_df = ctx.sql(f"""
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'B' as order_type  
        union all 
        select 'ghi' as order_id, 'A' as order_type
    """).to_arrow_table()

    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_scenario_10k_rows():

    """
        tests merging 10000 rows on a single key to simulate larger workload
    """

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 10000, False, ctx)
    source_df = gen_source_dataset(5001, 15000, False, False, ctx)
    
    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 5000
    rows_inserted_should_be = 5000

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_scenario_composite_key():

    """
        tests merging 200 rows with a composite key
    """

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 200, True, ctx)
    source_df = gen_source_dataset(101, 300, True, False, ctx)
    

    res = table.merge_rows(df=source_df, join_cols=["order_id", "order_line_id"])

    rows_updated_should_be = 100
    rows_inserted_should_be = 100

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_update_only():
    
    """
        tests explicit merge options to do update only
    """

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 1000, False, ctx)
    source_df = gen_source_dataset(501, 1500, False, False, ctx)
    
    res = table.merge_rows(df=source_df, join_cols=["order_id"], when_matched_update_all=True, when_not_matched_insert_all=False)

    rows_updated_should_be = 500
    rows_inserted_should_be = 0

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_insert_only():
    """
        tests explicit merge options to do insert only
    """

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 1000, False, ctx)
    source_df = gen_source_dataset(501, 1500, False, False, ctx)
    
    res = table.merge_rows(df=source_df, join_cols=["order_id"], when_matched_update_all=False, when_not_matched_insert_all=True)

    rows_updated_should_be = 0
    rows_inserted_should_be = 500

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    purge_warehouse()

def test_merge_source_dups():

    """
        tests duplicate rows in source
    """

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 10, False, ctx)
    source_df = gen_source_dataset(5, 15, False, True, ctx)
    
    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    error_msgs = res['error_msgs']

    assert 'Duplicate rows found in source dataset' in error_msgs, f"error message should contain 'Duplicate rows found in source dataset', but got {error_msgs}"

    purge_warehouse()

def test_key_cols_misaligned():

    """
        tests join columns missing from one of the tables
    """

    ctx = SessionContext()

    #generate dummy target iceberg table
    df = ctx.sql("select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type").to_arrow_table()

    catalog = get_sql_catalog(_TEST_NAMESPACE)
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    df_src = ctx.sql("select 1 as item_id, date '2021-05-01' as order_date, 'B' as order_type").to_arrow_table()

    res = table.merge_rows(df=df_src, join_cols=['order_id'])
    error_msgs = res['error_msgs']

    assert 'Join columns missing in tables' in error_msgs, f"error message should contain 'Join columns missing in tables', but got {error_msgs}"

    purge_warehouse()

test_merge_scenario_single_ins_upd()
test_merge_scenario_skip_upd_row()
test_merge_scenario_date_as_key()
test_merge_scenario_string_as_key()
test_merge_scenario_10k_rows()
test_merge_scenario_composite_key()
test_merge_update_only()
test_merge_insert_only()
test_merge_source_dups()
test_key_cols_misaligned()
