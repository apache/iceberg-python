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
import pytest

_TEST_NAMESPACE = "test_ns"

try:
    from datafusion import SessionContext
except ModuleNotFoundError as e:
    raise ModuleNotFoundError("For upsert testing, DataFusion needs to be installed") from e

def get_test_warehouse_path():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    return f"{curr_dir}/warehouse"

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

def gen_target_iceberg_table(start_row: int, end_row: int, composite_key: bool, ctx: SessionContext, catalog: SqlCatalog, namespace: str):

    additional_columns = ", t.order_id + 1000 as order_line_id" if composite_key else ""

    df = ctx.sql(f"""
        with t as (SELECT unnest(range({start_row},{end_row+1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'A' as order_type
        from t
    """).to_arrow_table()

    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    return table

@pytest.fixture(scope="session")
def catalog_conn():
    warehouse_path = get_test_warehouse_path()
    os.makedirs(warehouse_path, exist_ok=True)
    print(warehouse_path)
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    catalog.create_namespace(namespace=_TEST_NAMESPACE)

    yield catalog

@pytest.mark.parametrize(
    "join_cols, src_start_row, src_end_row, target_start_row, target_end_row, when_matched_update_all, when_not_matched_insert_all, expected_updated, expected_inserted",
    [
        (["order_id"], 1, 2, 2, 3, True, True, 1, 1), # single row
        (["order_id"], 5001, 15000, 1, 10000, True, True, 5000, 5000), #10k rows
        (["order_id"], 501, 1500, 1, 1000, True, False, 500, 0), # update only
        (["order_id"], 501, 1500, 1, 1000, False, True, 0, 500), # insert only
    ]
)
def test_merge_rows(catalog_conn, join_cols, src_start_row, src_end_row, target_start_row, target_end_row
                    , when_matched_update_all, when_not_matched_insert_all, expected_updated, expected_inserted):

    ctx = SessionContext()

    catalog = catalog_conn

    source_df = gen_source_dataset(src_start_row, src_end_row, False, False, ctx)
    ice_table = gen_target_iceberg_table(target_start_row, target_end_row, False, ctx, catalog, _TEST_NAMESPACE)
    res = ice_table.upsert(df=source_df, join_cols=join_cols, when_matched_update_all=when_matched_update_all, when_not_matched_insert_all=when_not_matched_insert_all)

    assert res['rows_updated'] == expected_updated, f"rows updated should be {expected_updated}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == expected_inserted, f"rows inserted should be {expected_inserted}, but got {res['rows_inserted']}"

    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

@pytest.fixture(scope="session", autouse=True)
def cleanup():
    yield  # This allows other tests to run first
    purge_warehouse()  

def test_merge_scenario_skip_upd_row(catalog_conn):

    """
        tests a single insert and update; skips a row that does not need to be updated
    """

    ctx = SessionContext()

    df = ctx.sql(f"""
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """).to_arrow_table()

    catalog = catalog_conn
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    source_df = ctx.sql(f"""
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'B' as order_type  
        union all 
        select 3 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """).to_arrow_table()

    res = table.upsert(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

def test_merge_scenario_date_as_key(catalog_conn):

    """
        tests a single insert and update; primary key is a date column
    """

    ctx = SessionContext()

    df = ctx.sql(f"""
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'A' as order_type
    """).to_arrow_table()

    catalog = catalog_conn
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    source_df = ctx.sql(f"""
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'B' as order_type  
        union all 
        select date '2021-01-03' as order_date, 'A' as order_type
    """).to_arrow_table()

    res = table.upsert(df=source_df, join_cols=["order_date"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

def test_merge_scenario_string_as_key(catalog_conn):

    """
        tests a single insert and update; primary key is a string column
    """

    ctx = SessionContext()

    df = ctx.sql(f"""
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'A' as order_type
    """).to_arrow_table()

    catalog = catalog_conn
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    source_df = ctx.sql(f"""
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'B' as order_type  
        union all 
        select 'ghi' as order_id, 'A' as order_type
    """).to_arrow_table()

    res = table.upsert(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

def test_merge_scenario_composite_key(catalog_conn):

    """
        tests merging 200 rows with a composite key
    """

    ctx = SessionContext()

    catalog = catalog_conn
    table = gen_target_iceberg_table(1, 200, True, ctx, catalog, _TEST_NAMESPACE)
    source_df = gen_source_dataset(101, 300, True, False, ctx)
    

    res = table.upsert(df=source_df, join_cols=["order_id", "order_line_id"])

    rows_updated_should_be = 100
    rows_inserted_should_be = 100

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

def test_merge_source_dups(catalog_conn):

    """
        tests duplicate rows in source
    """

    ctx = SessionContext()


    catalog = catalog_conn
    table = gen_target_iceberg_table(1, 10, False, ctx, catalog, _TEST_NAMESPACE)
    source_df = gen_source_dataset(5, 15, False, True, ctx)
    
    with pytest.raises(Exception, match="Duplicate rows found in source dataset based on the key columns. No upsert executed"):
        table.upsert(df=source_df, join_cols=["order_id"])


    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

def test_key_cols_misaligned(catalog_conn):

    """
        tests join columns missing from one of the tables
    """

    ctx = SessionContext()

    df = ctx.sql("select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type").to_arrow_table()

    catalog = catalog_conn
    table = catalog.create_table(f"{_TEST_NAMESPACE}.target", df.schema)

    table.append(df)

    df_src = ctx.sql("select 1 as item_id, date '2021-05-01' as order_date, 'B' as order_type").to_arrow_table()

    with pytest.raises(Exception, match=r"""Field ".*" does not exist in schema"""):
        table.upsert(df=df_src, join_cols=['order_id'])

    catalog.drop_table(f"{_TEST_NAMESPACE}.target")

