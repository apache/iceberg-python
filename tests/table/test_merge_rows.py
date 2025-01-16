## unit tests for merging rows

from datafusion import SessionContext
from pyiceberg.catalog.sql import SqlCatalog
import os
import shutil

ctx = SessionContext()

test_namespace = "test_ns"

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

def show_iceberg_table(table):
    import pyarrow.dataset as ds
    table_name = "target"
    if ctx.table_exist(table_name):
        ctx.deregister_table(table_name)
    ctx.register_dataset(table_name, ds.dataset(table.scan().to_arrow()))
    ctx.sql(f"SELECT * FROM {table_name} limit 5").show()

def show_df(df):
    import pyarrow.dataset as ds
    ctx.register_dataset("df", ds.dataset(df))
    ctx.sql("select * from df limit 10").show()

def gen_source_dataset(start_row: int, end_row: int, composite_key: bool, add_dup: bool):

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

    #print(sql)

    df = ctx.sql(sql).to_arrow_table()


    return df

def gen_target_iceberg_table(start_row: int, end_row: int, composite_key: bool):

    additional_columns = ", t.order_id + 1000 as order_line_id" if composite_key else ""

    df = ctx.sql(f"""
        with t as (SELECT unnest(range({start_row},{end_row+1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'A' as order_type
        from t
    """).to_arrow_table()

    catalog = get_sql_catalog(test_namespace)
    table = catalog.create_table(f"{test_namespace}.target", df.schema)

    table.append(df)

    return table

def test_merge_scenario_1_simple():

    """
        tests a single insert and update
    """

    table = gen_target_iceberg_table(1, 2, False)
    source_df = gen_source_dataset(2, 3, False, False)
    

    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 1
    rows_inserted_should_be = 1

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    #print(res)

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_scenario_2_10k_rows():

    """
        tests merging 10000 rows on a single key to simulate larger workload
    """

    table = gen_target_iceberg_table(1, 10000, False)
    source_df = gen_source_dataset(5001, 15000, False, False)
    

    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    rows_updated_should_be = 5000
    rows_inserted_should_be = 5000

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_scenario_3_composite_key():

    """
        tests merging 200 rows with a composite key
    """

    table = gen_target_iceberg_table(1, 200, True)
    source_df = gen_source_dataset(101, 300, True, False)
    

    res = table.merge_rows(df=source_df, join_cols=["order_id", "order_line_id"])

    #print(res)

    rows_updated_should_be = 100
    rows_inserted_should_be = 100

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_update_only():
    
    """
        tests explicit merge options to do update only
    """

    table = gen_target_iceberg_table(1, 1000, False)
    source_df = gen_source_dataset(501, 1500, False, False)
    
    merge_options = {'when_matched_update_all': True, 'when_not_matched_insert_all': False}
    res = table.merge_rows(df=source_df, join_cols=["order_id"], merge_options=merge_options)

    rows_updated_should_be = 500
    rows_inserted_should_be = 0

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_insert_only():
    """
        tests explicit merge options to do insert only
    """

    table = gen_target_iceberg_table(1, 1000, False)
    source_df = gen_source_dataset(501, 1500, False, False)
    
    merge_options = {'when_matched_update_all': False, 'when_not_matched_insert_all': True}
    res = table.merge_rows(df=source_df, join_cols=["order_id"], merge_options=merge_options)

    rows_updated_should_be = 0
    rows_inserted_should_be = 500

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_source_dups():

    """
        tests duplicate rows in source
    """

    table = gen_target_iceberg_table(1, 10, False)
    source_df = gen_source_dataset(5, 15, False, True)
    
    res = table.merge_rows(df=source_df, join_cols=["order_id"])

    error_msgs = res['error_msgs']

    #print(error_msgs)

    assert 'Duplicate rows found in source dataset' in error_msgs, f"error message should contain 'Duplicate rows found in source dataset', but got {error_msgs}"

    purge_warehouse()

def test_key_cols_misaligned():

    """
        tests join columns missing from one of the tables
    """

    #generate dummy target iceberg table
    df = ctx.sql("select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type").to_arrow_table()

    catalog = get_sql_catalog(test_namespace)
    table = catalog.create_table(f"{test_namespace}.target", df.schema)

    table.append(df)

    df_src = ctx.sql("select 1 as item_id, date '2021-05-01' as order_date, 'B' as order_type").to_arrow_table()

    res = table.merge_rows(df=df_src, join_cols=['order_id'])
    error_msgs = res['error_msgs']

    #print(res)

    assert 'Join columns missing in tables' in error_msgs, f"error message should contain 'Join columns missing in tables', but got {error_msgs}"

    purge_warehouse()

if __name__ == "__main__":
    test_merge_scenario_1_simple()
    test_merge_scenario_2_10k_rows()
    test_merge_scenario_3_composite_key()
    test_merge_update_only()
    test_merge_insert_only()
    test_merge_source_dups()
    test_key_cols_misaligned()
