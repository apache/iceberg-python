## unit test for merging rows

## todo
"""
    simplify this unit testing to reusable functions
    check with how the other unit tests are flagged and add accordlingly
    fix the warehouse path; its not creating it in this test module
    wonder if the pyiceberg team already has a warehouse folder to stash all these tests in?
    add a show_table function to visually see the new table
    add a function to nuke the warehouse folder to cleanup

    update these functions to all start with "test_"

    test_1: single update/insert
    test_2: scale to 1k updates/inserts on single key
    test_3: test update only
    test_4: test insert only
    test_5: test no update or insert
    test_6: composite key update/insert 100 rows
    test_7: composite key update/insert 1000 rows 

"""

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

def test_merge_scenario_1():

    """
        tests a single insert and update
    """

    table = gen_target_iceberg_table(1, 2, False)
    source_df = gen_source_dataset(2, 3, False, False)
    

    res = table.merge_rows(source_df, ["order_id"])

    assert res['rows_updated'] == 1, f"rows updated should be 1, but got {res['rows_updated']}"
    assert res['rows_inserted'] == 1, f"rows inserted should be 1, but got {res['rows_inserted']}"

    #print(res)

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_scenario_2():

    """
        tests merging 1000 rows on a single key
    """

    table = gen_target_iceberg_table(1, 1000, False)
    source_df = gen_source_dataset(501, 1500, False, False)
    

    res = table.merge_rows(source_df, ["order_id"])

    rows_updated_should_be = 500
    rows_inserted_should_be = 500

    assert res['rows_updated'] == rows_updated_should_be, f"rows updated should be {rows_updated_should_be}, but got {res['rows_updated']}"
    assert res['rows_inserted'] == rows_inserted_should_be, f"rows inserted should be {rows_inserted_should_be}, but got {res['rows_inserted']}"

    #show_iceberg_table(table)

    purge_warehouse()

def test_merge_scenario_3():

    """
        tests merging 200 rows with a composite key
    """

    table = gen_target_iceberg_table(1, 200, True)
    source_df = gen_source_dataset(101, 300, True, False)
    

    res = table.merge_rows(source_df, ["order_id", "order_line_id"])

    #print(res)

    rows_updated_should_be = 100
    rows_inserted_should_be = 100

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
    
    res = table.merge_rows(source_df, ["order_id"])

    error_msgs = res['error_msgs']

    #print(error_msgs)

    assert 'Duplicate rows found in source dataset' in error_msgs, f"error message should contain 'Duplicate rows found in source dataset', but got {error_msgs}"

    purge_warehouse()


def test_merge_update_only():
    print('blah')

def test_merge_insert_only():
    print('blah')

def test_key_cols_misaligned():
    print('blah')

if __name__ == "__main__":
    test_merge_scenario_1()
    test_merge_scenario_2()
    test_merge_scenario_3()
    test_merge_source_dups()
