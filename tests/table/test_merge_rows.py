## unit test for merging rows

## todo
"""
    simplify this unit testing to reusable functions
    check with how the other unit tests are flagged and add accordlingly
    fix the warehouse path; its not creating it in this test module
    wonder if the pyiceberg team already has a warehouse folder to stash all these tests in?
    add a show_table function to visually see the new table
    add a function to nuke the warehouse folder to cleanup
"""

from datafusion import SessionContext
from pyiceberg.catalog.sql import SqlCatalog


ctx = SessionContext()


def merge_rows_test_1():

    warehouse_path = "./warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    
    namespace = "test_ns"
    catalog.create_namespace(namespace=namespace)

    df = ctx.sql("""
                select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
                union all
                select 2 as order_id, date '2021-01-02' as order_date, 'B' as order_type
            """).to_arrow_table()
    
    table = catalog.create_table(f"{namespace}.target", df.schema)

    table.append(df)

    ## generate new dataset to upsert
    df_new = ctx.sql("""
        select 1 as order_id, date '2021-01-05' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-02' as order_date, 'B' as order_type
        union all
        select 3 as order_id, date '2021-04-01' as order_date, 'C' as order_type
    """).to_arrow_table()
    

    res = table.merge_rows(df_new, ["order_id"])

    assert res['rows_updated'] == 1, "rows updated should be 1"
    assert res['rows_inserted'] == 1, "rows inserted should be 1"

    #print(res)

def merge_rows_test_2():

    warehouse_path = "./warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    
    namespace = "test_ns"
    catalog.create_namespace(namespace=namespace)

    df = ctx.sql("""
                select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
                union all
                select 2 as order_id, date '2021-01-02' as order_date, 'B' as order_type
            """).to_arrow_table()
    
    table = catalog.create_table(f"{namespace}.target", df.schema)

    table.append(df)

    ## generate new dataset to upsert
    df_new = ctx.sql("""
        select 1 as order_id, date '2021-01-05' as order_date, 'B' as order_type
        union all
        select 2 as order_id, date '2021-01-02' as order_date, 'C' as order_type
        union all
        select 3 as order_id, date '2021-04-01' as order_date, 'C' as order_type
        union all
        select 4 as order_id, date '2024-01-01' as order_date, 'D' as order_type
    """).to_arrow_table()
    

    res = table.merge_rows(df_new, ["order_id"])

    assert res['rows_updated'] == 2, "rows updated should be 2"
    assert res['rows_inserted'] == 2, "rows inserted should be 2"

    #print(res)

def merge_rows_test_3():

    warehouse_path = "./warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    
    namespace = "test_ns"
    catalog.create_namespace(namespace=namespace)

    df = ctx.sql("""
                select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
                union all
                select 2 as order_id, date '2021-01-02' as order_date, 'B' as order_type
            """).to_arrow_table()
    
    table = catalog.create_table(f"{namespace}.target", df.schema)

    table.append(df)

    ## generate new dataset to upsert
    df_new = ctx.sql("""
        select 1 as order_id, date '2021-01-05' as order_date, 'B' as order_type
        union all
        select 2 as order_id, date '2021-01-02' as order_date, 'C' as order_type
        union all
        select 3 as order_id, date '2021-04-01' as order_date, 'C' as order_type
        union all
        select 4 as order_id, date '2024-01-01' as order_date, 'D' as order_type
    """).to_arrow_table()
    

    res = table.merge_rows(df_new, ["order_id"], merge_options={'when_not_matched_insert_all': True})
    print(res)
    assert res['rows_updated'] == 0, "rows updated should be 0"
    assert res['rows_inserted'] == 2, "rows inserted should be 2"


def merge_rows_test_4():

    warehouse_path = "./warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    
    namespace = "test_ns"
    catalog.create_namespace(namespace=namespace)

    df = ctx.sql("""
                select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
                union all
                select 2 as order_id, date '2021-01-02' as order_date, 'B' as order_type
            """).to_arrow_table()
    
    table = catalog.create_table(f"{namespace}.target", df.schema)

    table.append(df)

    ## generate new dataset to upsert
    df_new = ctx.sql("""
        select 1 as order_id, date '2021-01-05' as order_date, 'B' as order_type
        union all
        select 2 as order_id, date '2021-01-02' as order_date, 'C' as order_type
        union all
        select 3 as order_id, date '2021-04-01' as order_date, 'C' as order_type
        union all
        select 4 as order_id, date '2024-01-01' as order_date, 'D' as order_type
    """).to_arrow_table()
    

    res = table.merge_rows(df_new, ["order_id"], merge_options={'when_matched_update_all': True})
    print(res)
    assert res['rows_updated'] == 2, "rows updated should be 2"
    assert res['rows_inserted'] == 0, "rows inserted should be 0"

def merge_rows_test_5():
    warehouse_path = "./warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    
    namespace = "test_ns"
    catalog.create_namespace(namespace=namespace)

    df = ctx.sql("""
                select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
                union all
                select 2 as order_id, date '2021-01-02' as order_date, 'B' as order_type
            """).to_arrow_table()
    
    table = catalog.create_table(f"{namespace}.target", df.schema)

    table.append(df)

    ## generate new dataset to upsert
    df_new = ctx.sql("""
        select 1 as order_id, date '2021-01-05' as order_date, 'B' as order_type
        union all
        select 2 as order_id, date '2021-01-02' as order_date, 'C' as order_type
        union all
        select 3 as order_id, date '2021-04-01' as order_date, 'C' as order_type
        union all
        select 4 as order_id, date '2024-01-01' as order_date, 'D' as order_type
    """).to_arrow_table()
    

    res = table.merge_rows(df_new, ["order_id"], merge_options={'no_options': True})
    print(res)
    #assert res['rows_updated'] == 2, "rows updated should be 2"
    #assert res['rows_inserted'] == 0, "rows inserted should be 0"

if __name__ == "__main__":
    merge_rows_test_1()
    merge_rows_test_2()
    merge_rows_test_3()
    merge_rows_test_4()
    merge_rows_test_5()
