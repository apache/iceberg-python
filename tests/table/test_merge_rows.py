## unit test for merging rows
from datafusion import SessionContext
from pyiceberg.catalog.sql import SqlCatalog

ctx = SessionContext()


def merge_rows_test_1():

    print('**** START ***')

    warehouse_path = "./warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///:memory:",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    print(' ***** CATALOG CREATED *****')
    
    namespace = "test_ns"
    catalog.create_namespace_if_not_exists(namespace=namespace)
    print('namespace created')

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

    print(res)

if __name__ == "__main__":
    merge_rows_test_1()

