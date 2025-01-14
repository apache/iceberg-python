
from datafusion import SessionContext

def get_table_column_list(connection: SessionContext, table_name: str) -> list:
    """
    This function retrieves the column names and their data types for the specified table.
    It returns a list of tuples where each tuple contains the column name and its data type.
    
    Args:
        connection: DataFusion SessionContext.
        table_name: The name of the table for which to retrieve column information.
    
    Returns:
        A list of tuples containing column names and their corresponding data types.
    """
    # DataFusion logic
    res = connection.sql(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """).collect()

    column_names = res[0][0].to_pylist()  # Extract the first column (column names)
    data_types = res[0][1].to_pylist()    # Extract the second column (data types)

    return list(zip(column_names, data_types))  # Combine into list of tuples

def dups_check_in_source(source_table: str, join_cols: list, connection: SessionContext) -> bool:
    """
    This function checks if there are duplicate rows in the source and target tables based on the join columns.
    It returns True if there are duplicate rows in either table, otherwise it returns False.
    """
    # Check for duplicates in the source table
    source_dup_sql = f"""
        SELECT {', '.join(join_cols)}, COUNT(*)
        FROM {source_table}
        GROUP BY {', '.join(join_cols)}
        HAVING COUNT(*) > 1
        LIMIT 1
    """
    source_dup_df = connection.sql(source_dup_sql).collect()
    source_dup_count = len(source_dup_df)

    return source_dup_count > 0 

def do_join_columns_exist(source_col_list: set, target_col_list: set, join_cols: list) -> bool:
 
    """
    This function checks if the join columns exist in both the source and target tables.
    It returns a dictionary indicating which join columns are missing from each table.
    """
    missing_columns = {
        'source': [],
        'target': []
    }

    for col in join_cols:
        if col not in source_col_list:
            missing_columns['source'].append(col)
        if col not in target_col_list:
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
