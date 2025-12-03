CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_CREATE_LOAD_SPROC"("SOURCE_SPROC_NAME" VARCHAR, "TARGET_TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
"""
This stored procedure dynamically generates de SQL string to create the 
loader stored procedures for craftable. (you have to copy the output and run it yourself).

It acts as a factory to create specific `SP_LOAD_DATASHARE_*` procedures.
The generated procedure will:
1. Execute a specified source stored procedure which returns a dataset.
2. Insert the results from the source procedure into a specified target table.

This automates the creation of boilerplate loading procedures.

Args:
    SOURCE_SPROC_NAME (VARCHAR): schema.sproc name of the source procedure
                                 that provides the data.
    TARGET_TABLE_NAME (VARCHAR): schema.table name of the target table
                                 where the data will be inserted.

Returns:
    VARCHAR: A string containing the SQL DDL for the newly created
             loader stored procedure, or an error message.
"""
import snowflake.snowpark as snowpark

TARGET_SCHEMA_FOR_LOAD_SPROC = ''DATAADMIN''

def main(session: snowpark.Session, source_sproc_name: str, target_table_name: str) -> str:
    # 1. Parse schema and table name from the fully qualified input
    try:
        # Assuming the format is DATABASE.SCHEMA.TABLE, we only need the last two parts
        schema_name, table_name = target_table_name.upper().split(''.'')[-2:]
    except ValueError:
        return "Error: TARGET_TABLE_NAME must be a fully qualified name (e.g., DB.SCHEMA.TABLE)."

    # 2. Derive the loader procedure name from the target table name
    loader_sproc_name = f"SP_LOAD_DATASHARE_{table_name}".replace(''-'', ''_'').replace('' '', ''_'') 

    # 3. Fetch the column list for the target table
    try:
        # Use SHOW COLUMNS to get the column names in their correct order and case
        columns_df = session.sql(f"SHOW COLUMNS IN TABLE {target_table_name}")
        
        # Collect the results and extract just the column names
        column_names = [row[''column_name''] for row in columns_df.collect()]
        
        # Format the list for inclusion in the SQL INSERT and SELECT statements.
        # Each column is double-quoted, and the list is indented for readability.
        column_list_str = '',\\n''.join([f''        "{col}"'' for col in column_names])
    except Exception as e:
        return f"Error: Could not retrieve columns for table ''{target_table_name}''. Details: {e}"

    # 4. Use a Python f-string as a template for the generated SQL.
    # This template now follows the direct AS BEGIN...END syntax.
    generated_sql = f"""CREATE OR REPLACE PROCEDURE {TARGET_SCHEMA_FOR_LOAD_SPROC}.{loader_sproc_name}(START_DATE VARCHAR, END_DATE VARCHAR, LOCATION_IDS VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    -- Execute the source procedure that returns the data
    CALL {source_sproc_name}(:START_DATE, :END_DATE, :LOCATION_IDS);

    -- Insert the results into the target table
    INSERT INTO {target_table_name} (
{column_list_str}
    )
    SELECT
{column_list_str}
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN ''Success: Data loaded into {target_table_name}.'';
END;"""

    return generated_sql
';