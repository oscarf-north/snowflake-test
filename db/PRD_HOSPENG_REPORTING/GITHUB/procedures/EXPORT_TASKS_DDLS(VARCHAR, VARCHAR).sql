CREATE OR REPLACE PROCEDURE "EXPORT_TASKS_DDLS"("DB" VARCHAR, "TARGET_SCHEMA" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session

def main(session: Session, DB: str, TARGET_SCHEMA: str) -> str:
    if not TARGET_SCHEMA or TARGET_SCHEMA.strip() == "":
        return "Error: TARGET_SCHEMA parameter is required and cannot be empty."

    # 1) Ensure output table exists
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.export_ddls_tasks (
            object_type STRING,
            schema_name STRING,
            object_name STRING,
            ddl         STRING
        )
    """
    session.sql(create_table_sql).collect()

    # 2) Truncate output table
    session.sql(f"TRUNCATE TABLE {TARGET_SCHEMA}.export_ddls_tasks").collect()

    # 3) SHOW TASKS in the given database and capture results
    #    This avoids relying on {DB}.information_schema.tasks
    show_sql = f''SHOW TASKS IN DATABASE "{DB}"''
    session.sql(show_sql).collect()

    # Use RESULT_SCAN on the LAST_QUERY_ID() from SHOW TASKS
    tasks_df = session.sql("""
        SELECT "name", "schema_name"
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    """)
    tasks = tasks_df.collect()

    for row in tasks:
        task_name = row["name"]
        schema    = row["schema_name"]

        # Fully qualified task name for GET_DDL
        # Example: "MYDB"."MYSCHEMA"."MY_TASK"
        full_name   = f''"{DB}"."{schema}"."{task_name}"''
        object_name = task_name

        try:
            # 4) Get DDL via GET_DDL(''TASK'', full_name)
            ddl_df   = session.sql("SELECT GET_DDL(''TASK'', ?)", params=[full_name])
            ddl_rows = ddl_df.collect()
            ddl_text = ddl_rows[0][0] if ddl_rows else None

            # 5) Insert DDL into output table
            session.sql(
                f"""
                INSERT INTO {TARGET_SCHEMA}.export_ddls_tasks
                    (object_type, schema_name, object_name, ddl)
                VALUES (''TASK'', ?, ?, ?)
                """,
                params=[schema, object_name, ddl_text]
            ).collect()

        except Exception as e:
            # 6) Insert error into output table
            err_msg = "ERROR: " + str(e)
            session.sql(
                f"""
                INSERT INTO {TARGET_SCHEMA}.export_ddls_tasks
                    (object_type, schema_name, object_name, ddl)
                VALUES (''TASK'', ?, ?, ?)
                """,
                params=[schema, object_name, err_msg]
            ).collect()

    return "Task DDL export completed."
';