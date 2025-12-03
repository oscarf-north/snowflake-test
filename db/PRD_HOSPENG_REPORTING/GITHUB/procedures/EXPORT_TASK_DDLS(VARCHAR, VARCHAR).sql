CREATE OR REPLACE PROCEDURE "EXPORT_TASK_DDLS"("P_DB_NAME" VARCHAR, "P_OUTPUT_SCHEMA" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_target_table  STRING;
    v_insert_sql    STRING;
BEGIN
    -- Fully qualified target table: "P_DB_NAME"."P_OUTPUT_SCHEMA"."EXPORT_DDLS_TASKS"
    v_target_table := ''"'' || P_DB_NAME || ''"."'' || P_OUTPUT_SCHEMA || ''"."EXPORT_DDLS_TASKS"'';

    -- 1) Ensure the output table exists (schema must already exist)
    EXECUTE IMMEDIATE
        ''CREATE TABLE IF NOT EXISTS '' || v_target_table || '' ('' ||
        ''  OBJECT_TYPE VARCHAR(16777216),'' ||
        ''  SCHEMA_NAME VARCHAR(16777216),'' ||
        ''  OBJECT_NAME VARCHAR(16777216),'' ||
        ''  DDL         VARCHAR(16777216)'' ||
        '')'';

    -- 2) Clean the table BEFORE we run SHOW
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || v_target_table;

    -- 3) Run SHOW TASKS in the same database (this is the result we want to scan)
    EXECUTE IMMEDIATE
        ''SHOW TASKS IN DATABASE "'' || P_DB_NAME || ''"'';

    -- 4) Build and execute the INSERT that scans the SHOW TASKS result
    v_insert_sql :=
        ''INSERT INTO '' || v_target_table || '' '' ||
        ''WITH tasks AS ('' ||
        ''  SELECT'' ||
        ''    "database_name" AS database_name,'' ||
        ''    "schema_name"   AS schema_name,'' ||
        ''    "name"          AS task_name '' ||
        ''  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))'' ||
        '') '' ||
        ''SELECT'' ||
        ''  ''''TASK''''      AS OBJECT_TYPE,'' ||
        ''  schema_name   AS SCHEMA_NAME,'' ||
        ''  task_name     AS OBJECT_NAME,'' ||
        ''  GET_DDL(''''TASK'''', ''''"'''' || database_name || ''''"."'''' || schema_name || ''''"."'''' || task_name || ''''"'''') AS DDL '' ||
        ''FROM tasks '' ||
        ''ORDER BY database_name, schema_name, task_name'';

    EXECUTE IMMEDIATE v_insert_sql;

    RETURN
        ''Exported TASK DDLs for database '' || P_DB_NAME ||
        '' into table '' || v_target_table;
END;
';