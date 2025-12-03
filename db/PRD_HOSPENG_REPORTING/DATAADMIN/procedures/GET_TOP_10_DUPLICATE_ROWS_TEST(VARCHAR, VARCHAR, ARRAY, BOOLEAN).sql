CREATE OR REPLACE PROCEDURE "GET_TOP_10_DUPLICATE_ROWS_TEST"("DB_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TARGET_TABLES" ARRAY, "EVALUATE_PK" BOOLEAN)
RETURNS TABLE ("DATABASE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_NAME" VARCHAR, "DUP_COUNT" NUMBER(38,0), "MTLN_CDC_LOAD_TIMESTAMP" TIMESTAMP_LTZ(9), "ROW_DATA" VARIANT)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    object_list CURSOR FOR SELECT "name" FROM temp_object_list;
    table_name VARCHAR;
    column_list VARCHAR;
    dynamic_sql VARCHAR;
    info_schema_path VARCHAR;
    schema_identifier VARCHAR;
    evaluate_pk_effective BOOLEAN;
    final_results RESULTSET;

BEGIN
    CREATE OR REPLACE TEMPORARY TABLE temp_object_list ("name" VARCHAR);
    
    -- ✅ Added DUP_COUNT to the temp table definition
    CREATE OR REPLACE TEMPORARY TABLE temp_duplicate_results (
        DATABASE_NAME VARCHAR,
        SCHEMA_NAME VARCHAR,
        TABLE_NAME VARCHAR,
        DUP_COUNT INTEGER,
        MTLN_CDC_LOAD_TIMESTAMP TIMESTAMP_LTZ,
        ROW_DATA VARIANT
    );

    evaluate_pk_effective := COALESCE(:EVALUATE_PK, TRUE);
    schema_identifier := :DB_NAME || ''.'' || :SCHEMA_NAME;

    SHOW TABLES IN SCHEMA IDENTIFIER(:schema_identifier);
    INSERT INTO temp_object_list ("name")
    SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW VIEWS IN SCHEMA IDENTIFIER(:schema_identifier);
    INSERT INTO temp_object_list ("name")
    SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    FOR rec IN object_list DO
        table_name := rec."name";

        IF (TARGET_TABLES IS NULL OR ARRAY_SIZE(TARGET_TABLES) = 0 OR ARRAY_CONTAINS(:table_name::VARIANT, TARGET_TABLES)) THEN

            info_schema_path := :DB_NAME || ''.INFORMATION_SCHEMA.COLUMNS'';

            SELECT
                LISTAGG(IFF(REGEXP_LIKE(column_name, ''^[a-zA-Z0-9_]+$''), column_name, ''"'' || column_name || ''"''), '', '')
            INTO column_list
            FROM IDENTIFIER(:info_schema_path)
            WHERE
                table_schema = :SCHEMA_NAME
                AND table_name = :table_name
                AND (:evaluate_pk_effective = TRUE OR NOT ENDSWITH(UPPER(column_name), ''_PK''));

            IF (column_list IS NOT NULL AND column_list != '''') THEN
                -- ✅ Modified to use QUALIFY to select one instance and get the count
                dynamic_sql := ''
                    INSERT INTO temp_duplicate_results (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DUP_COUNT, MTLN_CDC_LOAD_TIMESTAMP, ROW_DATA)
                    SELECT
                        '''''' || DB_NAME || '''''',
                        '''''' || SCHEMA_NAME || '''''',
                        '''''' || table_name || '''''',
                        COUNT(*) OVER (PARTITION BY '' || column_list || ''),
                        MTLN_CDC_LOAD_TIMESTAMP,
                        OBJECT_CONSTRUCT(*)
                    FROM '' || DB_NAME || ''.'' || SCHEMA_NAME || ''."'' || table_name || ''"
                    QUALIFY COUNT(*) OVER (PARTITION BY '' || column_list || '') > 1
                        AND ROW_NUMBER() OVER (PARTITION BY '' || column_list || '' ORDER BY MTLN_CDC_LOAD_TIMESTAMP DESC) = 1;
                '';

                EXECUTE IMMEDIATE dynamic_sql;
            END IF;
        END IF;
    END FOR;

    -- ✅ Updated final select to include DUP_COUNT
    final_results := (
        SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DUP_COUNT, MTLN_CDC_LOAD_TIMESTAMP, ROW_DATA
        FROM temp_duplicate_results
        ORDER BY MTLN_CDC_LOAD_TIMESTAMP DESC
    );

    RETURN TABLE(final_results);

END;
';