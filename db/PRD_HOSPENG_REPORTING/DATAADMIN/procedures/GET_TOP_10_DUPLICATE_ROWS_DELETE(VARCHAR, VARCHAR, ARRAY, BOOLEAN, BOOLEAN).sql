CREATE OR REPLACE PROCEDURE "GET_TOP_10_DUPLICATE_ROWS_DELETE"("DB_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TARGET_TABLES" ARRAY, "EVALUATE_PK" BOOLEAN, "PERFORM_DELETE" BOOLEAN)
RETURNS TABLE ("DATABASE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_NAME" VARCHAR, "DUP_COUNT" NUMBER(38,0), "MTLN_CDC_LOAD_TIMESTAMP" TIMESTAMP_LTZ(9), "ROW_DATA" VARIANT)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    -- Cursors and variables
    object_list CURSOR FOR SELECT "name" FROM temp_object_list;
    table_name VARCHAR;
    column_list VARCHAR;
    dynamic_sql VARCHAR;
    delete_sql VARCHAR;
    info_schema_path VARCHAR;
    schema_identifier VARCHAR;
    evaluate_pk_effective BOOLEAN;
    perform_delete_effective BOOLEAN;
    final_results RESULTSET;

BEGIN
    -- Temporary tables
    CREATE OR REPLACE TEMPORARY TABLE temp_object_list ("name" VARCHAR);
    CREATE OR REPLACE TEMPORARY TABLE temp_duplicate_results (
        DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR,
        DUP_COUNT INTEGER, MTLN_CDC_LOAD_TIMESTAMP TIMESTAMP_LTZ, ROW_DATA VARIANT
    );

    -- Set effective parameters with safe defaults
    evaluate_pk_effective := COALESCE(:EVALUATE_PK, TRUE);
    perform_delete_effective := COALESCE(:PERFORM_DELETE, FALSE);
    schema_identifier := :DB_NAME || ''.'' || :SCHEMA_NAME;

    -- Get list of all tables and views
    SHOW TABLES IN SCHEMA IDENTIFIER(:schema_identifier);
    INSERT INTO temp_object_list ("name") SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SHOW VIEWS IN SCHEMA IDENTIFIER(:schema_identifier);
    INSERT INTO temp_object_list ("name") SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Loop through each object
    FOR rec IN object_list DO
        table_name := rec."name";

        IF (TARGET_TABLES IS NULL OR ARRAY_SIZE(TARGET_TABLES) = 0 OR ARRAY_CONTAINS(:table_name::VARIANT, TARGET_TABLES)) THEN
            info_schema_path := :DB_NAME || ''.INFORMATION_SCHEMA.COLUMNS'';

            -- Get the list of columns to check for duplicates
            SELECT LISTAGG(IFF(REGEXP_LIKE(column_name, ''^[a-zA-Z0-9_]+$''), column_name, ''"'' || column_name || ''"''), '', '')
            INTO column_list
            FROM IDENTIFIER(:info_schema_path)
            WHERE table_schema = :SCHEMA_NAME AND table_name = :table_name
              AND (:evaluate_pk_effective = TRUE OR NOT ENDSWITH(UPPER(column_name), ''_PK''));

            IF (column_list IS NOT NULL AND column_list != '''') THEN
                -- Step 1: Identify and store the single instance of each duplicate group
                -- ✅ CORRECTED THIS LINE by removing the erroneous "(...)"
                dynamic_sql := ''
                    INSERT INTO temp_duplicate_results
                    SELECT '''''' || DB_NAME || '''''', '''''' || SCHEMA_NAME || '''''', '''''' || table_name || '''''',
                           COUNT(*) OVER (PARTITION BY '' || column_list || ''),
                           MTLN_CDC_LOAD_TIMESTAMP, OBJECT_CONSTRUCT(*)
                    FROM '' || DB_NAME || ''.'' || SCHEMA_NAME || ''."'' || table_name || ''"
                    QUALIFY COUNT(*) OVER (PARTITION BY '' || column_list || '') > 1
                        AND ROW_NUMBER() OVER (PARTITION BY '' || column_list || '' ORDER BY MTLN_CDC_LOAD_TIMESTAMP DESC) = 1;'';
                EXECUTE IMMEDIATE dynamic_sql;

                -- Step 2: If PERFORM_DELETE is true, run the surgical delete
                IF (perform_delete_effective) THEN
                    delete_sql := ''
                        DELETE FROM '' || DB_NAME || ''.'' || SCHEMA_NAME || ''."'' || table_name || ''"
                        WHERE SYSTEM$HASH(*) IN (
                            SELECT row_hash FROM (
                                SELECT SYSTEM$HASH(*) as row_hash,
                                       ROW_NUMBER() OVER (PARTITION BY '' || column_list || '' ORDER BY MTLN_CDC_LOAD_TIMESTAMP DESC) as rn
                                FROM '' || DB_NAME || ''.'' || SCHEMA_NAME || ''."'' || table_name || ''"
                            )
                            WHERE rn > 1 -- Target all rows EXCEPT the one we want to keep (rn=1)
                        );'';
                    EXECUTE IMMEDIATE delete_sql;
                END IF;

            END IF;
        END IF;
    END FOR;

    -- Final result set of identified duplicates
    final_results := (
        SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DUP_COUNT, MTLN_CDC_LOAD_TIMESTAMP, ROW_DATA
        FROM temp_duplicate_results
        ORDER BY DUP_COUNT DESC, MTLN_CDC_LOAD_TIMESTAMP DESC
    );

    RETURN TABLE(final_results);
END;
';