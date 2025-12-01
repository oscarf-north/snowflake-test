CREATE OR REPLACE PROCEDURE "SP_DAG_DATASHARE_CRAFTABLE_LOAD_TABLES"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    -- Runtime metadata
    run_metadata VARIANT;
    parent_query_id VARCHAR;
    task_run_group_id VARCHAR;
    attempt_number NUMBER;
    session_id VARCHAR;
    task_name VARCHAR;

    -- Log variable
    LOGS VARCHAR DEFAULT ''Execution Log:\\n''; --to debug, you can add variables to the log and then return it. for example: LOGS := LOGS || ''target_table_name: '' || COALESCE(:target_table_name, ''NULL'') || ''\\n'';

    -- Constants (Screaming Snake Case)
    LOOKBACK_DAYS INT DEFAULT 30;
    DATE_FORMAT VARCHAR DEFAULT ''YYYY-MM-DD'';
    CRAFTABLE_TABLES_TARGET_SCHEMA VARCHAR DEFAULT ''DATASHARE_DEV'';
    
    -- List of EXISTING Stored Procedures to call
    TARGET_SPROC_LIST ARRAY DEFAULT ARRAY_CONSTRUCT (
        ''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_CHECK''
        ,''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_GRATUITY''
        ,''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_DISCOUNT''
        ,''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_FEE'',
        ''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_ITEM'',
        ''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_LABOR'',
        ''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_MODIFIER'',
        ''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_PAYMENT'',
        ''DATAADMIN.SP_LOAD_DATASHARE_CRAFTABLE_VOID''
    );
    
    -- Variables needed for the SP call arguments
    -- SIGNED_UP_LOCATIONS VARCHAR DEFAULT ''4,25,26,27,28,32'';
    SIGNED_UP_LOCATIONS VARCHAR DEFAULT ''4,32'';
    
    -- Date Variables (DATE type)
    yesterday_date DATE;             -- The end date for the load (DATE type).
    retention_boundary_date DATE;    -- The date 30 days before yesterday (DATE type).

    -- String Variables (VARCHAR type - for passing to the SPROCs)
    start_date_str VARCHAR;          -- The start date parameter as a string.
    end_date_str VARCHAR;            -- The end date parameter as a string.

    -- Variables for the loop and dynamic execution
    sproc_name VARCHAR;
    sql_command VARCHAR;
    target_table_name VARCHAR;

    -- Variables for Watermark Logic
    target_table_max_date DATE;
    calculated_start_date DATE;
    res RESULTSET;
    c1 CURSOR FOR SELECT NULL; -- Initialized with a dummy query
    

BEGIN
    CALL dataadmin.sp_get_dag_run_metadata(COALESCE(last_query_id(), ''NOT_FOUND'')) INTO :run_metadata;
    parent_query_id := :run_metadata:parent_query_id::VARCHAR;
    task_run_group_id := :run_metadata:graph_run_group_id::VARCHAR;
    attempt_number := :run_metadata:run_attempt_number::NUMBER;
    session_id := :run_metadata:session_id::VARCHAR;
    task_name := :run_metadata:task_name::VARCHAR;
    
    ----------------------------------------------------------
    -- todo: get signed up locations. for the moment we are using `SIGNED_UP_LOCATIONS` and writting to a dev schema
    ----------------------------------------------------------

    -- Calculate Date Values (DATE Type)
    yesterday_date := DATEADD(day, -1, CURRENT_DATE());
    retention_boundary_date := DATEADD(day, -:LOOKBACK_DAYS, :yesterday_date);
    -- Convert Dates to String Format (VARCHAR Type)
    end_date_str := TO_VARCHAR(:yesterday_date, :DATE_FORMAT);
    
    -- Loop through each target stored procedure
    FOR i IN 0 TO ARRAY_SIZE(TARGET_SPROC_LIST) - 1 DO
        sproc_name := TARGET_SPROC_LIST[i];

        BEGIN
            -- Determine target table name from sproc name.
            target_table_name := ''CRAFTABLE_'' || SPLIT_PART(:sproc_name, ''_'', -1);

            -- =======================================================
            -- START: WATERMARK CALCULATION LOGIC
            -- =======================================================
            
            -- Step A: Run the Dynamic SQL and fetch the result into a variable
            sql_command := ''SELECT MAX("Business Day") FROM '' || CRAFTABLE_TABLES_TARGET_SCHEMA || ''.'' || target_table_name;
            LOGS := LOGS || ''Executing: '' || :sql_command || ''\\n'';
            res := (EXECUTE IMMEDIATE :sql_command);
            OPEN c1 FOR res;
            FETCH c1 INTO target_table_max_date;
            CLOSE c1;

            -- Step B: Handle NULL (First Load)
            IF (target_table_max_date IS NULL) THEN
                target_table_max_date := ''1900-01-01''::DATE;
            END IF;

            -- Step C: Compare Max Date vs Retention Boundary
            IF (target_table_max_date > retention_boundary_date) THEN
                calculated_start_date := DATEADD(day, 1, target_table_max_date);
            ELSE
                calculated_start_date := retention_boundary_date;
            END IF;

            -- Step D: Convert to String for SPROC call
            start_date_str := TO_VARCHAR(calculated_start_date, DATE_FORMAT);

            -- =======================================================
            -- END: WATERMARK CALCULATION LOGIC
            -- =======================================================

            -- Step 3: Call Load Procedure
            -- We pass the string variables (start_date_str, end_date_str) to the SPROC.
            sql_command := ''CALL '' || sproc_name || 
                         ''(?, ?, ?)'';
            LOGS := LOGS || ''Executing: '' || :sql_command || '' with ('' || :start_date_str || '', '' || :end_date_str || '', '' || :SIGNED_UP_LOCATIONS || '')\\n'';
                        
            EXECUTE IMMEDIATE :sql_command USING (start_date_str, end_date_str, SIGNED_UP_LOCATIONS);


            -- Step 4: Data Retention Policy (DELETE)
            -- Uses the dynamically extracted target_table_name.
            sql_command := ''DELETE FROM '' || CRAFTABLE_TABLES_TARGET_SCHEMA || ''.'' || target_table_name || 
                        '' WHERE "Business Day" <= ?'';
            LOGS := LOGS || ''Executing: '' || :sql_command || '' with ('' || :retention_boundary_date || '')\\n'';
                        
            EXECUTE IMMEDIATE :sql_command USING (retention_boundary_date);
            
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO dataadmin.error_logs (
                    parent_query_id,
                    task_run_group_id,
                    attempt_number,
                    session_id,
                    task_name,
                    failed_query_id,
                    error_type_id,
                    severity,
                    sql_error_code,
                    sql_error_message,
                    sql_state,
                    details
                )
                SELECT
                    :parent_query_id,
                    :task_run_group_id,
                    :attempt_number,
                    :session_id,
                    :task_name,
                    COALESCE(last_query_id(), ''NOT_FOUND''), --it should return the query_id of the failed command in the BEGIN block.
                    2, --1 are hard fails and 2 soft fails
                    ''ERROR'',
                    :SQLCODE,
                    :SQLERRM,
                    :SQLSTATE,
                    OBJECT_CONSTRUCT(''LOGS'', :LOGS);
        END;

    END FOR;
    
    RETURN ''Graph Run Group ID: ''||:run_metadata:graph_run_group_id || '', Attempt: ''||:run_metadata:run_attempt_number ||    ''--- LOGS:'' || COALESCE(:LOGS, ''LOGS variable is NULL'');
END;
';