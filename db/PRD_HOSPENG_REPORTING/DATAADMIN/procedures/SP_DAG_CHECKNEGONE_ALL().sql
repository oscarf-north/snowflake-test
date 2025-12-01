CREATE OR REPLACE PROCEDURE "SP_DAG_CHECKNEGONE_ALL"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
     -- Runtime metadata
    run_metadata VARIANT;
    --error handling vars
    failed_query_id VARCHAR;
    details_json VARIANT;
    failure_count NUMBER;
    error_message_summary VARCHAR;
BEGIN
    CALL dataadmin.sp_get_dag_run_metadata(COALESCE(last_query_id(), ''NOT_FOUND'')) INTO :run_metadata;

    --TODO add query tags in future sprints

    CALL dataadmin.SP_CHECKNEGONE_ALL();
    failed_query_id := COALESCE(last_query_id(), ''NOT_FOUND'');

    -- 3. Analyze the results from the last query (the CALL statement)
    -- We create a temporary table of only the failures.
    CREATE OR REPLACE TEMPORARY TABLE failed_dq_checks AS
    SELECT *
    FROM TABLE(RESULT_SCAN(:failed_query_id))
    WHERE COUNTVAL <> 1;

    -- 4. Count the number of failures
    SELECT COUNT(*) INTO :failure_count FROM failed_dq_checks;

    IF (:failure_count > 0) THEN

        error_message_summary := ''DQ Check Failed: '' || :failure_count || '' dimension table(s) have an incorrect count for the -1 record.'';

        details_json := (SELECT OBJECT_CONSTRUCT(
            ''query_output'', ARRAY_AGG(OBJECT_CONSTRUCT(*))
            )
            FROM failed_dq_checks);

        -- Insert a SINGLE row into the error log table
        INSERT INTO dataadmin.error_logs (
            parent_query_id,
            task_run_group_id,
            attempt_number,
            session_id,
            task_name,
            failed_query_id,
            error_type_id,
            severity,
            sql_error_message,
            details
        )
        SELECT 
            :run_metadata:parent_query_id::VARCHAR,
            :run_metadata:graph_run_group_id::VARCHAR,
            :run_metadata:run_attempt_number::NUMBER,
            :run_metadata:session_id::VARCHAR,
            :run_metadata:task_name::VARCHAR,
            :failed_query_id,
            3, -- Data Quality Issue
            ''WARN'',
            :error_message_summary,
            :details_json
        ;

        RETURN ''Data quality issues found and logged: '' || :failure_count || '' tables failed.'';
    ELSE
        RETURN ''Data quality check passed. No issues found.'';
    END IF;

END;
';