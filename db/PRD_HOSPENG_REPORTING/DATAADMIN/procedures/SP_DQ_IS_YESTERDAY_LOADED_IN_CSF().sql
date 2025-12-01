CREATE OR REPLACE PROCEDURE "SP_DQ_IS_YESTERDAY_LOADED_IN_CSF"()
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

    -- Procedure specific variables
    max_fiscal_date DATE;
    yesterday_date DATE;
    select_query_id VARCHAR;
BEGIN
    -- Get runtime metadata for logging and traceability
    -- This part is boilerplate from our orchestration framework
    CALL dataadmin.sp_get_dag_run_metadata(COALESCE(last_query_id(), ''NOT_FOUND'')) INTO :run_metadata;
    parent_query_id := :run_metadata:parent_query_id::VARCHAR;
    task_run_group_id := :run_metadata:graph_run_group_id::VARCHAR;
    attempt_number := :run_metadata:run_attempt_number::NUMBER;
    session_id := :run_metadata:session_id::VARCHAR;
    task_name := COALESCE(:run_metadata:task_name::VARCHAR, ''sp_dq_is_yesterday_loaded_in_csf''); -- Task name might be null on manual run

    -- The actual data quality check logic
    yesterday_date := DATEADD(day, -1, CURRENT_DATE());

    SELECT max(fiscal_date) INTO :max_fiscal_date
    FROM DATAWAREHOUSE.CLOSEOUTSUMMARY_FACT
    WHERE dw_iscurrentrow;

    select_query_id := last_query_id();

    IF (:max_fiscal_date IS NULL OR :max_fiscal_date <> :yesterday_date) THEN
        -- If the max date is null (empty table) or less than yesterday, log a data quality issue.
        INSERT INTO DATAADMIN.error_logs (
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
            :select_query_id,
            3, -- 1=Hard Fail, 2=Soft Fail, 3=Data Quality
            ''WARNING'',
            NULL, -- No SQL error code for DQ check
            ''Data quality check failed: CLOSEOUTSUMMARY_FACT is not updated with yesterday data.'',
            NULL, -- No SQL state for DQ check
            OBJECT_CONSTRUCT(
                ''check_name'', ''sp_dq_is_yesterday_loaded_in_csf'',
                ''yesterday_date'', :yesterday_date,
                ''max_fiscal_date_in_table'', :max_fiscal_date
            );
        RETURN ''Data quality check failed. CLOSEOUTSUMMARY_FACT max(fiscal_date) is '' || COALESCE(TO_VARCHAR(:max_fiscal_date), ''NULL'') || '', expected at least '' || :yesterday_date;
    ELSE
        RETURN ''Data quality check passed.'';
    END IF;

END;
';