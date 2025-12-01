CREATE OR REPLACE PROCEDURE "SP_DQ_ACCELERATION_ELEGIBLE"()
RETURNS VARIANT
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
    select_query_id VARCHAR;
    opportunities_result VARIANT;
    row_count NUMBER;
BEGIN
    -- Get runtime metadata for logging and traceability
    CALL dataadmin.sp_get_dag_run_metadata(COALESCE(last_query_id(), ''NOT_FOUND'')) INTO :run_metadata;
    parent_query_id := :run_metadata:parent_query_id::VARCHAR;
    task_run_group_id := :run_metadata:graph_run_group_id::VARCHAR;
    attempt_number := :run_metadata:run_attempt_number::NUMBER;
    session_id := :run_metadata:session_id::VARCHAR;
    task_name := COALESCE(:run_metadata:task_name::VARCHAR, ''sp_dq_acceleration_elegible'');

    -- The actual data quality check logic
    -- Aggregate potential opportunities into a JSON array, preserving the order of importance.
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        ''query_id'', q.query_id,
        ''eligible_query_acceleration_time'', q.eligible_query_acceleration_time,
        ''query_text'', q.query_text,
        ''query_type'', q.query_type,
        ''user_name'', q.user_name,
        ''role_name'', q.role_name,
        ''query_tag'', q.query_tag,
        ''start_time'', q.start_time,
        ''end_time'', q.end_time,
        ''user_type'', q.user_type
    )) WITHIN GROUP (ORDER BY q.eligible_query_acceleration_time DESC)
    INTO :opportunities_result
    FROM (
        SELECT
            qae.query_id,
            qae.eligible_query_acceleration_time,
            qh.query_text,
            qh.query_type,
            qh.user_name,
            qh.role_name,
            qh.query_tag,
            qh.start_time,
            qh.end_time,
            qh.user_type
        FROM (
            select query_id,eligible_query_acceleration_time  from SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE WHERE
            start_time > DATEADD(''day'', -7, CURRENT_TIMESTAMP())
            ) qae
        LEFT JOIN
            (select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY where start_time > DATEADD(''day'', -7, CURRENT_TIMESTAMP()) ) qh
             ON qae.query_id = qh.query_id
    ) q;

    select_query_id := last_query_id();
    row_count := ARRAY_SIZE(COALESCE(:opportunities_result, ARRAY_CONSTRUCT()));

    IF (row_count > 0) THEN
        -- If there are opportunities, log a single data quality issue.
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
            NULL,
            ''Data quality check failed: Query acceleration opportunities detected.'',
            NULL,
            OBJECT_CONSTRUCT(
                ''check_name'', ''sp_dq_acceleration_elegible'',
                ''eligible_query_count'', :row_count,
                ''message'', ''Queries eligible for acceleration were found. See return value for details.''
            );
        RETURN :opportunities_result;
    ELSE
        RETURN OBJECT_CONSTRUCT(''status'', ''Data quality check passed. No acceleration opportunities found.'');
    END IF;

END;
';