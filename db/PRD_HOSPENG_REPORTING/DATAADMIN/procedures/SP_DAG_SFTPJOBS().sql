CREATE OR REPLACE PROCEDURE "SP_DAG_SFTPJOBS"()
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
    --actual variables for pipeline
    dateName VARCHAR;
    yesterday_dates VARIANT;

    loc_id VARCHAR;
    avero_loc_id VARCHAR;
BEGIN
    CALL dataadmin.sp_get_dag_run_metadata(COALESCE(last_query_id(), ''NOT_FOUND'')) INTO :run_metadata;
    parent_query_id := :run_metadata:parent_query_id::VARCHAR;
    task_run_group_id := :run_metadata:graph_run_group_id::VARCHAR;
    attempt_number := :run_metadata:run_attempt_number::NUMBER;
    session_id := :run_metadata:session_id::VARCHAR;
    task_name := :run_metadata:task_name::VARCHAR;

    --TODO add query tags in future sprints

    CALL SP_GET_YESTERDAY_DATES() INTO :yesterday_dates;
    dateName := :yesterday_dates:dateName;

    CALL SP_LOAD_AVERO_GETLOCATIONS();
    LET locations_cursor CURSOR FOR SELECT locationId, averoLocationId FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Loop through the cursor, row by row.
    FOR location_row IN locations_cursor DO
        -- Access the columns from the cursor using the row variable
        loc_id := location_row.locationId::VARCHAR ;
        avero_loc_id := location_row.averoLocationId::VARCHAR ;

        BEGIN
            -- execute worker procedures as soft fails.
            CALL dataadmin.SP_LOAD_AVERO_EXTRACTALLFILES(:dateName,:loc_id);
            CALL dataadmin.SP_COPY_AVEROFILES_FROM_S3_TO_SFTP(:dateName,:loc_id, :avero_loc_id);
        EXCEPTION
            -- If the worker fails, log the error here and continue the loop.
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
                VALUES (
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
                    null
                );
        END;
    END FOR;

RETURN ''Graph Run Group ID: ''||:run_metadata:graph_run_group_id || '', Attempt: ''||:run_metadata:run_attempt_number;
END;
';