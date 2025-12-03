CREATE OR REPLACE PROCEDURE "SP_GET_DAG_RUN_METADATA"("PARENT_QUERY_ID" VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    -- Local variables
    graph_run_group_id VARCHAR;
    task_name VARCHAR;
    run_attempt_number NUMBER;
    session_id VARCHAR;
    metadata_obj VARIANT;
BEGIN
    session_id := COALESCE(CURRENT_SESSION(), ''NOT_FOUND'');

    -- GET RUNTIME INFO
    BEGIN
        graph_run_group_id := SYSTEM$TASK_RUNTIME_INFO(''CURRENT_TASK_GRAPH_RUN_GROUP_ID'');
        task_name := SYSTEM$CURRENT_USER_TASK_NAME();
    EXCEPTION
        WHEN OTHER THEN
            graph_run_group_id := ''MANUAL_RUN'';
            task_name := null;
    END;

    -- GET ATTEMPT NUMBER
    IF (task_name IS NULL) THEN
        run_attempt_number := 1; -- Manual run is always attempt 1
    ELSE
        BEGIN
            SELECT COALESCE(MAX(attempt_number), 1)
            INTO :run_attempt_number
            FROM TABLE(information_schema.task_history(
                scheduled_time_range_start => dateadd(''hour'', -1, current_timestamp()),
                task_name => :task_name
            ))
            WHERE graph_run_group_id = :graph_run_group_id;
        EXCEPTION
            WHEN OTHER THEN
                run_attempt_number := 1; -- Fallback on permission/other error
        END;
    END IF;

    -- Construct a VARIANT object to return all metadata
    metadata_obj := OBJECT_CONSTRUCT(
        ''parent_query_id'', :PARENT_QUERY_ID,
        ''graph_run_group_id'', :graph_run_group_id,
        ''task_name'', :task_name,
        ''run_attempt_number'', :run_attempt_number,
        ''session_id'', :session_id
    );

    RETURN metadata_obj;
END;
';