CREATE OR REPLACE PROCEDURE "SP_DQ_TAX_ARRAY_ISSUE"()
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
    yesterday_date DATE;
    validation_query_id VARCHAR;
    row_count NUMBER;
    error_message VARCHAR;

BEGIN
    -- Get runtime metadata for logging and traceability
    CALL dataadmin.sp_get_dag_run_metadata(COALESCE(last_query_id(), ''NOT_FOUND'')) INTO :run_metadata;
    parent_query_id := :run_metadata:parent_query_id::VARCHAR;
    task_run_group_id := :run_metadata:graph_run_group_id::VARCHAR;
    attempt_number := :run_metadata:run_attempt_number::NUMBER;
    session_id := :run_metadata:session_id::VARCHAR;
    task_name := COALESCE(:run_metadata:task_name::VARCHAR, ''sp_dq_tax_array_issue'');

    -- Set the target date for the validation
    yesterday_date := DATEADD(day, -1, CURRENT_DATE());

    -- run validation and store it in a temp table
    WITH main_query AS (
        SELECT
            CHK.LOCATION_DIM_FK AS LOCATION_DIM_FK,
            CHK.FISCAL_DATE AS FISCAL_DATE,
            POS.ID AS cheque_id,
            TRY_PARSE_JSON(POS.BALANCE):tax::DECIMAL(10, 2) AS balance_tax,
            COALESCE(SUM(CASE 
                             WHEN ITM.value:status::STRING IN (''Added'', ''Sent'') 
                             THEN TAX.value:tax::DECIMAL(10, 2) 
                             ELSE 0 
                         END), 0) AS calculated_items_tax_total,
            abs(balance_tax - calculated_items_tax_total) AS delta
        FROM
            DATALANDING.POSAPI_PUBLIC_CHEQUE POS
            -- (select * FROM DATALANDING.POSAPI_PUBLIC_CHEQUE  WHERE ITEMS <> ''__value_not_modified__'') POS --to exclude non toast column issue
        INNER JOIN
            (
                SELECT CHEQUE_FACT_NK, MTLN_CDC_SEQUENCE_NUMBER, FISCAL_DATE, LOCATION_DIM_FK
                FROM DATAWAREHOUSE.CHEQUE_FACT
                WHERE DW_ISCURRENTROW
                AND FISCAL_DATE = :yesterday_date -- Parameterized date
                AND STATUS IN(''Closed'')
            ) CHK ON POS.ID = CHK.CHEQUE_FACT_NK AND POS.MTLN_CDC_SEQUENCE_NUMBER = CHK.MTLN_CDC_SEQUENCE_NUMBER
        , LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(POS.ITEMS), OUTER => TRUE) ITM
        , LATERAL FLATTEN(INPUT => ITM.value:taxes, OUTER => TRUE) TAX
        GROUP BY 1,2,3,4
    )
    SELECT 
        LOCATION_DIM_FK,
        FISCAL_DATE,
        delta
    FROM main_query
    WHERE delta > 0;

    validation_query_id := last_query_id();

     CREATE OR REPLACE TEMP TABLE dq_tax_array_issue_results AS
     SELECT * FROM TABLE(RESULT_SCAN(:validation_query_id)); 

    SELECT COUNT(*) INTO :row_count FROM dq_tax_array_issue_results;

    IF (row_count > 0) THEN
        -- If there are deltas, construct the summary error message
        -- We first need to aggregate the deltas per location, and then list-aggregate the results.
        SELECT ''For '' || :yesterday_date || '' the following locations have deltas: '' ||
               LISTAGG(location_summary, '' ; '') WITHIN GROUP (ORDER BY LOCATION_DIM_FK)
        INTO :error_message
        FROM (
            SELECT 
                LOCATION_DIM_FK,
                LOCATION_DIM_FK || '': '' || SUM(delta) AS location_summary
            FROM dq_tax_array_issue_results
            GROUP BY LOCATION_DIM_FK
        );

        -- Log the single, summarized error message
        INSERT INTO DATAADMIN.error_logs (
            parent_query_id, task_run_group_id, attempt_number, session_id, task_name,
            failed_query_id, error_type_id, severity, sql_error_code, sql_error_message, sql_state, details
        )
        SELECT
            :parent_query_id, :task_run_group_id, :attempt_number, :session_id, :task_name,
            :validation_query_id, 3, ''WARNING'', NULL, :error_message, NULL,
            OBJECT_CONSTRUCT(''check_name'', ''sp_dq_tax_array_issue'', ''fiscal_date_checked'', :yesterday_date);

        RETURN ''Data quality check failed. Details logged: '' || :error_message;
    ELSE
        -- If there are no deltas, return a success message
        RETURN ''Data quality check passed for '' || :yesterday_date || ''. No tax calculation deltas found.'';
    END IF;

END;
';