CREATE OR REPLACE PROCEDURE "SP_DQ_DISCOUNT_ARRAY_ISSUE"()
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
    task_name := COALESCE(:run_metadata:task_name::VARCHAR, ''sp_dq_discount_array_issue'');

    -- Set the target date for the validation
    yesterday_date := DATEADD(day, -1, CURRENT_DATE());

    --===========================
    --start validation query
    --===========================
    WITH current_cheques_raw AS (
    -- Step 1: Get the raw data for current cheques on a specific day and location
        SELECT
            ppc.ID,
            ppc.info,
            ppc.balance,
            ppc.ITEMS,
            cf.fiscal_date,
            cf.location_dim_fk
        FROM datalanding.POSAPI_PUBLIC_CHEQUE ppc
        -- (select * FROM DATALANDING.POSAPI_PUBLIC_CHEQUE WHERE ITEMS <> ''__value_not_modified__'') ppc --to exclude non toast column issue
        INNER JOIN (
            --cheques from warehouse
            SELECT CHEQUE_FACT_NK, MTLN_CDC_SEQUENCE_NUMBER, fiscal_date, location_dim_fk
            FROM datawarehouse.cheque_fact
            WHERE dw_iscurrentrow
            AND status in (''Closed'')
            AND location_dim_fk IN (37,7,1,35,9,38,32,27,13,4,41,29,43,11,21,25,3,6,39) -- Target Location ID from example
            AND fiscal_date = :yesterday_date -- Parameterized date
        ) cf ON ppc.ID = cf.CHEQUE_FACT_NK AND ppc.MTLN_CDC_SEQUENCE_NUMBER = cf.MTLN_CDC_SEQUENCE_NUMBER
        WHERE 1=1
            AND ppc.ITEMS <> ''__value_not_modified__'' 
        ),
        check_discounts_sum AS (
            -- Step 2a: Flatten the discounts array from the INFO column (check-level discounts) and sum the appliedValue
            SELECT
                ID,
                SUM(d.value:appliedValue::DECIMAL(38, 2)) as sum_applied_value_from_info
            FROM current_cheques_raw,
            LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(''{discounts:'' || TRY_PARSE_JSON(info):discounts || ''}''), PATH => ''discounts'') d
            WHERE d.value:appliedValue IS NOT NULL
            GROUP BY ID
        ),
        item_discounts_sum AS (
            -- Step 2b: Flatten the ITEMS array and their internal discounts array (item-level discounts) and sum the appliedValue
            SELECT
                ID,
                SUM(d.value:appliedValue::DECIMAL(38, 2)) as sum_applied_value_from_items
            FROM current_cheques_raw,
            LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(ITEMS)) i,
            LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(''{discounts:'' || i.value:discounts || ''}''), PATH => ''discounts'') d
            WHERE i.value:status IN (''Added'', ''Sent'')
            AND d.value:appliedValue IS NOT NULL
            GROUP BY ID
        )
    -- Step 3: Compare the aggregated discount values from BALANCE against the sums from INFO and ITEMS
    SELECT
        ccr.ID AS CHEQUE_ID,
        ccr.fiscal_date,
        ccr.location_dim_fk,

        -- Check-level discount comparison
        TRY_PARSE_JSON(ccr.balance):discountCheck::DECIMAL(38, 4) AS discountCheck_from_balance,
        COALESCE(cds.sum_applied_value_from_info, 0) AS sum_applied_value_from_info,
        ABS((COALESCE(discountCheck_from_balance, 0) - COALESCE(sum_applied_value_from_info, 0))) AS difference_check_discount,

        -- Item-level discount comparison
        TRY_PARSE_JSON(ccr.balance):discountItem::DECIMAL(38, 4) AS discountItem_from_balance,
        COALESCE(ids.sum_applied_value_from_items, 0) AS sum_applied_value_from_items,
        ABS((COALESCE(discountItem_from_balance, 0) - COALESCE(sum_applied_value_from_items, 0))) AS difference_item_discount,

        -- Total discount for reference
        TRY_PARSE_JSON(ccr.balance):discount::DECIMAL(38, 4) AS total_discount_from_balance,

        (ABS(difference_check_discount) + ABS(difference_item_discount)) as total_delta,

        -- Raw data for inspection
        ccr.info,
        ccr.balance,
        ccr.items
    FROM current_cheques_raw ccr
    LEFT JOIN check_discounts_sum cds ON ccr.ID = cds.ID
    LEFT JOIN item_discounts_sum ids ON ccr.ID = ids.ID
    WHERE
        -- Filtering for records where the values do not match, allowing for small floating point differences
        ABS(difference_check_discount) > 0.001 OR ABS(difference_item_discount) > 0.001
    ORDER BY (ABS(difference_check_discount) + ABS(difference_item_discount)) DESC
    ;
    --===========================
    --END VALIDATION QUERY
    --===========================

    validation_query_id := last_query_id();

    CREATE OR REPLACE TEMP TABLE dq_discount_array_issue_results AS
        SELECT * FROM TABLE(RESULT_SCAN(:validation_query_id)); 

    SELECT COUNT(*) INTO :row_count FROM dq_discount_array_issue_results;

    IF (row_count > 0) THEN
        -- If there are deltas, construct the summary error message
        -- We first need to aggregate the deltas per location, and then list-aggregate the results.
        SELECT ''For '' || :yesterday_date || '' the following locations have deltas: '' ||
               LISTAGG(location_summary, '' ; '') WITHIN GROUP (ORDER BY LOCATION_DIM_FK)
        INTO :error_message
        FROM (
            SELECT 
                LOCATION_DIM_FK,
                LOCATION_DIM_FK || '': '' || SUM(total_delta) AS location_summary
            FROM dq_discount_array_issue_results
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
            OBJECT_CONSTRUCT(''validation_name'', ''SP_DQ_DISCOUNT_ARRAY_ISSUE'', ''fiscal_date_checked'', :yesterday_date);

        RETURN ''Data quality check failed. Details logged: '' || :error_message;
    ELSE
        -- If there are no deltas, return a success message
        RETURN ''Data quality check passed for '' || :yesterday_date || ''. No discount calculation deltas found.'';
    END IF;

END;
';