CREATE OR REPLACE PROCEDURE "SP_GET_YESTERDAY_DATES"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
/*
 * Stored Procedure: SP_GET_YESTERDAY_DATES
 *
 * Description:
 * This procedure calculates and retrieves yesterday''s date in two common string formats.
 * It''s designed as a reusable utility to provide consistent date values for other
 * stored procedures or queries that rely on the previous day''s date.
 *
 * Returns:
 * A VARIANT (JSON object) containing two key-value pairs:
 * - "dateName": VARCHAR - Yesterday''s date in ''YYYY-MM-DD'' format.
 * - "dateText": VARCHAR - Yesterday''s date in ''YYYYMMDD'' format.
 *
 * Example Return Value:
 * {
 * "dateName": "2025-09-01",
 * "dateText": "20250901"
 * }
 *
 * Usage:
 * CALL SP_GET_YESTERDAY_DATES();
 *
 */
DECLARE
    -- Declare variables to hold the calculated date values
    date_name_val VARCHAR;
    date_text_val VARCHAR;
    return_object VARIANT;
BEGIN
    -- Execute the query and store the results into the variables
    SELECT
        TO_CHAR(DATEADD(day, -1, CURRENT_DATE()), ''YYYY-MM-DD''),
        REPLACE(TO_CHAR(DATEADD(day, -1, CURRENT_DATE()), ''YYYY-MM-DD''), ''-'', '''')
    INTO
        :date_name_val,
        :date_text_val;

    -- Construct a JSON object to return both values
    SELECT OBJECT_CONSTRUCT(''dateName'', :date_name_val, ''dateText'', :date_text_val)
    INTO :return_object;

    RETURN return_object;
END;
';