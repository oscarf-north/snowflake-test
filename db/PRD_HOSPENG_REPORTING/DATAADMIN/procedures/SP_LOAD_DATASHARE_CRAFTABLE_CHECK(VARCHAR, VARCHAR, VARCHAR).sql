CREATE OR REPLACE PROCEDURE "SP_LOAD_DATASHARE_CRAFTABLE_CHECK"("START_DATE" VARCHAR, "END_DATE" VARCHAR, "LOCATION_IDS" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN
    -- Execute the source procedure that returns the data
    CALL DATAADMIN.SP_DATASHARE_CRAFTABLE_CHECK(:START_DATE, :END_DATE, :LOCATION_IDS);

    -- Insert the results into the target table
    INSERT INTO DATASHARE_DEV.CRAFTABLE_CHECK (
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Check Number",
        "Check ID",
        "Rev Center Name",
        "Rev Center ID",
        "Meal Period/Day Part Name",
        "Meal Period/Day Part ID",
        "Ticket Open",
        "Ticket Closed",
        "Order Type Name",
        "Order Type ID",
        "Server Name",
        "Server ID",
        "Check Total",
        "Check Total Tips",
        "Gratuity Total"
    )
    SELECT
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Check Number",
        "Check ID",
        "Rev Center Name",
        "Rev Center ID",
        "Meal Period/Day Part Name",
        "Meal Period/Day Part ID",
        "Ticket Open",
        "Ticket Closed",
        "Order Type Name",
        "Order Type ID",
        "Server Name",
        "Server ID",
        "Check Total",
        "Check Total Tips",
        "Gratuity Total"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN ''Success: Data loaded into DATASHARE_DEV.CRAFTABLE_CHECK.'';
END';