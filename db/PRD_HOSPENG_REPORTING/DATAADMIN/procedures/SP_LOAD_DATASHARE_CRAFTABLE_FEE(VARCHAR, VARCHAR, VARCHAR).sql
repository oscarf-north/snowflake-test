CREATE OR REPLACE PROCEDURE "SP_LOAD_DATASHARE_CRAFTABLE_FEE"("START_DATE" VARCHAR, "END_DATE" VARCHAR, "LOCATION_IDS" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN
    -- Execute the source procedure that returns the data
    CALL DATAADMIN.SP_DATASHARE_CRAFTABLE_FEES(:START_DATE, :END_DATE, :LOCATION_IDS);

    -- Insert the results into the target table
    INSERT INTO DATASHARE_DEV.CRAFTABLE_FEE (
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Check Number",
        "Check ID",
        "Fee Name",
        "Fee ID",
        "Fee Amount",
        "Fee Tax"
    )
    SELECT
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Check Number",
        "Check ID",
        "Fee Name",
        "Fee ID",
        "Fee Amount",
        "Fee Tax"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN ''Success: Data loaded into DATASHARE_DEV.CRAFTABLE_FEE.'';
END';