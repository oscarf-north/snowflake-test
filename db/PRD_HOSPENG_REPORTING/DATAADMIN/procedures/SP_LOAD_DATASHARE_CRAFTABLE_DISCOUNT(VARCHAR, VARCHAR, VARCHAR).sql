CREATE OR REPLACE PROCEDURE "SP_LOAD_DATASHARE_CRAFTABLE_DISCOUNT"("START_DATE" VARCHAR, "END_DATE" VARCHAR, "LOCATION_IDS" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN
    -- Execute the source procedure that returns the data
    CALL DATAADMIN.SP_DATASHARE_CRAFTABLE_DISCOUNT(:START_DATE, :END_DATE, :LOCATION_IDS);

    -- Insert the results into the target table
    INSERT INTO DATASHARE_DEV.CRAFTABLE_DISCOUNT (
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Discount ID",
        "Discount Level",
        "Discount Name",
        "Check Number",
        "Check ID",
        "Item Name",
        "Item ID",
        "Employee for Discount (approver)",
        "Discount Amount",
        "Cash Discount Amount",
        "Disc Amount No Cash"
    )
    SELECT
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Discount ID",
        "Discount Level",
        "Discount Name",
        "Check Number",
        "Check ID",
        "Item Name",
        "Item ID",
        "Employee for Discount (approver)",
        "Discount Amount",
        "Cash Discount Amount",
        "Disc Amount No Cash"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN ''Success: Data loaded into DATASHARE_DEV.CRAFTABLE_DISCOUNT.'';
END';