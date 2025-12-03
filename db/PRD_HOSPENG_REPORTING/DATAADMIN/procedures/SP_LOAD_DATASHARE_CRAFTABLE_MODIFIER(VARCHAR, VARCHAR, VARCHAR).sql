CREATE OR REPLACE PROCEDURE "SP_LOAD_DATASHARE_CRAFTABLE_MODIFIER"("START_DATE" VARCHAR, "END_DATE" VARCHAR, "LOCATION_IDS" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN
    -- Execute the source procedure that returns the data
    CALL DATAADMIN.SP_DATASHARE_CRAFTABLE_MODIFIER(:START_DATE, :END_DATE, :LOCATION_IDS);

    -- Insert the results into the target table
    INSERT INTO DATASHARE_DEV.CRAFTABLE_MODIFIER (
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Check ID",
        "Check Number",
        "Item Name",
        "Item ID",
        "Modifier Group",
        "Modifier Group ID",
        "Is Sub Modifier",
        "Modifier Parent Name",
        "Modifier Parent ID",
        "Modifier Name",
        "Modifier ID",
        "Modifier Quantity",
        "Modifier Amount"
    )
    SELECT
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Check ID",
        "Check Number",
        "Item Name",
        "Item ID",
        "Modifier Group",
        "Modifier Group ID",
        "Is Sub Modifier",
        "Modifier Parent Name",
        "Modifier Parent ID",
        "Modifier Name",
        "Modifier ID",
        "Modifier Quantity",
        "Modifier Amount"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN ''Success: Data loaded into DATASHARE_DEV.CRAFTABLE_MODIFIER.'';
END';