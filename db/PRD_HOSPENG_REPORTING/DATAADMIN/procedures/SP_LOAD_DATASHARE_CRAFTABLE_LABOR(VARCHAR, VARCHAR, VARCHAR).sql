CREATE OR REPLACE PROCEDURE "SP_LOAD_DATASHARE_CRAFTABLE_LABOR"("START_DATE" VARCHAR, "END_DATE" VARCHAR, "LOCATION_IDS" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'BEGIN
    -- Execute the source procedure that returns the data
    CALL DATAADMIN.SP_DATASHARE_CRAFTABLE_LABOR(:START_DATE, :END_DATE, :LOCATION_IDS);

    -- Insert the results into the target table
    INSERT INTO DATASHARE_DEV.CRAFTABLE_LABOR (
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Shift ID",
        "Time In",
        "Time Out",
        "Department",
        "Job Title",
        "Employee No",
        "Employee Name",
        "Regular Hours",
        "Overtime Hours",
        "Hourly wage",
        "Break Seconds"
    )
    SELECT
        "Organization Name",
        "Organization ID",
        "Location Name",
        "Location ID",
        "Business Day",
        "Shift ID",
        "Time In",
        "Time Out",
        "Department",
        "Job Title",
        "Employee No",
        "Employee Name",
        "Regular Hours",
        "Overtime Hours",
        "Hourly wage",
        "Break Seconds"
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN ''Success: Data loaded into DATASHARE_DEV.CRAFTABLE_LABOR.'';
END';