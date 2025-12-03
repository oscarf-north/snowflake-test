CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_FEES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  reportSet resultset;
--   startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';
--   enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z'';
--   locationid string           := ''[35]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today char(11)              := CURRENT_DATE()::date::VARCHAR(10);
-----------------------------------------------------------------------------------------------------------------------
BEGIN
DROP TABLE if exists FEES_DATA_TEMP;

CALL DATAADMIN.SP_REPORT_FEES(:startdate,:enddate,:locationidS);
CREATE TEMP TABLE FEES_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
;
-----------------------------------------------------------------------------------------------------------------------
     reportSet := (
         SELECT 
            ORG.ORGANIZATION             AS "Organization Name",
            ORG.ORGANIZATION_DIM_NK      AS "Organization ID",
            FDT."Location"               AS "Location Name",
            FDT."Location ID"            AS "Location ID",
            FDT."Fiscal Date"            AS "Business Day",
            FDT."Check"                  AS "Check Number"
            ,null                        AS "Check ID" --We will have to add Check ID to the fee report
            ,FDT."Fee"                   AS "Fee Name"
            ,FDT."Support ID"            AS "Fee ID"
            ,FDT."Total"                 AS "Fee Amount"
            ,null                        AS "Fee Tax" --TODO pending to decide with Diane how to get this
         FROM 
         FEES_DATA_TEMP FDT
         LEFT JOIN DATAWAREHOUSE.ORGANIZATION_DIM                                        ORG
            ON FDT."Location ID"  = ORG.ORGANIZATION_DIM_NK
            AND ORG.DW_ISCURRENTROW
     );
--=====================================================================================================================
RETURN TABLE(reportSet);
END';