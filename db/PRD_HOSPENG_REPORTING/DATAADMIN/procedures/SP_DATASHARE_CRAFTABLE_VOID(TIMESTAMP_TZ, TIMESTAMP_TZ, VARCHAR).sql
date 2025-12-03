CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_VOID"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  reportSet resultset;
  -- startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';
  -- enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z'';
  -- locationid string           := ''[35]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today char(11)              := CURRENT_DATE()::date::VARCHAR(10);
-----------------------------------------------------------------------------------------------------------------------
BEGIN
DROP TABLE if exists VOID_DATA_TEMP;

CALL DATAADMIN.SP_REPORT_VOID_0001(:startdate,:enddate,:locationidS);
CREATE TEMP TABLE VOID_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
;
-----------------------------------------------------------------------------------------------------------------------
     reportSet := (
         SELECT 
             ORG.ORGANIZATION          AS "Organization Name"
            ,ORG.ORGANIZATION_DIM_NK   AS "Organization ID"
            ,VDT."Location"            AS "Location Name"
            ,VDT."Location ID"         AS "Location ID"
            ,VDT."Fiscal Date"         AS "Business Day"
            ,VDT."Support ID"          AS "Void ID"
            ,VDT."Level"               AS "Void Level"
            ,VDT."Reason"              AS "Void Reason"
            ,VDT."Check"               AS "Check Number"  
            ,VDT."Check ID"            AS "Check ID"
            ,VDT."Item"                AS "Item Name"
            ,VDT."Item ID"             AS "Item ID"
            ,null                      AS "Item Amount" --TODO do we need this? the void report is using the item price to set the void amount, so how can this number be different than the void amount?
            ,VDT."Approver"            AS "Employee for Void (approver)"
            ,VDT."Amount"              AS "Void Amount" --TODO there are 0 and null void amounts. why?
         FROM 
         VOID_DATA_TEMP                                     VDT
         LEFT JOIN DATAWAREHOUSE.ORGANIZATION_DIM           ORG
            ON VDT."Location ID" = ORG.ORGANIZATION_DIM_NK
            AND ORG.DW_ISCURRENTROW
     );
--=====================================================================================================================
RETURN TABLE(reportSet);
END';