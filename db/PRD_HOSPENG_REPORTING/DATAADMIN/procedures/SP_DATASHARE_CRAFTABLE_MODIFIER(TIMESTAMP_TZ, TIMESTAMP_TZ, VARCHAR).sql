CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_MODIFIER"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
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
DROP TABLE if exists MODIFIER_DATA_TEMP;

CALL DATAADMIN.SP_REPORT_MODIFIER(:startdate,:enddate,:locationidS);
CREATE TEMP TABLE MODIFIER_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
;
-----------------------------------------------------------------------------------------------------------------------
     reportSet := (
         SELECT
             ORG.ORGANIZATION          AS "Organization Name"
            ,ORG.ORGANIZATION_DIM_NK   AS "Organization ID"
            ,MDT."Location"            AS "Location Name"
            ,MDT."Location ID"         AS "Location ID"
            ,MDT."Fiscal Date"         AS "Business Day" 
            ,MDT."Check ID"            AS "Check ID"
            ,MDT."Check"               AS "Check Number"
            ,MDT."Menu Item"           AS "Item Name"
            ,MDT."Item ID"             AS "Item ID"
            ,MDT."Modifier Group"      AS "Modifier Group"
            ,MDT."Modifier Group ID"   AS "Modifier Group ID"
            ,FALSE                     AS "Is Sub Modifier"
            ,MDT."Menu Item"           AS "Modifier Parent Name"
            ,MDT."Item ID"             AS "Modifier Parent ID"
            ,MDT."Modifier"            AS "Modifier Name"
            ,MDT."Support ID"          AS "Modifier ID"
            ,MDT."Count"               AS "Modifier Quantity"
            ,MDT."Total Sales"         AS "Modifier Amount"
         FROM 
         MODIFIER_DATA_TEMP                                 MDT
         LEFT JOIN DATAWAREHOUSE.ORGANIZATION_DIM           ORG
            ON MDT."Location ID" = ORG.ORGANIZATION_DIM_NK
            AND ORG.DW_ISCURRENTROW
     );
--=====================================================================================================================
RETURN TABLE(reportSet);
END';