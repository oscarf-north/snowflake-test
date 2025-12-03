CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_ITEM"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
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
DROP TABLE if exists ITEM_DATA_TEMP;

CALL DATAADMIN.SP_REPORT_PMIX(:startdate,:enddate,:locationidS);
CREATE OR REPLACE TEMP TABLE ITM_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
;
-----------------------------------------------------------------------------------------------------------------------
     reportSet := (
         SELECT 
         ORG.ORGANIZATION             AS "Organization Name"
         ,ORG.ORGANIZATION_DIM_NK     AS "Organization ID"
         ,IDT."Location"              AS "Location Name"
         ,IDT."Location ID"           AS "Location ID"
         ,IDT."Fiscal Date"           AS "Business Day"
         ,IDT."Check ID"              AS "Check Number"
         ,IDT."Check"                 AS "Check ID"
         ,IDT."Menu Item"             AS "Item Name"
         ,IDT."Support ID"            AS "Item ID"
         ,IDT."Category"              AS "Item Category"
         ,IDT."Price"                 AS "Item Price"
         ,IDT."Count"                 AS "Item Quantity"
         ,IDT."Tax"                   AS "Item Tax"
         ,IDT."Net"                   AS "Net Amount"
         FROM 
         ITM_DATA_TEMP                                      IDT
         LEFT JOIN DATAWAREHOUSE.ORGANIZATION_DIM           ORG
            ON IDT."Location ID"  = ORG.ORGANIZATION_DIM_NK
            AND ORG.DW_ISCURRENTROW
     );
--=====================================================================================================================
RETURN TABLE(reportSet);
END';