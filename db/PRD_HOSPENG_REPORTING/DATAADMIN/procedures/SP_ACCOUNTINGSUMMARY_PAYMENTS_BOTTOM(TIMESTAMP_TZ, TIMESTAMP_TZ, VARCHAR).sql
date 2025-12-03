CREATE OR REPLACE PROCEDURE "SP_ACCOUNTINGSUMMARY_PAYMENTS_BOTTOM"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet           resultset;
  -- startdate string    := ''2025-06-11'';  
  -- enddate string      := ''2025-06-11''; 
  -- locationid string   := ''[351]'';
  locationidS string  :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_TABLE1;

------------------------------------------------------------------------------------------------  
CALL DATAADMIN.SP_REPORT_ACCOUNTSUMMARY_PAYMENTS(:startdate,:enddate,:locationid);
CREATE TEMP TABLE TEMP_TABLE1 AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT ROW_NUMBER() OVER (ORDER BY "Location ID")  AS "Support ID"   , *

 FROM (
  SELECT "Location ID"                             AS "Location ID"
    ,''Total Payments''                              AS "Group 2"

    ,SUM("Total")                                  AS "Total" 
FROM TEMP_TABLE1
    WHERE "Group 1" = ( ''Total Payments'') 
         AND  "Group 2" in (''Tips2'',''Surcharges'',''Net Sales'',''Gift Card Sales'',''Taxes'',''Unpaid'',''Deposits'',''PayInOut'',''Refunds'')  --should include Net Sales
GROUP BY "Location ID"

UNION

SELECT   "Location ID"                             AS "Location ID"
    ,REPLACE("Group 2",''Tips2'',''Tips'')             AS "Group 2"

    ,SUM("Total") * -1                             AS "Total" 
FROM TEMP_TABLE1
    WHERE "Group 1" = ( ''Total Payments'') 
         AND  "Group 2" in (''Tips2'',''Surcharges'',''Gift Card Sales'',''Taxes'',''Unpaid'',''Deposits'',''PayInOut'',''Refunds'') --Should not include net sals
GROUP BY "Group 1","Group 2","Location ID"
)
     ;    
     
--=========================================================================================
 reportSet:= (
   SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';