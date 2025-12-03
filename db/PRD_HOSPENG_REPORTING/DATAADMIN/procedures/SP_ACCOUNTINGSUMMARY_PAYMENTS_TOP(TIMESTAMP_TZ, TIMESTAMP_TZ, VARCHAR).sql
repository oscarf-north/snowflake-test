CREATE OR REPLACE PROCEDURE "SP_ACCOUNTINGSUMMARY_PAYMENTS_TOP"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
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

SELECT  ROW_NUMBER() OVER (ORDER BY "Location ID") AS "Support ID"     
        ,"Location ID"                             AS "Location ID"
       ,"Group 1"                                  AS "Group 1"
       ,"Group 2"                                  AS "Group 2"
       ,SUM("Count")                               AS "Count"
       ,SUM("Tips")                                AS "Tips"
       ,SUM("Sales")                               AS "Sales"
       ,SUM("Pay In")                              AS "Pay In"
       ,SUM("Deposit")                             AS "Deposit"
       ,SUM("Total")                               AS "Total" 
    FROM TEMP_TABLE1
      WHERE "Group 1" <> ''Total Payments''
    GROUP BY "Group 1","Group 2","Location ID"
    ORDER BY "Location ID","Group 1","Group 2"
;    

--=========================================================================================
 reportSet:= (
   SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';