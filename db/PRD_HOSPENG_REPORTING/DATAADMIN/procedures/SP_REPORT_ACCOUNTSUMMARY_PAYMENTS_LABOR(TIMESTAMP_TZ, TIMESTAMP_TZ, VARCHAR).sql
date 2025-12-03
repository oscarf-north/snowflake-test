CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_LABOR"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet           resultset;
  -- startdate string    := ''2025-11-10'';  
  -- enddate string      := ''2025-11-10''; 
  -- locationid string   := ''[1]'';
  locationidS string  :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_TABLE1;

------------------------------------------------------------------------------------------------  
CALL DATAADMIN.SP_REPORT_LABOR(:startdate,:enddate,:locationid);

SELECT 
  "Location ID"                                                        AS "Location ID"
  ,"Job Position"                                                      AS "Job Position"
  ,"Job Category"                                                      AS "Job Category"
  ,("Regular Seconds")::DECIMAL(18,2)                                  AS "Regular Seconds"
  ,("Overtime Seconds")::DECIMAL(18,2)                                 AS "Overtime Seconds"
  ,((("Regular Seconds"/3600)  * "Regular Rate") 
      + (("Overtime Seconds"/3600) * "Overtime Rate"))::DECIMAL(18,2)  AS "Labor Cost"
  ,IFNULL("Net Sales",0)                                               AS "Net Sales"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;
    
--=========================================================================================
 reportSet:= (
 SELECT ROW_NUMBER() OVER (ORDER BY "Location ID")      AS  "Support ID" 
  , "Location ID"                                       AS "Location ID"
  , "Job Position"                                      AS "Job Position"
  , "Job Category"                                      AS "Job Category" 
  , SUM("Regular Seconds")                              AS "Regular Seconds"
  , SUM("Overtime Seconds")                             AS "Overtime Seconds"
  , SUM("Labor Cost")                                   AS "Labor Cost"
  , SUM("Net Sales")                                    AS "Net Sales"
 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
 GROUP BY "Location ID"  
   ,"Job Position"
   ,"Job Category" 
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';