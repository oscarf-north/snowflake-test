CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_REFUNDS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string           := ''[351,352,400,403,352,501,357]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  
-----------------------------------------------------------------------------------------------------------------------
  BEGIN
    --drop temp tables
    DROP TABLE IF EXISTS REPORT_DATA;

-----------------------------------------------------------------------------------------------------------------------
    CALL DATAADMIN.SP_REPORT_REFUNDS(:startdate,:enddate,:locationid);
    
    CREATE TEMP TABLE REPORT_DATA AS
       SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    ;

-----------------------------------------------------------------------------------------------------------------------    
    --return values from the sproc with validated columns only  
     reportSet := (
       SELECT TO_CHAR(ROW_NUMBER() OVER (ORDER BY "Location ID"))  AS  "Support ID" 
          ,"Location ID"   AS "Location ID"
          ,"Location"      AS "Location"
          ,"Check"         AS "Check"
          ,"Tip Amount"    AS "Tip Amount"
          ,"Check Amount"  AS "Check Amount"
          ,"Refund Amount" AS "Refund Amount"
        FROM REPORT_DATA   REPD

     );

--=====================================================================================================================
RETURN TABLE(reportSet); 
END';