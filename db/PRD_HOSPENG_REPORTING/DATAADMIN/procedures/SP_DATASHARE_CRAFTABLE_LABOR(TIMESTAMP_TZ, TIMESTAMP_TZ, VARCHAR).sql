CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_LABOR"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
--   startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';  
--   enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z''; 
--   locationid string           := ''[35]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  
-----------------------------------------------------------------------------------------------------------------------
  BEGIN
    --drop temp tables
    DROP TABLE IF EXISTS REPORT_DATA;

-----------------------------------------------------------------------------------------------------------------------
    CALL DATAADMIN.SP_REPORT_LABOR(:startdate,:enddate,:locationid);
    
    CREATE TEMP TABLE REPORT_DATA AS
       SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    ;

-----------------------------------------------------------------------------------------------------------------------    
    --return values from the sproc with validated columns only  
     reportSet := (
        SELECT 
             ORG.ORGANIZATION                                            AS "Organization Name"
            ,ORG.ORGANIZATION_DIM_NK                                     AS "Organization ID"
            ,REPD."Location"                                             AS "Location Name"
            ,REPD."Location ID"                                          AS "Location ID"
            ,REPD."Fiscal Day"                                           AS "Business Day"
            ,REPD."Shift ID"                                             AS "Shift ID"
            ,REPD."Clocked In At"                                        AS "Time In"
            ,REPD."Clocked Out At"                                       AS "Time Out"
            ,REPD."Job Category"                                         AS "Department"
            ,REPD."Job Position"                                         AS "Job Title"
            ,REPD."Employee ID"                                          AS "Employee No"
            ,REPD."Employee"                                             AS "Employee Name"
            ,(IFNULL(REPD."Regular Seconds",0)/(60*60))::DECIMAL(36,4)   AS "Regular Hours"
            ,(IFNULL(REPD."Overtime Seconds",0)/(60*60))::DECIMAL(36,4)  AS "Overtime Hours"            
            ,REPD."Regular Rate" ::DECIMAL(36,2)                         AS "Hourly wage"   
            ,IFNULL(REPD."Break Seconds",0)::DECIMAL(36,2)               AS "Break Seconds"
        FROM REPORT_DATA                                                 REPD
        LEFT JOIN DATAWAREHOUSE.ORGANIZATION_DIM                         ORG
            ON REPD."Location ID"  = ORG.ORGANIZATION_DIM_NK
            AND ORG.DW_ISCURRENTROW
        WHERE NOT "Detail ID" LIKE (''TGR%'')
     );

--=====================================================================================================================
RETURN TABLE(reportSet); 
END';