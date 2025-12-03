CREATE OR REPLACE PROCEDURE "SP_LOAD_365_LABORDETAIL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--================================================================================================================================
--ISSUES
--No column in layout for Fiscal Day this should be Business Day
--No column in layout for Overtime Hours
--Should this be only hourly employees?
--=================================================================================================================================
BEGIN
--=================================================================================================================================
CALL DATAADMIN.SP_REPORT_LABOR(:startdate,:enddate,:locationid);

reportSet := (  
------------------------------------------------------------------------------------------------------------------------------------
SELECT  REPLACE(IFNULL(NULL,"Employee ID"),'','','' '')                  AS "EmployeeNumber" --*string  --employee id in R365.
-- ,TO_NUMBER(REPLACE(LRS."Shift ID",''Shift '',''''))                      AS "ShiftNumber"    --*intege ra single business 
,ROW_NUMBER() over (PARTITION BY "Employee ID" ORDER BY LRS."Shift ID")                   
                                                                   AS "ShiftNumber"    --*intege ra single business 
,TO_CHAR(TO_TIMESTAMP(LRS."Clocked In At"), ''MM/DD/YYYY'')            AS "ClockInDate"    --*DateTime(mm/dd/yyyy hh:mm:ss)
,TO_CHAR(TO_TIMESTAMP(LRS."Clocked In At"), ''MM/DD/YYYY HH24:MI:SS'') AS "ClockInTime"    --*DateTime(mm/dd/yyyy hh:mm:ss)
,TO_CHAR(TO_TIMESTAMP(LRS."Clocked Out At"),''MM/DD/YYYY HH24:MI:SS'') AS "ClockOutTime"   --*DateTime(mm/dd/yyyy hh:mm:ss)
,REPLACE(IFNULL(LRS."Job Position",''None''),'','','' '')                  AS "JobName"        --string--Name of the job that 
,LRS."Regular Rate"::NUMBER(18,2)                                    AS "PayRate"        --decimal
,(LRS."Regular Seconds"/60/60)::NUMBER(18,6)                         AS "RegularHours"   --decimal
--------------------------------------
  --NOT   ,"Employee"                AS "Employee"
  --ADDED ,"Job Position"            AS "Job Title" --should this be only hourly employees???????  if custom--add tips and
  -- ,"Regular Seconds"              AS "Hours"    ---is this an integer?  int.dec  same as ''OVertime''
  --ADDED  ,"Regular Rate"           AS "Pay Rate"
  --NOT    , NULL                    AS "Total"    ----what is this?  
  --NOT    ,"Fiscal Day"             AS "Date"
  --ADDED  ,"Clocked In At"          AS "Start DateTime"
  --ADDED  ,"Clocked Out At"         AS "End DateTime"
  --       ,"Overtime Seconds"       AS "Overtime" ------intger?  hours?  same as ''Hours''
  --ADDED  ,"Shift ID"               AS "POS ID"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LRS 
  WHERE "Detail ID" not like (''TGR%'')  --don''t include gratuity totals from stored proceedure
    AND "Pay Basis" = ''hourly''
    AND LRS."Clocked Out At" IS NOT NULL
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';