CREATE OR REPLACE PROCEDURE "SP_LOAD_365_JOBS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2025-02-19T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2025-02-19T14:48:37.661Z''; 
  -- locationid string      := ''[15]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  -- GRANT usage ON procedure dataadmin.SP_LOAD_365_JOBS(timestamp_tz,timestamp_tz,string) TO ROLE matillion;
--==================================================================================================================================
BEGIN
--==================================================================================================================================
DROP TABLE IF EXISTS TEMP_EMPS;

SELECT JOBPOSITION_DIM_FK
    FROM DATAWAREHOUSE.SHIFT_DIM SHD
    WHERE shd.fiscal_day::date
        >= dateadd(DAY,-10,:startdate)::date     --calculate data 10 days around selected dates so that  
      AND  shd.fiscal_day::date  
            <= dateadd(DAY,10,:enddate)::date    --the data in the selected range calcs overtime for full fiscal week
      AND SHD.LOCATION_DIM_FK IN ( SELECT table1.value 
            FROM table(split_to_table(:locationidS, '',''))  table1)
      AND SHD.DW_ISCURRENTROW
      AND NOT SHD.DW_ISDELETED
    GROUP BY JOBPOSITION_DIM_FK;

CREATE TEMP TABLE TEMP_EMPS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

--================================================================================================================================= 
reportSet := (  
------------------------------------------------------------------------------------------------------------------------------------
SELECT  IFNULL(REPLACE(JPD.JOBPOSITION_DIM_NK,'','',''''),''None'')  AS JobCode  --* string Number used to identify the job in R365.
 , IFNULL(REPLACE(JPD.JOB_POSITION,'','',''''),''None'')             AS JobName  --* string Name of the job.
 FROM DATAWAREHOUSE.JOBPOSITION_DIM                            JPD
   INNER JOIN TEMP_EMPS                                        EMPS
     ON  JPD.JOBPOSITION_DIM_NK = EMPS.JOBPOSITION_DIM_FK
       AND JPD.DW_ISCURRENTROW
      AND NOT JPD.IS_TRAINING
--==================================================================================================================================
);
RETURN TABLE(reportSet); 
END';