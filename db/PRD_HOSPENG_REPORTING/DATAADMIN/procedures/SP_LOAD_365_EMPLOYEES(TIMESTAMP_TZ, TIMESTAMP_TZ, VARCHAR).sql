CREATE OR REPLACE PROCEDURE "SP_LOAD_365_EMPLOYEES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2025-02-19T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2025-02-19T14:48:37.661Z''; 
  -- locationid string      := ''[15]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--================================================================================================================================
--ISSUES
--=================================================================================================================================
BEGIN
--=================================================================================================================================
DROP TABLE IF EXISTS TEMP_EMPS;

SELECT EMPLOYEE_DIM_FK
    FROM DATAWAREHOUSE.SHIFT_DIM SHD
    WHERE shd.fiscal_day::date
        >= dateadd(DAY,-10,:startdate)::date     --calculate data 10 days around selected dates so that  
      AND  shd.fiscal_day::date  
            <= dateadd(DAY,10,:enddate)::date    --the data in the selected range calcs overtime for full fiscal week
      AND SHD.LOCATION_DIM_FK IN ( SELECT table1.value 
            FROM table(split_to_table(:locationidS, '',''))  table1)
      AND SHD.DW_ISCURRENTROW
      AND NOT SHD.DW_ISDELETED
    GROUP BY EMPLOYEE_DIM_FK;

CREATE TEMP TABLE TEMP_EMPS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

--==================================================================================================================================
reportSet := (  
------------------------------------------------------------------------------------------------------------------------------------
SELECT EMD.EMPLOYEE_DIM_NK                                 AS EmployeeId   --* string
  ,IFNULL(REPLACE(EMD.FIRST_NAME,'','',''''),''None'')           AS FirstName    --* string
  ,IFNULL(REPLACE(EMD.LAST_NAME,'','',''''),''None'')            AS LastName     --* string
  FROM DATAWAREHOUSE.EMPLOYEE_DIM            EMD
    INNER JOIN TEMP_EMPS                     TEM
      ON TEM.EMPLOYEE_DIM_FK = EMD.EMPLOYEE_DIM_NK
        AND EMD.DW_ISCURRENTROW
--==================================================================================================================================
);
RETURN TABLE(reportSet); 
END';