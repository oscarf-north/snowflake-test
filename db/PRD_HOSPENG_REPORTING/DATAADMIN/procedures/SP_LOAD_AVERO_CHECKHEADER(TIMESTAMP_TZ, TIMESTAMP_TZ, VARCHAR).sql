CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_CHECKHEADER"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=================================================================================================================================
BEGIN
--=================================================================================================================================
reportSet := (  
----------------------------------------------------------------------------------------------------------------------------------
SELECT CHF.FISCAL_DATE    AS BUSDATE --Business Day Date as YYYYMMDD 8 digits(99991231)fiscal day of check20110101
,CHF.REVENUECENTER_DIM_FK AS RVCNUM -- Revenue Center Number Number 12 digits(999999999999)Unique identifier for the revenuecenter 
,CHF.REVENUECENTERNAME    AS RVCDESC --Revenue CenterDescriptionString 40 characters Description of the revenue center
,CHF.CHEQUE_FACT_NK       AS CHKSEQ --Unique check sequence identifier String
,CHF.CHEQUENUMBER         AS CHKNUM --Check number Number
,CHF.EMPLOYEE_DIM_FK      AS EMPNUM -- Employee Number Number 12 digit(999999999999)Unique identifier for check owner
,EMD.EMPLOYEE_NAME        AS EMPDESC --Employee Description String 40 characters First and Last Name of check owning employee
,SHD.JOBPOSITION_DIM_FK   AS EMPCLASS --NUM Employee Class Number Number 12 digit (999999999999)Employee class number
,JPD.JOB_POSITION         AS EMPCLASSDESC   --Employee ClassDescription string 40 characters Class Description of employee
,TO_CHAR(TO_DATE(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.OPENED_AT::timestamp_ntz )),''YYYYMMDD'')
                          AS CHKOPENDAY --Check Open Day YYYYMMDD 8 digits(99991231)Actual calendar date check opened 20110101
,TO_CHAR(TO_TIMESTAMP(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.OPENED_AT::timestamp_ntz )),''HH24:MI:SS'')
                          AS CHKOPENTIME --Check Open Time Time as HH24:MM 23:59 Time the check was opened, inlocal time11:10
,TO_CHAR(TO_DATE(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CLOSED_AT::timestamp_ntz )),''YYYYMMDD'')
                          AS CHKCLOSEDAY  --Check Close Day Date as YYYYMMDD 8 digits(99991231)calendar date check closed20110101
,TO_CHAR(TO_TIMESTAMP(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CLOSED_AT::timestamp_ntz )),''HH24:MI:SS'')
                          AS CHKCLOSETIME --Check Close Time Time as HH24:MM 23:59 Time the check was closed. inlocal time13:40
,CHF.PARTY_COUNT::NUMBER(12,0) AS COVERS  --Count Number 12 digits(999999999999)Number of covers, or guests, onthe check.4
FROM DATAWAREHOUSE.CHEQUE_FACT          CHF
  INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM EMD
    ON EMD.EMPLOYEE_DIM_NK = CHF.EMPLOYEE_DIM_FK
      AND CHF.DW_ISCURRENTROW
      AND EMD.DW_ISCURRENTROW
      AND CHF.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      AND NOT CHF.IS_TRAINING
      AND CHF.DW_ISCURRENTROW
      AND CHF.STATUS in (''Closed'')
          AND (CHF.FISCAL_DATE::date >= :startdate::date 
                AND CHF.FISCAL_DATE::date  <= :enddate::date)
  INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 LOC
    ON LOC.LOCATION_DIM_NK = CHF.LOCATION_DIM_fK
      AND CHF.DW_ISCURRENTROW
      AND LOC.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.SHIFT_DIM                    SHD
    ON SHD.SHIFT_DIM_NK = CHF.SHIFT_DIM_FK
      AND SHD.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.JOBPOSITION_DIM              JPD
    ON JPD.JOBPOSITION_DIM_NK = SHD.JOBPOSITION_DIM_FK
      AND JPD.DW_ISCURRENTROW
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';