CREATE OR REPLACE PROCEDURE "SP_LOAD_AVERO_CHECKDETAIL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
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
SELECT ITF.ITEM_FACT_PK   AS CHROWNUM     --Row number   
,TO_CHAR(CHF.FISCAL_DATE,''YYYYMMDD'') 
                          AS BUSDATE      --Business Day Date as YYYYMMDD 8 digits(99991231)fiscal day of check20
,CHF.REVENUECENTER_DIM_FK AS RVCNUM       --Revenue center number
,CHF.REVENUECENTERNAME AS RVCDESC         --Revenue center desription
,CHF.CHEQUE_FACT_NK       AS CHKSEQ       --Unique check sequence identifier String
,CHF.CHEQUENUMBER         AS CHKNUM       --CHEQUE NUMBER
,CHF.EMPLOYEE_DIM_FK      AS EMPNUM       --Employee Number Number 12 digit(999999999999)Unique identifier for check owner
,EMD.EMPLOYEE_NAME        AS EMPDESC      --Employee Description String 40 characters First and Last Name of check owning employee
,SHD.JOBPOSITION_DIM_FK   AS EMPCLASS     --NUM Employee Class Number Number 12 digit (999999999999)Employee class number
,JPD.JOB_POSITION         AS EMPCLASSDESC --Employee ClassDescription string 40 characters Class Description of employ
,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.OPENED_AT::timestamp_ntz ),''YYYYMMDD'' )     
                          AS TRANSDATE    --Transaction day
,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.OPENED_AT::timestamp_ntz ),''HH:MM'' )     
                          AS TRANSTIME    --Transaction TIME                                                         
-- --???????????????????????????????????????????????
,NULL AS RECTYPE --Record type
,NULL AS NUM     --RECTYPE Number-
,NULL AS "DESC"  --RECTYPE Description
-- --???????????????????????????????????????????????
,ITF.QUANTITY             AS QTY--Quantity
,ITF.APPLIEDAMOUNT        AS AMT--RECtYPE amt
FROM DATAWAREHOUSE.CHEQUE_FACT                           CHF
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
  INNER JOIN DATAWAREHOUSE.ITEM_FACT                     ITF
    ON ITF.CHEQUE_FACT_FK = CHF.CHEQUE_FACT_NK
      AND ITF.DW_ISCURRENTROW
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