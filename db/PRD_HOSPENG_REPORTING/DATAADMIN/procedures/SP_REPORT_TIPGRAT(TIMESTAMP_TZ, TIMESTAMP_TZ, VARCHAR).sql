CREATE OR REPLACE PROCEDURE "SP_REPORT_TIPGRAT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- ========================================================================================
--Example Call Statement
-- CALL DATAADMIN.SP_REPORTDATAGROOM(''SP_REPORT_TIPGRAT'',365,351);--------------------
-- CALL DATAADMIN.SP_REPORT_TIPGRAT(''2000-11-20T14:48:37.661Z'',''2027-11-20T14:48:37.661Z'',''[351,400,403,352,501,357]'');
-- GRANT usage ON procedure dataadmin.SP_REPORT_TIPGRAT(timestamp_tz,timestamp_tz,string) TO ROLE DATA_REPLICATION_HOSPENG;
-- ========================================================fis=================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string        :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
 reportSet:= (
SELECT CHEQUE_FACT_NK                                        as "Support ID" 
    , ''TIG-'' ||row_number() over (order by CHEQUE_FACT_NK) 
                                                          as "Detail ID"  
--status, category, level-------------------------------------------------------------------
    -- ,IFNULL(chk.CHECKSTATUS,''None'')                       as "Check Status"
    -- ,IFNULL(chk.ITEMSTATUS,''None'')                        as "Item Status"
    -- ,IFNULL(meg.REPORTCATEGORY,''None'')                 as "Category"
-- --geography-------------------------------------------------------------------------------- 
    ,IFNULL(loc.LOCATIONNAME,''None'')                      as "Location"
    ,IFNULL(chk.REVENUECENTERNAME,''None'')                 as "Revenue Center"
-- --dates-------------------------------------------------------------------------------------
    ,LOC.TZ_NAME                                          as "Time Zone"
    ,to_char(LEFT(
    chk.CLOSED_AT::timestamp_ntz 
    ,4))                                                  as "Year"
    ,to_char(LEFT(
    chk.CLOSED_AT::timestamp_ntz 
    ,7))                                                  as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                           as "Daypart"
    ,chk.CLOSED_AT::timestamp_ntz 
                                                          as "Closed At"
    ,chk.FISCAL_DATE::date 
                                                          as "Fiscal Date"
    ,IFNULL(DAYNAME(
    chk.CLOSED_AT::timestamp_ntz 
    ),'' None'') 
                                                          as "Day of Week"
    ,CASE WHEN DAYNAME(
    chk.CLOSED_AT::timestamp_ntz 
    ) IN (''Sat'',''Sun'')  
    THEN TRUE ELSE FALSE END                              as "Is Weekend"
    ,YEAR(chk.CLOSED_AT) || ''- Week '' || WEEKOFYEAR(chk.CLOSED_AT)    as "Year and Week" 
-- --flags--------------------------------------------------------------------------------------
-- --people-------------------------------------------------------------------------------------
    ,emd.EMPLOYEE_NAME
-- --Descriptors-------------------------------------------------------------------------------- 
    ,IFNULL(chk.chequenumber ,''None'')                     as "Check"
-- --Facts-----------------------------------------------------------------------------------------
    ,1::NUMBER(18,0)                                      as "Count"
    ,IFNULL(chk.tip,0)::NUMBER(18,2)                      as "Tip Amount"
    ,IFNULL(chk.GRATUITIES,0)::NUMBER(18,2)               as "Gratuity Amount"
--------------------------------------------------------------------------------------------   
FROM DATAADMIN.CHEQUE_FACT                                chk
      INNER JOIN DATAADMIN.LOCATION_DIM                   loc
        ON chk.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
          AND chk.STATUS = ''Closed''
          AND chk.OPENED_AT is not null
          AND chk.DW_ISCURRENTROW  
          AND NOT chk.DW_ISDELETED
          AND NOT chk.IS_TRAINING
          AND chk.CLOSED_AT::timestamp_ntz 
              > :startdate::timestamp_ntz 
          AND chk.CLOSED_AT::timestamp_ntz 
              < :enddate::timestamp_ntz 
          AND chk.LOCATION_DIM_FK IN ( 
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAADMIN.DAYPART_DIM                      dad
        ON chk.daypart_dim_fk = dad.daypart_dim_pk
          AND dad.DW_ISCURRENTROW = TRUE
      INNER JOIN EMPLOYEE_DIM                               emd
        ON emd.EMPLOYEE_DIM_NK = chk.EMPLOYEE_DIM_FK
          AND emd.DW_ISCURRENTROW
      
--==========================================================================================
);
RETURN TABLE(reportSet); 
END;
';