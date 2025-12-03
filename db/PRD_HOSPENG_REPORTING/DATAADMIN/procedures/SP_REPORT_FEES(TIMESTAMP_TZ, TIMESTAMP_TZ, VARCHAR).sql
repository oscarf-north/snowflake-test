CREATE OR REPLACE PROCEDURE "SP_REPORT_FEES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-22T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2025-08-22T14:48:37.661Z''; 
  -- locationid string      := ''[2,351]'';
  locationidS string        :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=============================================================================================
BEGIN
 reportSet:= (
SELECT SURCHARGE_FACT_NK                                                       as "Support ID" 
    , ''FEE-'' ||row_number() over (order by surcharge_FACT_NK) 
                                                                               as "Detail ID"  
--status, category, level-----------------------------------------------------------------------
    ,IFNULL(chk.STATUS,''None'')                                                 as "Check Status"
-- --geography----------------------------------------------------------------------------------
    ,IFNULL(loc.LOCATIONNAME,''None'')                                           as "Location"
    ,loc.LOCATION_DIM_NK                                                       as "Location ID"    
-- --dates---------------------------------------------------------------------------------------
   ,LOC.TZ_NAME                                                                as "Time Zone"
    ,to_char(LEFT(CHK.FISCAL_DATE,4))                                          as "Year"
    ,to_char(YEAR(CHK.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(CHK.FISCAL_DATE),2))
                                                                               as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                                                as "Daypart"
    
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,chk.CLOSED_AT::timestamp_ntz )::timestamp) 
                                                                               as "Closed At"
    ,to_char(chk.FISCAL_DATE)                                                  as "Fiscal Date"

    ,IFNULL(DAYNAME(chk.FISCAL_DATE),''None'')                                   as "Day of Week"
    ,CASE WHEN DAYNAME(chk.FISCAL_DATE) IN (''Sat'',''Sun'')  
       THEN TRUE ELSE FALSE END                                                as "Is Weekend"
   --people-------------------------------------------------------------------------------------
    ,IFNULL(emd.EMPLOYEE_NAME,''None'')                                          as "Employee"
-- --flags--------------------------------------------------------------------------------------
   ,SUF.IS_TAXABLE                                                             as "Is Taxable"
-- --Descriptors-------------------------------------------------------------------------------- 
    ,IFNULL(SUD.SURCHARGE, ''None'')                                             as "Fee"   
    ,IFNULL(CHK.chequenumber ,''None'')                                          as "Check"
    ,SUF.SURCHARGE_TYPE                                                        as "Type"       
 
-- --Facts--------------------------------------------------------------------------------------
    ,1::NUMBER(18,0)                                                           as "Count"
    ,SUF.APPLIEDAMOUNT::NUMBER(18,2)                                           as "Total"
------------------------------------------------------------------------------------------------  
FROM DATAADMIN.CHEQUE_FACT                           CHK
      INNER JOIN DATAADMIN.SURCHARGE_FACT            SUF
          ON SUF.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_NK
             AND CHK.DW_ISCURRENTROW
             AND SUF.DW_ISCURRENTROW
             AND NOT CHK.DW_ISDELETED
             AND NOT CHK.IS_TRAINING
             AND CHK.STATUS = ''Closed''
             -- AND NOT SUF.SURCHARGE_TYPE = ''None''
             AND NOT SUF.STATUS = ''Disabled''
             AND NOT SUF.IS_GRATUITY
             AND CHK.FISCAL_DATE::date
                >= :startdate::date 
            AND CHK.FISCAL_DATE::date  
                <= :enddate::date 
            AND CHK.LOCATION_DIM_FK IN ( 
               SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM            EMD
        ON EMD.EMPLOYEE_DIM_NK= CHK.EMPLOYEE_DIM_FK
          AND EMD.DW_ISCURRENTROW                           
      INNER JOIN DATAWAREHOUSE.LOCATION_DIM            LOC
        ON CHK.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.DAYPART_DIM             DAD
        ON CHK.daypart_dim_fk = dad.daypart_dim_Nk
          AND dad.DW_ISCURRENTROW = TRUE  
      INNER JOIN DATAWAREHOUSE.SURCHARGE_DIM           SUD
        ON SUF.SURCHARGE_DIM_NK = SUD.SURCHARGE_DIM_NK
          AND SUD.DW_ISCURRENTROW

   
--=============================================================================================
);
RETURN TABLE(reportSet); 
END';