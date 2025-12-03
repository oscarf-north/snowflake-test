CREATE OR REPLACE PROCEDURE "SP_REPORT_CLOSE"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2024-08-01T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2024-08-05T14:48:37.661Z''; 
  -- locationid string      := ''[3,351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
BEGIN
 reportSet   := (
SELECT  COS.CLOSEOUTSUMMARY_FACT_NK                                            as "Support ID" 
    , ''cos-'' ||row_number() over (order by cos.CLOSEOUTSUMMARY_FACT_NK) 
                                                                               as "Detail ID"   
  --Status, categories and levels-------------------------------------------
  --Geography---------------------------------------------------------------
  ,IFNULL(loc.locationname,''None'')                                             as "Location"
  ,cos.location_dim_fk::decimal(38,0)                                          as "Location ID"
 --Payment Method----------------------------------------------------------- 
 --Dates---------------------------------------------------------------------
    ,LOC.TZ_NAME                                                               as "Time Zone"
    ,to_char(LEFT(COS.FISCAL_DATE,4))                                          as "Year"
    ,to_char(YEAR(COS.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(COS.FISCAL_DATE),2))
                                                                               as "Year and Month"                                                                             
    ,to_char(COS.FISCAL_DATE)                                                  as "Fiscal Day"
    ,IFNULL(DAYNAME(COS.FISCAL_DATE),''None'')                                   as "Day of Week"
    ,CASE WHEN DAYNAME(COS.FISCAL_DATE) IN (''Sat'',''Sun'')  
    THEN TRUE ELSE FALSE END                                                   as "Is Weekend"
    ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,COS.CREATED_AT::timestamp_ntz )::timestamp                                             
                                                                               as "Created At"
  -- --Flags----------------------------------------------------
  -- --People---------------------------------------------------
  -- --Report Specific Dimensions--------------------------------
  -- --Facts-----------------------------------------------------
    ,1::NUMBER(10,0)                                      as "Count"
    ,COS.TAX::NUMBER(18,2)                                as "Tax" 
    ,COS.TIPS::NUMBER(18,2)                               as "Tips" 
    ,COS.VOIDS::NUMBER(18,2)                              as "Voids" 
    ,COS.DISCOUNTS::NUMBER(18,2)                          as "Discounts" 
    ,COS.FEES::NUMBER(18,2)                               as "Fees" 
    ,COS.GRATUITIES::NUMBER(18,2)                         as "Gratuities"     
    ,COS.GROSS_RECEIPTS::NUMBER(18,2)                     as "Gross Receipts" 
    ,COS.GROSS_SALES::NUMBER(18,2)                        as "Gross"     
    
 ------------------------------------------------------------  
FROM datawarehouse.CLOSEOUTSUMMARY_FACT                       COS
      INNER JOIN datawarehouse.LOCATION_DIM                   LOC
        ON cos.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
          AND cos.DW_ISCURRENTROW  
          AND COS.FISCAL_DATE::date
              >= :startdate::date 
          AND COS.FISCAL_DATE::date  
              <= :enddate::date 
          AND cos.LOCATION_DIM_FK IN ( 
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1) 
ORDER BY COS.FISCAL_DATE                 

--==========================================================================================
);
RETURN TABLE(reportSet); 
END';