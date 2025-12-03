CREATE OR REPLACE PROCEDURE "SP_LOAD_DATASHARE_ITEMS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- -- =====================================================================================
-- Example Call Statement
-- CALL DATAWAREHOUSE.SP_LOAD_DATASHARE_ITEMS(''2000-11-20T14:48:37.661Z'',''2026-11-20T14:48:37.661Z'',''[351]'');
-- GRANT usage ON procedure DATAWAREHOUSE.SP_LOAD_DATASHARE_ITEMS(timestamp_tz,timestamp_tz,string) TO ROLE DATA_REPLICATION_HOSPENG;
-- --=========================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
 reportSet:= (
SELECT itf.ITEM_FACT_NK                                   as "Support ID" 
--status, category, level-------------------------------------------------------------------
    ,IFNULL(ccd.COGSCATEGORY,''None'')                      as "Category Name"
    ,IFNULL(ccd.cogscategory_dim_nk,0)                    as "Category ID"
-- --geography--------------------------------------------------------------------------------
    ,IFNULL(loc.LOCATIONNAME,''None'')                      as "Location Name"
    ,IFNULL(loc.LOCATION_DIM_NK,''None'')                   as "Location ID"
    ,IFNULL(itf.REVENUECENTERNAME,''None'')                 as "Revenue Center Name"
-- --dates-------------------------------------------------------------------------------------
    ,LOC.TZ_NAME                                          as "Time Zone"
    ,to_char(LEFT(chk.FISCAL_DATE,4))                     as "Year"
    ,to_char(YEAR(chk.FISCAL_DATE)) || ''|'' || TO_CHAR(LEFT(''0'' || MONTH(chk.FISCAL_DATE),2))                                       
                                                          as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                           as "Daypart"
    ,to_char(chk.FISCAL_DATE)                             as "Fiscal Date"
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,chk.OPENED_AT::timestamp_ntz )::timestamp )                                                                           
                                                          as "Opened At"                                                           
    ,IFNULL(DAYNAME(chk.FISCAL_DATE),''None'')              as "Day of Week"
    ,CASE WHEN DAYNAME(chk.FISCAL_DATE) IN (''Sat'',''Sun'')  
             THEN TRUE ELSE FALSE END                     as "Is Weekend"
-- --flags--------------------------------------------------------------------------------------
    ,IFNULL(itf.HASMODIFIERS,FALSE)                       as "Has Modifiers"
-- --Descriptors-------------------------------------------------------------------------------- 
    ,IFNULL(itf.chequenumber ,''None'')                     as "Check"
    ,IFNULL(itf.CHEQUE_FACT_FK ,''None'')                   as "Check ID"    
    ,IFNULL(med.MENUITEMNAME,'' None'')                     as "Item Name"
    ,IFNULL(med.MENUITEMNAME,'' None'')                     as "Item Code"
    ,IFNULL(itf.COMBINEDNAME,''Regular'')                   as "Variant Name"     
-- --Facts-----------------------------------------------------------------------------------------
    ,IFNULL(itf.QUANTITY::NUMBER(10,0),0)                 as "Count"   
    ,IFNULL(itf.APPLIEDAMOUNT::NUMBER(18,2),0)            as "Net Amount"
    ,IFNULL(itf.GROSS::NUMBER(18,2),0)                    as "Gross Amount" 
    ,IFNULL(itf.BASEPRICE::NUMBER(18,2),0)                as "Base Price"
    ,IFNULL(itf.PRICE::NUMBER(18,2),0)                    as "Price"
    ,IFNULL(dis."Item Discount Amount"::NUMBER(18,2),0)   as "Item Discount Amount"
    ,IFNULL(itf.INCLUSIVETAX::NUMBER(18,2),0)             as "Inclusive Tax Amount"    
    ,IFNULL(itf.TAX::NUMBER(18,2),0)                      as "Tax Amount"
--------------------------------------------------------------------------------------------   
FROM DATAWAREHOUSE.ITEM_FACT                              itf
     INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM            med
       ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND itf.CHECKSTATUS = ''Closed''
          AND itf.DW_ISCURRENTROW  
          AND med.DW_ISCURRENTROW  
          AND NOT itf.DW_ISDELETED
          AND NOT itf.IS_TRAINING
          AND itf.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                                chk
        ON chk.CHEQUE_FACT_NK = itf.CHEQUE_FACT_FK
          AND chk.DW_ISCURRENTROW
          AND chk.STATUS = ''Closed''
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND (chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date  <= :enddate::date)
      INNER JOIN DATAWAREHOUSE.LOCATION_DIM                     loc
        ON itf.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.DAYPART_DIM                      dad
        ON itf.daypart_dim_fk = dad.daypart_dim_nk
          AND dad.DW_ISCURRENTROW = TRUE         
      INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM               meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW = TRUE
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                  ccd
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW
      LEFT JOIN (
          SELECT daf.ITEM_FACT_FK, SUM(daf.APPLIED_AMOUNT) AS "Item Discount Amount"
              FROM DATAWAREHOUSE.DISCOUNTITEM_FACT daf
                WHERE daf.DW_ISCURRENTROW 
                  AND (daf.FISCAL_DATE::date >= :startdate::date 
                  AND daf.FISCAL_DATE::date  <= :enddate::date)
                  AND daf.LOCATION_DIM_FK IN (--351,352
                     SELECT table1.value 
                        FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND NOT daf.IS_TRAINING
                GROUP BY daf.ITEM_FACT_FK
                                                          )dis 
            ON dis.ITEM_FACT_FK = itf.ITEM_FACT_NK
          
--==========================================================================================
);
RETURN TABLE(reportSet); 
END;
-- ';