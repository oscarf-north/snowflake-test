CREATE OR REPLACE PROCEDURE "SP_REPORT_PMIX_NEW"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,2,3,4]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
BEGIN  

DROP TABLE IF EXISTS TEMP_TAXBASIS;

--=========================================================================================
--get tax basis - can be multiple taxes per item - select the max tax basis for that item 
CREATE TABLE TEMP_TAXBASIS AS
SELECT tax.ITEM_FACT_FK
   ,MAX(CASE WHEN ABS(tax.AMOUNT - (itf.GROSS * (tax.PERCENT/100))) < 0.009
      THEN itf.GROSS 
      ELSE itf.APPLIEDAMOUNT END) ::NUMBER(18,2)          as "Tax Basis Amount"
FROM DATAWAREHOUSE.TAX_FACT                               tax
     INNER JOIN DATAWAREHOUSE.ITEM_FACT                   itf
        ON itf.ITEM_FACT_NK = tax.ITEM_FACT_FK
          AND itf.TAX > 0.000
          AND itf.DW_ISCURRENTROW
    WHERE tax.ITEMSTATUS IN (''Added'',''Sent'')
          AND tax.CHECKSTATUS = ''Closed''
          AND tax.OPENED_AT is not null
          AND tax.DW_ISCURRENTROW  
          AND NOT tax.DW_ISDELETED
          AND NOT tax.IS_TRAINING
          AND tax.FISCAL_DATE::date
              >= :startdate::date 
          AND tax.FISCAL_DATE::date  
              <= :enddate::date 
          AND tax.LOCATION_DIM_FK IN ( 
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
  GROUP BY tax.ITEM_FACT_FK
;

--=========================================================================================
 reportSet:= (
SELECT itf.ITEM_FACT_NK                                   as "Support ID" 
    , ''PMIX-'' ||row_number() over (order by itf.ITEM_FACT_NK) 
                                                          as "Detail ID"
--status, category, level-------------------------------------------------------------------
    ,IFNULL(itf.CHECKSTATUS,''None'')                       as "Check Status"
    ,IFNULL(itf.ITEMSTATUS,''None'')                        as "Item Status"
    ,IFNULL(itf.STATUSREASON,''None'')                      as "Status Reason"
    ,IFNULL(ccd.COGSCATEGORY,''None'')                      as "Category"
    -- ,IFNULL(ccd.COGSCATEGORY,''None'')                   as "COGS Category"
-- --geography
    ,IFNULL(loc.LOCATIONNAME,''None'')                      as "Location"
    ,itf.LOCATION_DIM_FK::DECIMAL(36,0)                   as "Location ID"
    ,IFNULL(itf.REVENUECENTERNAME,''None'')                 as "Revenue Center"
-- --dates-------------------------------------------------------------------------------------
    ,LOC.TZ_NAME                                          as "Time Zone"
    ,to_char(LEFT(chk.FISCAL_DATE,4))                     as "Year"
    ,to_char(YEAR(chk.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(chk.FISCAL_DATE),2))                                       
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
-- --people-------------------------------------------------------------------------------------
    ,IFNULL(emd.EMPLOYEE_NAME,''None'')                     as "Employee"
-- --Descriptors-------------------------------------------------------------------------------- 
    ,IFNULL(itf.chequenumber ,''None'')                     as "Check"
    ,IFNULL(itf.cheque_fact_fk::decimal(10,0)::string ,''None'')                   as "Check ID"    
    ,IFNULL(med.MENUITEMNAME,'' None'')                     as "Menu Item"
    ,IFNULL(itf.NOTE,''None'')                              as "Note"
    ,IFNULL(itf.DESCRIPTION,''None'')                       as "Description"
    ,IFNULL(itf.COMBINEDNAME,''Regular'')                   as "Variant"     
    ,itf.SPLITBY::NUMBER(18,0)                            as "Split By"
-- --Facts-----------------------------------------------------------------------------------------
    ,itf.QUANTITY::NUMBER(10,0)                           as "Count"
    ,itf.REPORTQUANTITY::NUMBER(18,2)                     as "Report Quantity"   
    ,itf.APPLIEDAMOUNT::NUMBER(18,2)                      as "Applied Amount"
    ,itf.BASEPRICE::NUMBER(18,2)                          as "Base Price"
    ,itf.PRICE::NUMBER(18,2)                              as "Price"
    ,itf.GROSS::NUMBER(18,2)                              as "Gross" 
    ,dis."Item Discount Amount"::NUMBER(18,2)             as "Item Discount Amount"
    ,(IFNULL(itf.GROSS,0) - IFNULL(dis."Item Discount Amount",0))::NUMBER(18,2)                      
                                                          as "Net"  
    ,itf.INCLUSIVETAX::NUMBER(18,2)                       as "Inclusive Tax"    
    ,itf.TAX::NUMBER(18,2)                                as "Tax"
    ,ttb."Tax Basis Amount"                               as "Tax Basis Amount"
    ,itf.TOTAL::NUMBER(18,2)                              as "Total" 
--------------------------------------------------------------------------------------------   
FROM DATAWAREHOUSE.ITEM_FACT                                  itf
     INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                med
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
      INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM                     emd
        ON itf.employee_dim_fk = emd.employee_dim_Nk
          AND emd.DW_ISCURRENTROW 
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
      LEFT JOIN TEMP_TAXBASIS                              ttb
            ON  ttb.item_fact_fk = itf.item_fact_nk
          
--==========================================================================================
);
RETURN TABLE(reportSet); 
END';