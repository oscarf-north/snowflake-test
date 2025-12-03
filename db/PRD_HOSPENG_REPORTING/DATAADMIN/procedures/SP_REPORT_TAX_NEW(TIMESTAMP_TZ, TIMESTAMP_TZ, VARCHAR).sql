CREATE OR REPLACE PROCEDURE "SP_REPORT_TAX_NEW"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- =======================================================================================================
--Example Call Statement
-- CALL DATAADMIN.SP_REPORTDATAGROOM(''SP_REPORT_TAX'',2,2)
-- CALL DATAADMIN.SP_REPORT_TAX_NEW(''2000-11-20T14:48:37.661Z'',''2027-11-20T14:48:37.661Z'',''[3,2]'');
-- GRANT usage ON procedure dataadmin.SP_REPORT_TAX(timestamp_tz,timestamp_tz,string) TO ROLE DATA_REPLICATION_HOSPENG;
-- Ticket https://nabancard.atlassian.net/browse/HOSPENG-10042 deals with check tax totals that do not match the sum of the items
-- =======================================================================================================
--NOTE:  include Discount amount in Tax basis if discount flag isposttax is true else not
--========================================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2024-09-18T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2024-09-18T14:48:37.661Z''; 
  -- locationid string      := ''[2]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--========================================================================================================
BEGIN

-------------------------------------------------------------------------------------------------------------
 reportSet:= (
SELECT 
    TAX_FACT_NK                                           as "Support ID" 
    , ''TAX-'' ||row_number() over (order by TAX_FACT_NK) 
                                                          as "Detail ID"  
--Status, category, level------------------------------------------------------------------------------------
    ,IFNULL(tax.CHECKSTATUS,''None'')                       as "Check Status"
    ,IFNULL(tax.ITEMSTATUS,''None'')                        as "Item Status"
    ,IFNULL(ccd.COGSCATEGORY,''None'')                      as "Category"
-- --Geography----------------------------------------------------------------------------------------------
    ,IFNULL(org.ORGANIZATION,''None'')                      as "Organization"    
    ,IFNULL(loc.LOCATIONNAME,''None'')                      as "Location"
    ,IFNULL(tax.REVENUECENTERNAME,''None'')                 as "Revenue Center"
-- --Dates---------------------------------------------------------------------------------------------------
   ,LOC.TZ_NAME                                           as "Time Zone"
    ,to_char(LEFT(tax.FISCAL_DATE,4))                     as "Year"
    ,to_char(YEAR(tax.FISCAL_DATE)) || ''|'' 
        || TO_CHAR(LEFT(''0'' || MONTH(tax.FISCAL_DATE),2))
                                                          as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                           as "Daypart"
    
    -- ,tax.CLOSED_AT::timestamp_ntz 
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,
       tax.CLOSED_AT::timestamp_ntz )::timestamp) 
                                                          as "Closed At"
    ,to_char(tax.FISCAL_DATE)                             as "Fiscal Date"

    ,IFNULL(DAYNAME(tax.FISCAL_DATE),''None'')              as "Day of Week"
    ,CASE WHEN DAYNAME(tax.FISCAL_DATE) IN (''Sat'',''Sun'')  
       THEN TRUE ELSE FALSE END                           as "Is Weekend" 
-- --Flags---------------------------------------------------------------------------------------------------
    ,tax.IS_TAX_INCLUDED::BOOLEAN                         as "Is Tax Included"
-- --People--------------------------------------------------------------------------------------------------
-- --Descriptors---------------------------------------------------------------------------------------------- 
    ,IFNULL(tax.CHEQUENUMBER ,''None'')                     as "Check"
    ,IFNULL(med.MENUITEMNAME,'' None'')                     as "Menu Item"
    ,IFNULL(ord.ORDER_TYPE,'' None'')                       as "Order Type"
    ,IFNULL(tax.TAXRATENAME,'' None'')                      as "Rate Name"
-- --Facts-----------------------------------------------------------------------------------------------------
    ,CASE WHEN split_part(tax.tax_fact_nk, ''.'',  3)  = 0
      THEN itf.GROSS::NUMBER(18,2)  
      ELSE NULL::NUMBER(18,2) 
      END                              
                                                          as "Gross"                      
    ,CASE WHEN split_part(tax.tax_fact_nk, ''.'',  3)  = 0
      THEN itf.APPLIEDAMOUNT::NUMBER(18,2)  
      ELSE NULL::NUMBER(18,2) 
      END
                                                          as "Applied Amount"
    
    ,(CASE WHEN ABS(tax.AMOUNT - (itf.GROSS * (tax.PERCENT/100))) < 0.009
      THEN itf.GROSS 
      ELSE itf.APPLIEDAMOUNT END) ::NUMBER(18,2)          as "Tax Basis Amount"
    ,tax.AMOUNT::NUMBER(18,2)                             as "Tax Amount"    
    ,tax.PERCENT::NUMBER(18,2)                            as "Tax Percent"
----------------------------------------------------------------------------------------------------------------   
FROM DATAWAREHOUSE.TAX_FACT                               tax
     INNER JOIN DATAWAREHOUSE.ITEM_FACT                   itf
        ON itf.ITEM_FACT_NK = tax.ITEM_FACT_FK
          AND itf.TAX > 0.000
          AND itf.DW_ISCURRENTROW
          AND tax.ITEMSTATUS IN (''Added'',''Sent'')
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
      INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 loc
        ON tax.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM             med
       ON med.MENUITEMNAME_DIM_NK = tax.MENUITEMNAME_DIM_FK
         AND med.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                ord
        ON ord.ORDERTYPE_DIM_NK = tax.ORDERTYPE_DIM_FK
          AND ord.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM             org
        ON org.ORGANIZATION_DIM_NK = loc.ORGANIZATION_DIM_FK
          AND org.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.DAYPART_DIM                  dad
        ON tax.DAYPART_DIM_FK = dad.DAYPART_DIM_NK
          AND dad.DW_ISCURRENTROW         
      INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM           meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW 
    INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM               ccd
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW
order by itf.ITEM_FACT_NK, tax.tax_fact_nk
--==============================================================================================================
);
RETURN TABLE(reportSet); 
END;
-- ';