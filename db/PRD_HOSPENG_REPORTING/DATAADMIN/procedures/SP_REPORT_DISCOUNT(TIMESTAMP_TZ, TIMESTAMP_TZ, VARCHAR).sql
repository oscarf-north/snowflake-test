CREATE OR REPLACE PROCEDURE "SP_REPORT_DISCOUNT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-09-18'';  
  -- enddate timestamp_tz   := ''2024-09-18''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--======================================================================================================
BEGIN
 reportSet   := (
        SELECT CHF.DISCOUNTitem_FACT_NK                      AS "Support ID" 
           , ''DISI-'' ||row_number() over (order by  CHF.DISCOUNTitem_FACT_NK ) 
                                                             AS "Detail ID"
         --Status, categories and levels----------------------------------------------------------
            ,CHF.STATUS                                      AS "Status" 
            ,CHF.DISCOUNTLEVEL                               AS "Discount Level"
        --geography--------------------------------------------------------------------------------
            ,IFNULL(LOC.LOCATIONNAME ,''None'')                AS "Location"
            ,CHF.LOCATION_DIM_FK::DECIMAL(36,0)              AS "Location ID"
            ,IFNULL(CHF.revenueCenterName ,''None'')           AS "Revenue Center"
        --dates-------------------------------------------------------------------------------------  
        
            ,LOC.TZ_NAME                                     AS "Time Zone"
            ,to_char(LEFT(CHF.FISCAL_DATE,4))                AS "Year"
            ,to_char(YEAR(CHF.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(CHF.FISCAL_DATE),2))
                                                             AS "Year and Month"
            ,IFNULL(dad.DAYPART,''None'')                      AS "Daypart"
            ,to_char(CHF.FISCAL_DATE)                        AS "Fiscal Date"
            ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ))                           
                                                             AS "Added At"

            ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz ))                                             
  
                                                             AS "Created At"
            ,IFNULL(DAYNAME(CHF.FISCAL_DATE),''None'') 
                                                             AS "Day of Week"
            ,CASE WHEN DAYNAME(CHF.FISCAL_DATE) IN (''Sat'',''Sun'')  
                THEN TRUE ELSE FALSE END                     AS "Is Weekend"
    
        --flags-------------------------------------------------------------------------------------
        --people------------------------------------------------------------------------------------
            ,COALESCE(EMD_ADD.EMPLOYEE_NAME,''None'')          AS "Added By"
            ,COALESCE(EMD_APP.EMPLOYEE_NAME,''None'')          AS "Approved By"
        --Descriptors------------------------------------------------------------------------------
            ,STD.STANDARDDISCOUNTNAME                        AS "Discount Name"
            ,CHF.DISCOUNTREASON                              AS "Discount Reason"
            ,ORD.ORDER_TYPE                                  AS "Order Type"
            ,CHF.PROMOCODE                                   AS "Promo Code" 
            ,CHF.CHEQUENUMBER                                AS "Check"  
            ,CHF.CHEQUE_FACT_FK                              AS "Check ID"
            ,STD.DISCOUNTTYPE                                AS "Discount Type"
            ,CHF.DISCOUNT_TYPE                               AS "Application"  
        --Facts-----------------------------------------------------------------------------------------
            ,1::NUMBER(10,0)                                 AS "Count"   
            ,CHF.DISCOUNT_PERCENT::NUMBER(18,2)              AS "Discount Percent"   
            ,CHF.NET             ::NUMBER(18,2)              AS "Check Net Amount" 
            ,CHF.GROSS           ::NUMBER(18,2)              AS "Check Gross Amount"
            ,itf.APPLIEDAMOUNT   ::NUMBER(18,2)              AS "Discount Net Sales"
            ,CHF.APPLIED_AMOUNT  ::NUMBER(18,2)              AS "Discount Amount" 
         
                                                    --Percentages must be calculated within pyarrow cube as numerator and denominator can be filtred dynamically
            -- ,NULL                                AS "% of Total Discounts" --ex: per discount id: total count / all discount count    
            -- ,NULL                                AS "Discount % of Net Sales"  --example (total discount amount / net sales + total discount amount) * 100 
                                                    --This represents the presumed % of lost revenue
        --------------------------------------------------------------------------------------------------------
        FROM DATAWAREHOUSE.DISCOUNTITEM_FACT                  CHF
            INNER JOIN DATAWAREHOUSE.ITEM_FACT                ITF
              ON CHF.ITEM_FACT_FK = ITF.ITEM_FACT_NK
                  AND ITF.ITEMSTATUS IN (''Added'',''Sent'')
                  AND NOT CHF.STATUS  = ''Disabled''
                  AND CHF.CHEQUESTATUS  IN (''Closed'')
                  AND IFNULL(CHF.APPLIED_AMOUNT,0.00) > 0.00
                  -- AND CHF.DISCOUNTREASON <> ''CASHDISCOUNT''
                  AND CHF.FISCAL_DATE::date
                      >= :startdate::date
                  AND CHF.FISCAL_DATE::date   
                      <= :enddate ::date 
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.DW_ISCURRENTROW
                  AND ITF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED                
                  AND NOT CHF.IS_TRAINING
            INNER JOIN DATAWAREHOUSE.LOCATION_DIM            LOC  
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                 AND LOC.DW_ISCURRENTROW  
            INNER JOIN DATAWAREHOUSE.STANDARDDISCOUNT_DIM    STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW
            INNER JOIN DATAWAREHOUSE.DAYPART_DIM             DAD
              ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
                  AND DAD.DW_ISCURRENTROW
            INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM           ORD
              ON ITF.ORDERTYPE_DIM_FK = ORD.ORDERTYPE_DIM_NK
                AND ORD.DW_ISCURRENTROW            
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM             EMD_ADD
              ON EMPLOYEE_DIM_FK_AS_ADDED_BY = EMD_ADD.EMPLOYEE_DIM_NK
                AND EMD_ADD.DW_ISCURRENTROW
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM            EMD_APP
              ON EMPLOYEE_DIM_FK_AS_APPROVED_BY = EMD_APP.EMPLOYEE_DIM_NK
                AND EMD_APP.DW_ISCURRENTROW
UNION ALL

        SELECT CHF.DISCOUNTCHECK_FACT_NK                    AS "Support ID" 
                   ,''DISC-'' || row_number() over (order by  CHF.DISCOUNTCHECK_FACT_NK ) 
                                                            AS "Detail ID"
         --Status, categories and levels----------------------------------------------------------
            ,CHF.STATUS                                     AS "Status"
            ,CHF.DISCOUNTLEVEL                              AS "Discount Level"
        --geography--------------------------------------------------------------------------------
            ,IFNULL(LOC.LOCATIONNAME ,''None'')               AS "Location"
            ,CHF.LOCATION_DIM_FK::DECIMAL(36,0)             AS "Location ID"
            ,IFNULL(CHF.revenueCenterName ,''None'')          AS "Revenue Center"
        --dates-------------------------------------------------------------------------------------  
            ,LOC.TZ_NAME                                    AS "Time Zone"        
            ,to_char(LEFT( CHF.FISCAL_DATE,4))              AS "Year"
            ,to_char(YEAR(CHF.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(CHF.FISCAL_DATE),2))
                                                            AS "Year and Month"
            ,IFNULL(dad.DAYPART,''None'')                     AS "Daypart"
            ,to_char(CHF.FISCAL_DATE)                      
                                                            AS "Fiscal Date"
            ,to_char(CHF.ADDED_AT::timestamp_ntz)                            
                                                            AS "Added At"
            ,to_char(CHF.CREATED_AT::timestamp_ntz)                          
                                                            AS "Created At"
            ,IFNULL(DAYNAME(
            CHF.FISCAL_DATE 
            )
            ,''None'')                                        AS "Day of Week"
            ,CASE WHEN DAYNAME(
            CHF.FISCAL_DATE 
            ) IN (''Sat'',''Sun'')  
            THEN TRUE ELSE FALSE END                        AS "Is Weekend"
        --flags-------------------------------------------------------------------------------------
        --people------------------------------------------------------------------------------------
            ,COALESCE(EMD_ADD.EMPLOYEE_NAME,''None'')         AS "Added By"
            ,COALESCE(EMD_APP.EMPLOYEE_NAME,''None'')         AS "Approved By" 
        --Descriptors------------------------------------------------------------------------------
            ,STD.STANDARDDISCOUNTNAME                       AS "Discount Name"
            ,CHF.DISCOUNTREASON                             AS "Discount Reason"
            ,ORD.ORDER_TYPE                                 AS "Order Type"            
            ,CHF.PROMOCODE                                  AS "Promo Code" 
            ,CHF.CHEQUENUMBER                               AS "Check" 
            ,CHF.CHEQUE_FACT_FK                             AS "Check ID"
            ,STD.DISCOUNTTYPE                               AS "Discount Type"
            ,CHF.DISCOUNT_TYPE                              AS "Application"  
        --Facts-----------------------------------------------------------------------------------------
            ,1                           ::NUMBER(10,0)     AS "Count"          
            ,CHF.DISCOUNT_PERCENT        ::NUMBER(18,2)     AS "Discount Percent"   
            ,CHF.NET                     ::NUMBER(18,2)     AS "Check Net Amount" 
            ,CHF.GROSS                   ::NUMBER(18,2)     AS "Check Gross Amount"
            ,(CHF.GROSS - CHF.DISCOUNT)  ::NUMBER(18,2)     AS "Discount Net Sales"
            ,CHF.APPLIED_AMOUNT          ::NUMBER(18,2)     AS "Discount Amount" 
         
                                                    --Percentages must be calculated within pyarrow cube as numerator and denominator can be filtred dynamically
            -- ,NULL                                AS "% of Total Discounts" --ex: per discount id: total count / all discount count    
            -- ,NULL                                AS "Discount % of Net Sales"  --example (total discount amount / net sales + total discount amount) * 100 
                                                    --This represents the presumed % of lost revenue
        --------------------------------------------------------------------------------------------------------
        FROM DATAWAREHOUSE.DISCOUNTCHECK_FACT                CHF
            INNER JOIN DATAWAREHOUSE.LOCATION_DIM            LOC
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                AND IFNULL(CHF.APPLIED_AMOUNT,0.00) > 0.00
                  AND CHF.FISCAL_DATE::date
                      >= :startdate::date
                  AND CHF.FISCAL_DATE::date   
                      <= :enddate ::date 
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.CHEQUESTATUS IN (''Closed'')
                  AND NOT CHF.STATUS = ''Disabled''
                  AND CHF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED
                  AND LOC.DW_ISCURRENTROW          
                  AND NOT CHF.IS_TRAINING
            INNER JOIN DATAWAREHOUSE.DAYPART_DIM                 DAD
              ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
                  AND DAD.DW_ISCURRENTROW 
            INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                 CF 
                ON CHF.CHEQUE_FACT_FK = CF.CHEQUE_FACT_NK
                AND CF.DW_ISCURRENTROW     
            INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM               ORD
              ON CF.ORDERTYPE_DIM_FK = ORD.ORDERTYPE_DIM_NK
                AND ORD.DW_ISCURRENTROW                  
            LEFT JOIN DATAWAREHOUSE.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                 EMD_ADD
              ON EMPLOYEE_DIM_FK_AS_ADDED_BY = EMD_ADD.EMPLOYEE_DIM_NK
                AND EMD_ADD.DW_ISCURRENTROW
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                 EMD_APP
              ON EMPLOYEE_DIM_FK_AS_APPROVED_BY = EMD_APP.EMPLOYEE_DIM_NK
                AND EMD_APP.DW_ISCURRENTROW  

--==========================================================================================
); 
 RETURN TABLE(reportSet); 
END';