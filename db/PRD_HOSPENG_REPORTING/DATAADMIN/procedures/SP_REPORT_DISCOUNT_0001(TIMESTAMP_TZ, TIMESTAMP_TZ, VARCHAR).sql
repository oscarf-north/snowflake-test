CREATE OR REPLACE PROCEDURE "SP_REPORT_DISCOUNT_0001"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
--==========================================================================================
--Example Call Statement
--CALL DATAADMIN.SP_REPORT_DISCOUNT(''2000-11-20T14:48:37.661Z'',''2024-11-20T14:48:37.661Z'',''[351,352]'');
-- ==============================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--======================================================================================================
--TO DO
--ITEM 1:  Discounts are not broken out a the each discount level.  Ie there is a discount total at check level, and many discounts in the discount array in the discount column in the check table.
--ITEM 2:  Need to convert performed at tine to local pos time
--ITEM 3:  Need to calculate percentages wihin Perspective because of filetering
--ITEM 4:  Need to get the employee from activities
--ITEM 5:  IS DEV_HOSPENG_REPORTING.DATALANDING.POSAPI_PUBLIC_CASH_DISCOUNT A TABLE THAT WE NEED?
--ITEM 6:  Need to add discountitem_fact to this report as union
--ITEM 7:  Need to rule out training from discountitem_fact
--ITEM 8:  Need performed at for dates in activity
--NEED TO KNOW IF WE CALCULATE PERCENTAGES FROM THE VALUE FIELD -- issues with rouning?  calc rules outside database????
--======================================================================================================
BEGIN
 reportSet   := (
        SELECT CHF.DISCOUNTitem_FACT_NK             AS "Support ID" 
           , ''DISI-'' ||row_number() over (order by  CHF.DISCOUNTitem_FACT_NK ) 
                                                    AS "Detail ID"
         --Status, categories and levels----------------------------------------------------------
            ,CHF.STATUS                             AS "Status"
            ,CHF.DISCOUNTLEVEL                      AS "Discount Level"
        --geography--------------------------------------------------------------------------------
            ,IFNULL(LOC.LOCATIONNAME ,''None'')       AS "Location"
            ,IFNULL(CHF.revenueCenterName ,''None'')  AS "Revenue Center"
        --dates-------------------------------------------------------------------------------------   
            ,LOC.TZ_NAME                            AS "Time Zone"
            ,to_char(LEFT(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz )
            ,4))                                    AS "Year"
            ,to_char(LEFT(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz )
            ,7))                                    AS "Year and Month"
            ,IFNULL(dad.DAYPART,''None'')             AS "Daypart"
            ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.FISCAL_DATE::timestamp_ntz )        
                                                    AS "Fiscal Date"
            ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz )                           
                                                    AS "Added At"
            ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz)                        
                                                    AS "Created At"
            ,IFNULL(DAYNAME(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ) 
            ),''None'')                              AS "Day of Week"
            ,CASE WHEN DAYNAME(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ) 
            ) IN (''Sat'',''Sun'')  
                THEN TRUE ELSE FALSE END            AS"Is Weekend"
    
        --flags-------------------------------------------------------------------------------------
        --people------------------------------------------------------------------------------------
            ,COALESCE(EMD_ADD.EMPLOYEE_NAME,''None'') AS "Added By"
            ,COALESCE(EMD_APP.EMPLOYEE_NAME,''None'') AS "Approved By"
        --Descriptors------------------------------------------------------------------------------
            ,STD.STANDARDDISCOUNTNAME               AS "Discount Name"
            ,CHF.DISCOUNTREASON                     AS "Discount Reason"
            ,CHF.PROMOCODE                          AS "Promo Code" 
            ,CHF.CHEQUENUMBER                       AS "Check"  --davis would like hyperlink to checkdetail view in bridge
            ,STD.DISCOUNTTYPE                       AS "Discount Type"
            ,CHF.DISCOUNT_TYPE                      AS "Application"  
        --Facts-----------------------------------------------------------------------------------------
            ,1::NUMBER(10,0)                        AS "Count"   
            ,CHF.DISCOUNT_PERCENT::NUMBER(18,2)     AS "Discount Percent"   
            ,CHF.NET             ::NUMBER(18,2)     AS "Check Net Amount" 
            ,CHF.GROSS           ::NUMBER(18,2)     AS "Check Gross Amount"
            ,CHF.DISCOUNT_AMOUNT ::NUMBER(18,2)     AS "Discount Amount" 
         
                                                    --Percentages must be calculated within pyarrow cube as numerator and denominator can be filtred dynamically
            -- ,NULL                                AS "% of Total Discounts" --ex: per discount id: total count / all discount count    
            -- ,NULL                                AS "Discount % of Net Sales"  --example (total discount amount / net sales + total discount amount) * 100 
                                                    --This represents the presumed % of lost revenue
        --------------------------------------------------------------------------------------------------------
        FROM DATAADMIN.DISCOUNTITEM_FACT                      CHF
            INNER JOIN DATAADMIN.LOCATION_DIM                 LOC
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                  AND CHF.STATUS NOT IN (''Voided'',''Removed'')
                  AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz ) 
                      > CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:startdate::timestamp_ntz ) 
                  AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz ) 
                      < CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:enddate ::timestamp_ntz )
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.CHEQUESTATUS NOT IN (''Voided'')
                  AND CHF.DISCOUNT_AMOUNT > 0  --This will eliminate discounts that have been removed
                  AND CHF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED
                  AND LOC.DW_ISCURRENTROW          
                  AND NOT CHF.IS_TRAINING
            INNER JOIN DATAADMIN.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND CHF.DW_ISCURRENTROW
            INNER JOIN DATAADMIN.DAYPART_DIM                  DAD
              ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
                  AND CHF.DW_ISCURRENTROW
            INNER JOIN DATAADMIN.EMPLOYEE_DIM                 EMD_ADD
              ON EMPLOYEE_DIM_FK_AS_ADDED_BY = EMD_ADD.EMPLOYEE_DIM_NK
                AND EMD_ADD.DW_ISCURRENTROW
            LEFT JOIN DATAADMIN.EMPLOYEE_DIM                 EMD_APP
              ON EMPLOYEE_DIM_FK_AS_APPROVED_BY = EMD_APP.EMPLOYEE_DIM_NK
                AND EMD_APP.DW_ISCURRENTROW
UNION ALL

        SELECT CHF.DISCOUNTCHECK_FACT_NK            AS "Support ID" 
                   ,''DISC-'' || row_number() over (order by  CHF.DISCOUNTCHECK_FACT_NK ) 
                                                    AS "Detail ID"
         --Status, categories and levels----------------------------------------------------------
            ,CHF.STATUS                             AS "Status"
            ,CHF.DISCOUNTLEVEL                      AS "Discount Level"
        --geography--------------------------------------------------------------------------------
            ,IFNULL(LOC.LOCATIONNAME ,''None'')       AS "Location"
            ,IFNULL(CHF.revenueCenterName ,''None'')  AS "Revenue Center"
        --dates-------------------------------------------------------------------------------------  
            ,LOC.TZ_NAME                            AS "Time Zone"        
            ,to_char(LEFT(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ) 
            ,4))                                    AS "Year"
            ,to_char(LEFT(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ) 
            ,7))                                    AS "Year and Month"
            ,IFNULL(dad.DAYPART,''None'')             AS "Daypart"
            ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.FISCAL_DATE::timestamp_ntz )                         
                                                    AS "Fiscal Date"
            ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz )                            
                                                    AS "Added At"
            ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz )                          
                                                    AS "Created At"
            ,IFNULL(DAYNAME(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ) 
            )
            ,''None'')AS"Day of Week"
            ,CASE WHEN DAYNAME(
            CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.ADDED_AT::timestamp_ntz ) 
            ) IN (''Sat'',''Sun'')  
            THEN TRUE ELSE FALSE END                AS"Is Weekend"
        --flags-------------------------------------------------------------------------------------
        --people------------------------------------------------------------------------------------
            ,COALESCE(EMD_ADD.EMPLOYEE_NAME,''None'') AS "Added By"
            ,COALESCE(EMD_APP.EMPLOYEE_NAME,''None'') AS "proved By" 
        --Descriptors------------------------------------------------------------------------------
            ,STD.STANDARDDISCOUNTNAME               AS "Discount Name"
            ,CHF.DISCOUNTREASON                     AS "Discount Reason"
            ,CHF.PROMOCODE                          AS "Promo Code" 
            ,CHF.CHEQUENUMBER                       AS "Check"  --davis would like hyperlink to checkdetail view in bridge
            ,STD.DISCOUNTTYPE                       AS "Discount Type"
            ,CHF.DISCOUNT_TYPE                      AS "Application"  
        --Facts-----------------------------------------------------------------------------------------
            ,1                   ::NUMBER(10,0)     AS "Count"          
            ,CHF.DISCOUNT_PERCENT::NUMBER(18,2)     AS "Discount Percent"   
            ,CHF.NET             ::NUMBER(18,2)     AS "Check Net Amount" 
            ,CHF.GROSS           ::NUMBER(18,2)     AS "Check Gross Amount"
            ,CHF.DISCOUNT_AMOUNT ::NUMBER(18,2)     AS "Discount Amount" 
         
                                                    --Percentages must be calculated within pyarrow cube as numerator and denominator can be filtred dynamically
            -- ,NULL                                AS "% of Total Discounts" --ex: per discount id: total count / all discount count    
            -- ,NULL                                AS "Discount % of Net Sales"  --example (total discount amount / net sales + total discount amount) * 100 
                                                    --This represents the presumed % of lost revenue
        --------------------------------------------------------------------------------------------------------
        FROM DATAADMIN.DISCOUNTCHECK_FACT                     CHF
            INNER JOIN DATAADMIN.LOCATION_DIM                 LOC
              ON CHF.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                  AND CHF.STATUS NOT IN (''Voided'',''Removed'')
                  AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz ) 
                      > CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:startdate ::timestamp_ntz )
                  AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHF.CREATED_AT::timestamp_ntz ) 
                      < CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:enddate ::timestamp_ntz )
                  AND CHF.LOCATION_DIM_FK IN (
                     SELECT table1.value 
                       FROM table(split_to_table(:locationidS, '',''))  table1)
                  AND CHF.CHEQUESTATUS NOT IN (''Voided'')
                  AND CHF.DISCOUNT_AMOUNT > 0  --This will eliminate discounts that have been removed
                  AND CHF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED
                  AND LOC.DW_ISCURRENTROW          
                  AND NOT CHF.IS_TRAINING
            INNER JOIN DATAADMIN.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND CHF.DW_ISCURRENTROW
            INNER JOIN DATAADMIN.DAYPART_DIM                  DAD
              ON CHF.DAYPART_DIM_FK = DAD.DAYPART_DIM_NK
                  AND CHF.DW_ISCURRENTROW
            INNER JOIN DATAADMIN.EMPLOYEE_DIM                 EMD_ADD
              ON EMPLOYEE_DIM_FK_AS_ADDED_BY = EMD_ADD.EMPLOYEE_DIM_NK
                AND EMD_ADD.DW_ISCURRENTROW
            LEFT JOIN DATAADMIN.EMPLOYEE_DIM                 EMD_APP
              ON EMPLOYEE_DIM_FK_AS_APPROVED_BY = EMD_APP.EMPLOYEE_DIM_NK
                AND EMD_APP.DW_ISCURRENTROW
                
--==========================================================================================
); 
 RETURN TABLE(reportSet); 
END;
';