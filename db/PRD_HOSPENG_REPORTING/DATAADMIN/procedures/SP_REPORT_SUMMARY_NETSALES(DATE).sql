CREATE OR REPLACE PROCEDURE "SP_REPORT_SUMMARY_NETSALES"("DAY" DATE)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- =====================================================================================
--Example Call Statement
-- CALL DATAADMIN.SP_REPORT_SUMMARY_NETSALES(''2024-06-11'');
-- GRANT usage ON procedure dataadmin.SP_REPORT_SUMMARY_NETSALES(date) TO ROLE HOSPENG_ADMIN;
-- --=========================================================================================
DECLARE 
  reportSet resultset;
  -- day date := ''2024-06-11'';

--=========================================================================================
BEGIN
      DROP TABLE IF EXISTS TEMP_SUM;
      DROP TABLE IF EXISTS TEMP_CAT;
      DROP TABLE IF EXISTS TEMP_DISCHECK;
      DROP TABLE IF EXISTS TEMP_DISITEM;
      DROP TABLE IF EXISTS TEMP_PAY;
      DROP TABLE IF EXISTS TEMP_FISCALDATE;

      SELECT PAY.CHEQUE_FACT_FK
       ,MAX(PAY.FISCALDATE) AS "Fiscal Date"
          FROM PAYMENTS_FACT PAY
            WHERE PAY.DW_ISCURRENTROW
               AND PAY.FISCALDATE = :day--
       GROUP BY CHEQUE_FACT_FK
     ;

      CREATE TEMP TABLE TEMP_FISCALDATE AS
      SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
      
      SELECT  MAX("Fiscal Date") AS "Fiscal Date"
          ,SUM("Net Sales")      AS "Net Sales"
          ,SUM("Service Charge") AS "Service Charge"
          ,SUM("Tax Collected")  AS "Tax Collected"
          ,SUM("Net Sales") + SUM("Service Charge") + SUM("Tax Collected")  as "Total Revenue"

          ,SUM("Item Discounts")  AS "Item Discounts"
          ,SUM("Check Discounts") AS "Check Discounts"
          ,SUM("Total Discounts") AS "Total Discounts"      
      FROM (
        SELECT DATE(FSD."Fiscal Date")                                                  AS "Fiscal Date"
           ,IFNULL(CHK.NET::DECIMAL(18,2),0)  - IFNULL(CHK.SURCHARGE::DECIMAL(18,2),0)  AS "Net Sales"
           ,IFNULL(CHK.SURCHARGE::DECIMAL(18,2),0)                                      AS "Service Charge"
           ,IFNULL(CHK.TAX::DECIMAL(18,2),0)                                            AS "Tax Collected"
           --------------------------------
           ,CHK.DISCOUNTITEM                                             AS "Item Discounts"
           ,CHK.DISCOUNTCHECK                                            AS "Check Discounts"
           ,CHK.DISCOUNT                                                 AS "Total Discounts"
        
          FROM DATAADMIN.CHEQUE_FACT  CHK
            INNER JOIN TEMP_FISCALDATE                            fsd
            ON fsd.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_nK
               AND "Fiscal Date" = :day--DATE(''2024-06-11'')--:day
               AND location_dim_fk = 2
               AND CHK.DW_ISCURRENTROW
               AND NOT CHK.DW_ISDELETED
               AND NOT CHK.IS_TRAINING
               AND CHK.STATUS = ''Closed''
               -- AND DATE(CHK.CLOSED_AT) = DATE(''2024-06-11'')--:day--
        ) GROUP BY "Fiscal Date" ;

         CREATE TEMP TABLE TEMP_SUM AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

        ---------------------------------------------------------
        SELECT IFNULL(meg.REPORTCATEGORY,''None'')                  as "Category"
           ,itf.net  AS "NET"
        FROM DATAADMIN.ITEM_FACT                              itf
         INNER JOIN DATAADMIN.MENUITEMNAME_DIM                med
           ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
              AND itf.ITEMSTATUS IN (''Added'',''Sent'')
              AND itf.CHECKSTATUS = ''Closed''
              AND itf.OPENED_AT is not null
              AND itf.DW_ISCURRENTROW  
              AND med.DW_ISCURRENTROW  
              AND NOT itf.DW_ISDELETED
              AND NOT itf.IS_TRAINING
              AND itf.LOCATION_DIM_FK = 2 
              --AND DATE(itf.closed_at) =  DATE(''2024-06-11'')--:day--
          INNER JOIN DATAADMIN.REPORTCATEGORY_DIM               meg
            ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
              AND med.DW_ISCURRENTROW = TRUE
          INNER JOIN TEMP_FISCALDATE                            fsd
            ON fsd.CHEQUE_FACT_FK = itf.CHEQUE_FACT_FK
               AND "Fiscal Date" = :day--DATE(''2024-06-11'')--DATE(''2024-06-11'')--:day--
           ;

         CREATE TEMP TABLE TEMP_CAT AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    --------------------------------------------------------------------------------------
SELECT CHF.CHEQUENUMBER
    ,CHF.CHEQUE_FACT_FK
    ,CHF.APPLIED_AMOUNT  ::NUMBER(18,2)      AS "Discount Amount" 
    ,CHK.DISCOUNTCHECK
    ,SUM(CHF.APPLIED_AMOUNT) OVER (PARTITION BY 1) AS TOT_DET
    ,SUM(CHK.DISCOUNTCHECK) OVER (PARTITION BY 1) AS TOT_SUM    
    ,CHF.MTLN_CDC_SEQUENCE_NUMBER
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER
    ,STD.STANDARDDISCOUNTNAME AS "Discount Name"
    
  FROM DATAADMIN.DISCOUNTCHECK_FACT                     CHF
    INNER JOIN DATAADMIN.CHEQUE_FACT                  CHK
      ON CHF.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_NK
         AND CHF.LOCATION_DIM_FK = 2
         -- AND DATE(CHK.CLOSED_AT) =  DATE(''2024-06-11'')--:day--
         AND CHF.DW_ISCURRENTROW
         AND NOT CHF.DW_ISDELETED
         AND NOT CHF.IS_TRAINING
         AND CHK.DW_ISCURRENTROW
         AND NOT CHK.DW_ISDELETED
         AND NOT CHK.IS_TRAINING
         AND CHK.STATUS = ''Closed''
    INNER JOIN TEMP_FISCALDATE                            fsd
            ON fsd.CHEQUE_FACT_FK = CHf.CHEQUE_FACT_FK
              AND "Fiscal Date" = :day--DATE(''2024-06-11'')--         DATE(''2024-06-11'')--
    INNER JOIN DATAADMIN.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW    
                 ;
                 
    CREATE TEMP TABLE TEMP_DISCHECK AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
-------------------------------------------------------
SELECT CHF.CHEQUENUMBER
    ,CHF.CHEQUE_FACT_FK
    ,CHF.APPLIED_AMOUNT  ::NUMBER(18,2)      AS "Discount Amount" 
    ,CHK.DISCOUNTCHECK
    ,SUM(CHF.APPLIED_AMOUNT) OVER (PARTITION BY 1) AS TOT_DET
    ,SUM(CHK.DISCOUNTCHECK) OVER (PARTITION BY 1) AS TOT_SUM    
    ,CHF.MTLN_CDC_SEQUENCE_NUMBER
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER
    ,STD.STANDARDDISCOUNTNAME AS "Discount Name"
    
FROM DATAADMIN.DISCOUNTITEM_FACT                     CHF
    INNER JOIN DATAADMIN.CHEQUE_FACT                  CHK
      ON CHF.CHEQUE_FACT_FK = CHK.CHEQUE_FACT_NK
         AND CHF.LOCATION_DIM_FK = 2
         -- AND DATE(CHK.CLOSED_AT) =  DATE(''2024-06-11'')--:day--
         AND CHF.DW_ISCURRENTROW
         AND NOT CHF.DW_ISDELETED
         AND NOT CHF.IS_TRAINING
         AND CHK.DW_ISCURRENTROW
         AND NOT CHK.DW_ISDELETED
         AND NOT CHK.IS_TRAINING
         -- AND CHK.STATUS = ''Closed''
    INNER JOIN TEMP_FISCALDATE                            fsd
            ON fsd.CHEQUE_FACT_FK = CHf.CHEQUE_FACT_FK
              AND "Fiscal Date" = :day--DATE(''2024-06-11'')--         
    INNER JOIN DATAADMIN.STANDARDDISCOUNT_DIM         STD
               ON STD.STANDARDDISCOUNT_DIM_NK = CHF.STANDARDDISCOUNT_DIM_FK
                 AND STD.DW_ISCURRENTROW    
                 ;
                 
    CREATE TEMP TABLE TEMP_DISITEM AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));       
------------------------------------------------------------- 

SELECT CHK.CHEQUE_FACT_NK
   ,PAY.CHEQUENUMBER
   ,PAD.PAYMENTMETHODNAME
   ,OTD.ORDER_TYPE  AS "Order Type"
   ,pay.cardbrand   AS "Card Brand" 
   ,PAY.AMOUNTAPPLIEDTOCHECK
   ,CASE WHEN PAD.PAYMENTMETHODNAME = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK::DECIMAL(18,2) else pay.TOTAL::DECIMAL(18,2) end as "Amount"

FROM DATAADMIN.CHEQUE_FACT  CHK
  INNER JOIN PAYMENTS_FACT  PAY
     ON CHK.CHEQUE_FACT_NK = PAY.CHEQUE_FACT_FK
       AND PAY.DW_ISCURRENTROW
       AND CHK.location_dim_fk = 2
       AND CHK.DW_ISCURRENTROW
       AND NOT CHK.DW_ISDELETED
       AND NOT CHK.IS_TRAINING
       AND CHK.STATUS = ''Closed''
       AND PAY.PAYMENTSTATUS = ''Success''
       AND date(pay.fiscaldate) = :day  --DATE(''2024-06-11'')--''2024-06-11''
       --AND DATE(CHK.CLOSED_AT) = :day--
    INNER JOIN PAYMENTMETHOD_DIM PAD
      ON PAY.PAYMENTMETHOD_DIM_FK = PAD.PAYMENTMETHOD_DIM_NK
        AND PAD.DW_ISCURRENTROW 
    INNER JOIN ORDERTYPE_DIM   OTD
      ON OTD.ORDERTYPE_DIM_NK = CHK.ORDERTYPE_DIM_FK
        AND OTD.DW_ISCURRENTROW
        ;
        
CREATE TEMP TABLE TEMP_PAY AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));          
-------------------------------------------------------------         
 reportSet:= ( 
      SELECT " ","  " from (
            
        SELECT 0 as "Order",''Fiscal Date '' || "Fiscal Date"::DATE as " " , NULL as "  " FROM TEMP_SUM
        
        UNION
        SELECT 1 as "Order",''Net Sales'' as " "  ,"Net Sales"::DECIMAL(18,2)  as "  " FROM TEMP_SUM
        
        UNION
        SELECT 2 as "Order",''Service Charge'' as " "  ,"Service Charge"::DECIMAL(18,2) FROM TEMP_SUM
        
        UNION
        SELECT 3 as "Order",''Tax Collected'' as " "  ,"Tax Collected"::DECIMAL(18,2) FROM TEMP_SUM
        
        UNION
        SELECT 4 as "Order",''Total Revenue'' as " "  ,"Total Revenue"::DECIMAL(18,2) FROM TEMP_SUM
        
         UNION
        SELECT 5 as "Order",''  '' as " "  , NULL 

        UNION
        SELECT 6 as "Order",''Item Discounts'' as " "  ,"Item Discounts"::DECIMAL(18,2) FROM TEMP_SUM
        
        UNION
        SELECT 7 as "Order",''Check Discounts'' as " "  ,"Check Discounts"::DECIMAL(18,2) FROM TEMP_SUM
        
        UNION
        SELECT 8 as "Order",''Total Discounts'' as " "  , "Total Discounts"::DECIMAL(18,2) FROM TEMP_SUM

        UNION
        SELECT 9 as "Order",''  '' as " "  , NULL 
----------------
        UNION
        SELECT  10  ,"Order Type", sum("Amount")::DECIMAL(18,2) from TEMP_pay    group by "Order Type"
        
        UNION
        SELECT  11  ,''Total'', sum("Amount")::DECIMAL(18,2) from TEMP_pay 
  -----------------------      
        
        UNION
        SELECT 12 as "Order",''  '' as " "  , NULL 

        UNION 
        SELECT 13 as "Order", "Category" ,SUM("NET")::DECIMAL(18,2) FROM TEMP_CAT GROUP BY "Category"
          
        UNION 
        SELECT 14 as "Order",''Total''    ,SUM("NET")::DECIMAL(18,2) FROM TEMP_CAT

        UNION
        SELECT 15 ,''   ''   , NULL 
        
        --DISCOUNTS
        UNION
        SELECT  16  ,"Discount Name", sum("Discount Amount") from TEMP_DISCHECK group by "Discount Name"
        
        UNION
        SELECT  17  ,''Total'', sum("Discount Amount") from TEMP_DISCHECK

        UNION
        SELECT  18  ,"Discount Name", sum("Discount Amount") from TEMP_DISITEM group by "Discount Name"
        
        UNION
        SELECT  19  ,''Total'', sum("Discount Amount") from TEMP_DISITEM
        
        -- --- CC PAYMENTS
        UNION
        SELECT 20 ,''   ''   , NULL 
        
        UNION
        SELECT  21  ,"Card Brand", sum("Amount")::DECIMAL(18,2) from TEMP_pay where PAYMENTMETHODNAME = ''EPX''  group by "Card Brand"
        
        UNION
        SELECT  22  ,''Total With AMEX'', sum("Amount")::DECIMAL(18,2) from TEMP_pay where PAYMENTMETHODNAME = ''EPX''
        
        UNION
        SELECT  23  ,''Total Without AMEX'', sum("Amount")::DECIMAL(18,2) from TEMP_pay where PAYMENTMETHODNAME = ''EPX'' and "Card Brand" <> ''American Express''

        UNION
        SELECT 24 ,''   ''   , NULL 
        
        UNION
        SELECT  25  ,''Cash Total'', sum("Amount") from TEMP_pay where PAYMENTMETHODNAME = ''Cash''
        
        ) order by "Order"
      );

RETURN TABLE(reportSet); 
END;
-- ';