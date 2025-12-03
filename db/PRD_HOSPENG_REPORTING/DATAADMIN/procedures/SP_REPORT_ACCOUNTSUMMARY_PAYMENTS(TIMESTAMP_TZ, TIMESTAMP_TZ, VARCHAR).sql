CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet           resultset;
  -- startdate string    := ''2020-07-10'';  
  -- enddate string      := ''2029-07-10''; 
  -- locationid string   := ''[351]'';
  locationidS string  :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_GCPURCHASE;
  DROP TABLE IF EXISTS TEMP_PMIX;
  DROP TABLE IF EXISTS TEMP_CHECK;
  DROP TABLE IF EXISTS TEMP_GIFTCARDS;  
  DROP TABLE IF EXISTS TEMP_TAXDETAILSFEE;
  DROP TABLE IF EXISTS TEMP_TAXDETAILSTENDER;
  DROP TABLE IF EXISTS TEMP_REFUNDS;  
  DROP TABLE IF EXISTS TEMP_FEES;
  DROP TABLE IF EXISTS TEMP_PAYINOUT;

  DROP TABLE IF EXISTS TEMP_TABLE_PAYMENTS_1AA; 
  DROP TABLE IF EXISTS TEMP_TABLE_PAYMENTS_1AB;
  DROP TABLE IF EXISTS TEMP_TABLE_PAYMENTS_1AC;
  DROP TABLE IF EXISTS TEMP_TABLE_PAYMENTS_1A;   
--=========================================================================================

CREATE TEMP TABLE TEMP_PAYINOUT  AS
 SELECT ''PayInOut''                                                            as "Level"   
     ,SHD.FISCAL_DAY                                                          as "Fiscal Date"
    ,CASE WHEN PAR.IS_PAY_IN THEN ''Pay In'' ELSE ''Pay Out''END                  as "Type"  
    ,SHD.LOCATION_DIM_FK::decimal(36,0)                                       as "Location ID"  
    ,CASE WHEN PMD.PAYMENTMETHODTYPE IN (''CC'',''EPX'') 
       THEN ''Credit Card'' ELSE PMD.PAYMENTMETHODTYPE  END                                                             
                                                                              as "Group 1B"
    ,CASE WHEN PMD.PAYMENTMETHODTYPE IN (''CC'',''EPX'')  
             THEN CCT.CARD_TYPE
        ELSE ''None'' END                                                       as "Group 2B"  
    ,1                                                                        as "CountPIO"
    ,IFNULL(PMD.PAYMENTMETHODTYPE ,''None'')                                    as "Payment Type"
    ,IFNULL(PMD.PAYMENTMETHODTYPE ,''None'')                                    as "Payment Method"
    ,IFNULL(CCT.CARD_TYPE,''None'')                                             as "Card Brand"   
    ,SUM(1::NUMBER(18,0))                                                     as "Count"
    ,SUM(PIO.AMOUNT * CASE WHEN PAR.IS_PAY_IN THEN 1 ELSE -1 END
          ) ::DECIMAL(36,2)                                                   as "Amount" 
       FROM DATAWAREHOUSE.PAYINOUT_FACT                                       PIO  
            INNER JOIN DATAWAREHOUSE.SHIFT_DIM                                SHD
              ON PIO.SHIFT_DIM_FK = SHD.SHIFT_DIM_NK
                  AND SHD.DW_ISCURRENTROW
                  AND PIO.DW_ISCURRENTROW
                  AND NOT PIO.DW_ISDELETED
                  AND NOT PIO.IS_VOID
                  AND PIO.STATUS = ''Success''
                  AND SHD.FISCAL_DAY::date >= :startdate::date 
                  AND SHD.FISCAL_DAY::date <= :enddate::date  
                  AND SHD.LOCATION_DIM_FK in (
                    SELECT table1.value 
                      FROM table(split_to_table(:locationidS, '',''))  table1) 
            INNER JOIN DATAWAREHOUSE.PAYMENTMETHOD_DIM                         PMD
              ON PMD.PAYMENTMETHOD_DIM_NK = PIO.PAYMENTMETHOD_DIM_FK
                  AND PMD.DW_ISCURRENTROW
            INNER JOIN DATAWAREHOUSE.PAYINPAYOUTREASON_DIM                     PAR
              ON PAR.PAYINPAYOUTREASON_DIM_NK = PIO.PAYINPAYOUTREASON_DIM_FK
                  AND PAR.DW_ISCURRENTROW                
            LEFT JOIN DATAWAREHOUSE.CCTRANSACTION_FACT                         CCT
              ON CCT.CCTRANSACTION_FACT_NK = PIO.CCTRANSATION_FACT_FK
                  AND CCT.DW_ISCURRENTROW
            GROUP BY "Level" 
                ,"Type"
                ,"Group 1B"
                ,"Group 2B"
                ,"Fiscal Date"
                ,"Location ID"  
                ,"Payment Method"
                ,"Payment Type"
                ,"Card Brand"  ;

-------------------------------------------------------------------------------------------
 CREATE TEMP TABLE TEMP_REFUNDS
    AS
 SELECT ''Refunds''                                                       AS "Level"
    ,''Refunds''                                                          AS "Group 1C"
    ,''Refunds''                                                          AS "Group 2C"    
    ,CHK.FISCAL_DATE                                                    AS "Fiscal Date"
    ,IFNULL(PMD.PAYMENTMETHODTYPE ,''None'')                              AS "Payment Type"
    ,IFNULL(PMD.PAYMENTMETHODTYPE ,''None'')                              AS "Payment Method"
    ,ref.LOCATION_DIM_FK                                                AS "Location ID"
    ,1                                                                  AS "Count"
    ,ref.REFUND_AMOUNT::DECIMAL(18,2)                                   AS "Refund Amount"  
        FROM DATAWAREHOUSE.REFUNDS_FACT                                 ref
          INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                          chk
            ON chk.CHEQUE_FACT_NK = ref.CHEQUE_FACT_FK
              AND chk.DW_ISCURRENTROW
              AND NOT chk.dw_isdeleted
              AND chk.STATUS = ''Closed''
              AND ref.PAYMENTSTATUS = ''Success''
              AND (chk.FISCAL_DATE::date >= :startdate::date 
              AND chk.FISCAL_DATE::date  <= :enddate::date)
                              AND ref.opened_at::timestamp_ntz  >= :startdate::timestamp_ntz 
              AND ref.LOCATION_DIM_FK in (
                    SELECT table1.value 
                      FROM table(split_to_table(:locationidS, '',''))  table1)                               
              AND ref.DW_ISCURRENTROW
              AND NOT ref.IS_TRAINING
              AND NOT ref.dw_isdeleted
            INNER JOIN DATAWAREHOUSE.PAYMENTMETHOD_DIM                  pmd
              ON pmd.PAYMENTMETHOD_DIM_NK = ref.PAYMENTMETHOD_DIM_FK
                AND pmd.DW_ISCURRENTROW
;         

--===========================================================================================
CREATE TEMP TABLE TEMP_GCPURCHASE AS
  SELECT CHF.FISCAL_DATE   AS  FISCAL_DATE
      ,CHF.LOCATION_DIM_FK AS  LOCATION_DIM_FK
      ,GCF.CHEQUE_FACT_FK  AS  CHEQUE_FACT_FK
      ,SUM(GCF.AMOUNT)     AS "Gift Card Purchase Amount"
     FROM DATAWAREHOUSE.GIFTCARDTRANSACTION_FACT                       GCF  
        INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                           CHF
            ON CHF.CHEQUE_FACT_NK = GCF.CHEQUE_FACT_FK
                AND CHF.STATUS = ''Closed''
                AND GCF.COMMAND = ''Issue''
                AND CHF.DW_ISCURRENTROW
                AND GCF.DW_ISCURRENTROW
                AND NOT GCF.DW_ISDELETED
                AND CHF.FISCAL_DATE::date >= :startdate::date 
                AND CHF.FISCAL_DATE::date <= :enddate::date  
                AND CHF.LOCATION_DIM_FK in (
                  SELECT table1.value 
                     FROM table(split_to_table(:locationidS, '',''))  table1)                 
     GROUP BY CHF.FISCAL_DATE
       ,CHF.LOCATION_DIM_FK
       ,GCF.CHEQUE_FACT_FK;

-- -------------------------------------------------------------------------------------
CREATE TEMP TABLE TEMP_CHECK AS 
SELECT ''Cheque''                                                         as "Level"   
    ,pay.fiscaldate                                                     as "Fiscal Date"
    ,loc.location_dim_nk::decimal(36,0)                                 as "Location ID"  
    ,CASE WHEN PAYMENTTYPE IN (''CC'',''EPX'') 
       THEN ''Credit Card'' ELSE PAYMENTTYPE  END                                                             
                                                                        as "Group 1B"
    ,CASE WHEN PAYMENTTYPE IN (''CC'',''EPX'') 
             THEN pay.cardbrand
        ELSE  ptd.paymentmethodname END                                 as "Group 2B"    
    ,PAYMENTTYPE                                                        as "Payment Type"    
    ,IFNULL(ptd.paymentmethodname ,''None'')                              as "Payment Method"

    ,IFNULL(case pay.cardbrand when '''' 
             THEN ''Not a Credit Card'' 
            else pay.cardbrand end,''None'')  
                                                                        as "Card Brand" 
-------------------------------------------------------------------------------------
    ,ROW_NUMBER() OVER (PARTITION BY CHF.CHEQUE_FACT_NK 
        ORDER BY PAY.PAYMENTS_FACT_NK)                                  as "Check Count"
    ,CHF.CHEQUE_FACT_NK                                                 as CHEQUE_FACT_NK
    ,PAY.PAYMENTS_FACT_NK                                               as PAYMENTS_FACT_NK
    ,SUM(IFNULL(CHF.TAX       ,0.00))::DECIMAL(36,2)                    as "Taxes"
    ,SUM(IFNULL(CHF.SURCHARGE ,0.00))::DECIMAL(36,2)                    as "Surcharges"
    ,SUM(IFNULL(CHF.UNPAID    ,0.00))::DECIMAL(36,2)                    as "Unpaid"
    ,SUM(IFNULL(CHF.GRATUITIES,0.00))::DECIMAL(36,2)                    as "Gratuities"    
    ,SUM(IFNULL(CHF.FEES      ,0.00))::DECIMAL(36,2)                    as "Fees"    
    ,SUM(CASE WHEN  ptd.paymentmethodname ILIKE ''%Event Deposit%''
      THEN CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK 
                  ELSE pay.TOTAL END ELSE 0 END        
        - IFNULL(pay.Tip,0))                       ::DECIMAL(36,2)      as "Deposits"
    ,SUM(IFNULL(GCF."Gift Card Purchase Amount",0.00)) ::DECIMAL(36,2)  as "Gift Card Sales"
    ,SUM(IFNULL(CHF.TIP       ,0.00))::DECIMAL(36,2)                    as "Tips2"           
    ,SUM(0.00)::DECIMAL(36,2)                                           as "Pay In/Out"
    ,SUM(0.00)::DECIMAL(36,2)                                           as "Deposit Applied"
    ,SUM(IFNULL(CHF.net,0) - IFNULL(CHF.tax,0) - IFNULL(CHF.SURCHARGE,0) - IFNULL(GCF."Gift Card Purchase Amount",0)
    -- - CASE WHEN  ptd.paymentmethodname ILIKE ''%Event Deposit%''
    --   THEN CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK 
    --               ELSE pay.TOTAL END ELSE 0 END 
    
    )::DECIMAL(36,2) 
                                                                        as "Net Sales" 
    -- ,SUM(tpr."Applied Amount")::DECIMAL(36,2)                        as "Net Sates from Item"
-------------------------------------------------------------------------------------
    ,SUM(1::NUMBER(36,0))                                               as "Count"
    ,SUM(IFNULL(pay.Tip,0))::DECIMAL(36,2)                              as "Tips" 
    ,(SUM( CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK 
                  ELSE pay.TOTAL END   )    
        - SUM(IFNULL(pay.Tip,0))
        
        - SUM(IFNULL(CASE WHEN  ptd.paymentmethodname ILIKE ''%Event Deposit%''
      THEN CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK 
                  ELSE pay.TOTAL END ELSE 0 END,0)
        
        ))::DECIMAL(18,2)          
                                                                       as "Sales"       
 
    ,SUM(CASE WHEN ptd.paymentmethodname ilike ''%Event Deposit%'' 
             THEN CASE WHEN pay.PAYMENTTYPE = ''Cash'' 
                THEN pay.AMOUNTAPPLIEDTOCHECK::DECIMAL(18,2) 
             ELSE pay.TOTAL END  
           ELSE 0 END) ::DECIMAL(36,2)                                  as "Deposit"     
    ,SUM(CASE WHEN pay.PAYMENTTYPE = ''Cash'' 
          THEN pay.AMOUNTAPPLIEDTOCHECK 
                  ELSE pay.TOTAL END )::DECIMAL(36,2)                   as "Total"            
        FROM DATAWAREHOUSE.CHEQUE_FACT                                  chf
          INNER JOIN DATAWAREHOUSE.PAYMENTS_FACT                        pay
            ON pay.CHEQUE_FACT_FK = chf.CHEQUE_FACT_NK
              AND chf.DW_ISCURRENTROW
              AND pay.DW_ISCURRENTROW
              AND chf.FISCAL_DATE::date >= :startdate::date 
              AND chf.FISCAL_DATE::date <= :enddate::date  
              AND chf.LOCATION_DIM_FK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
               AND NOT chf.IS_TRAINING
               AND NOT chf.DW_ISDELETED
               AND pay.PAYMENTSTATUS = ''Success''
               AND chf.STATUS = ''Closed''
          INNER JOIN DATAWAREHOUSE.location_DIM                          loc
            ON pay.location_DIM_FK = loc.location_DIM_NK
              AND loc.DW_ISCURRENTROW
          INNER JOIN DATAWAREHOUSE.PaymentMethod_DIM                     ptd      
            ON pay.PaymentMethod_DIM_FK = ptd.PaymentMethod_DIM_NK
              AND ptd.DW_ISCURRENTROW
              AND NOT ptd.DW_ISDELETED  
          LEFT JOIN TEMP_GCPURCHASE                                      gcf
            ON chf.CHEQUE_FACT_NK = gcf.CHEQUE_FACT_FK
    GROUP BY "Level"   
    ,"Location ID" 
    ,loc.LOCATION_DIM_NK
    ,CHF.CHEQUE_FACT_NK
    ,PAY.PAYMENTS_FACT_NK
    ,"Group 1B"
    ,"Group 2B"
    ,"Payment Method"
    ,"Payment Type"
    ,"Card Brand" 
    ,"Fiscal Date"

UNION
 
 SELECT pio."Level"              as "Level"   
    ,pio."Fiscal Date"           as "Fiscal Date"
    ,pio."Location ID"           as "Location ID"  
    ,pio."Group 1B"              as "Group 1B"
    ,pio."Group 2B"              as "Group 2B"    
    ,pio."Payment Type"          as "Payment Type"    
    ,pio."Payment Method"        as "Payment Method"
    ,pio."Card Brand"            as "Card Brand" 
-------------------------------------------------------------------------------------
    ,0                           as "Check Count"
    ,NULL                        as CHEQUE_FACT_NK
    ,NULL                        as PAYMENTS_FACT_NK
    ,0                           as "Taxes"
    ,0                           as "Surcharges"
    ,0                           as "Unpaid"
    ,0                           as "Gratuities"    
    ,0                           as "Fees"    
    ,0                           as "Deposits"
    ,0                           as "Gift Card Sales"
    ,0                           as "Tips2"           
    ,pio."Amount"                as "Pay In/Out"
    ,0                           as "Deposit Applied"
    ,0                           as "Net Sales" 
-------------------------------------------------------------------------------------
    ,1                           as "Count"
    ,0                           as "Tips" 
    ,0                           as "Sales"       
    ,0                           as "Deposit"     
    ,pio."Amount"                as "Total"      
FROM TEMP_PAYINOUT               pio
;    

-------------------------------------------------------------------------------------------
-- UNPIVOT CHECK BALANCES TO CREATE ROWS FOR THE ''Total Payments'' Row
-- DROP TABLE TEMP_TABLE_PAYMENTS_1AA
CREATE TEMP TABLE TEMP_TABLE_PAYMENTS_1AA AS
 SELECT ''Total Payments''  as "Group 1A"
  ,"Level"                as "Group 2A"
  ,"Fiscal Date"          as "Fiscal Date"
  ,"Location ID"          as "Location ID"
  ,SUM("Amount")          as "Total1" 
    FROM TEMP_PAYINOUT
    GROUP BY "Group 1A","Group 2A","Fiscal Date","Location ID"  
    ;
    
CREATE TEMP TABLE TEMP_TABLE_PAYMENTS_1AC AS
 SELECT ''Total Payments''  as "Group 1A"
  ,''Refunds''              as "Group 2A"
  ,"Fiscal Date"          as "Fiscal Date"
  ,"Location ID"          as "Location ID"
  ,SUM("Count")           as "Count"
  ,SUM("Refund Amount")   as "Total1" 
    FROM TEMP_REFUNDS
    GROUP BY "Group 1A","Group 2A","Fiscal Date","Location ID"      
;

CREATE TEMP TABLE TEMP_TABLE_PAYMENTS_1AB AS
SELECT ''Total Payments'' as "Group 1A"
  ,"Group 2"            as "Group 2A"
  ,"Fiscal Date"        as "Fiscal Date"
  ,"Location ID"        as "Location ID"
  ,SUM("Total1")        as "Total1"
  FROM TEMP_CHECK
    UNPIVOT ("Total1" FOR "Group 2" IN ("Taxes","Surcharges","Fees","Deposit Applied","Gift Card Sales","Tips2","Pay In/Out","Net Sales"/*,"Net Sates from Item"*/,"Gratuities"))
    WHERE "Check Count" = 1 --get only one row per check for check level details
  GROUP BY "Group 1A","Group 2A","Fiscal Date","Location ID"   
  ;

-- select * from TEMP_TABLE_PAYMENTS_1A  whERE "Group 1" = ''Refunds'';
-- select "Group 1","Group 2" from TEMP_TABLE_PAYMENTS_1A group by "Group 1", "Group 2" order by "Group 1", "Group 2"
--===========================================================================================
CREATE TEMP TABLE TEMP_TABLE_PAYMENTS_1A AS --don''t change to join since not all groups of brand and type will exist as pio and checks
SELECT ''Payments 1''                                                                             AS "Display Table ID"
      ,INLT1."Fiscal Date"                                                                      AS "Fiscal Date"
      ,INLT1."Group 1"                                                                          AS "Group 1"                                                                                    
      ,INLT1."Group 2"                                                                          AS "Group 2" 
      ,INLT1."Location ID"                                                                      AS "Location ID"
      ,IFNULL(SUM(INLT1."Count")     ,0)                                                        AS "Count"
      ,IFNULL(SUM(INLT1."Tips")      ,0)                                                        AS "Tips"  
      ,IFNULL(SUM(INLT1."Sales")     ,0)                                                        AS "Sales"
      ,IFNULL(SUM(INLT1."Pay In")    ,0)                                                        AS "Pay In"
      ,IFNULL(SUM(INLT1."Deposit")   ,0)                                                        AS "Deposit"
      
      ,SUM(CASE WHEN INLT1."Level" = ''Summary''
        THEN INLT1."Total1"
        ELSE
        IFNULL(INLT1."Tips"         ,0)
        + IFNULL(INLT1."Sales"      ,0) 
        + IFNULL(INLT1."Pay In"     ,0) 
        + IFNULL(INLT1."Deposit"    ,0)  
        END )
                                                                                                AS "Total" 
  FROM (
          SELECT CHK."Level"       AS "Level"
              ,CHK."Group 1B"      AS "Group 1"
              ,CHK."Group 2B"      AS "Group 2"  
              ,CHK."Fiscal Date"   AS "Fiscal Date" 
              ,CHK."Location ID"   AS "Location ID"
              ,CHK."Payment Type"  AS "Payment Type"
              ,CHK."Payment Method"AS "Payment Method"
              ,CHK."Card Brand"    AS "Card Brand"
              ,CHK."Count"         AS "Count"
              ,CHK."Tips"          AS "Tips"  
              ,CHK."Sales"         AS "Sales"
              ,0                   AS "Pay In"
              ,CHK."Deposit"       AS "Deposit"
              ,0                    AS "Total1"
            FROM TEMP_CHECK        CHK
          UNION ALL
           SELECT PIO."Level"       AS "Level"
              ,"Group 1B"           AS "Group 1"
              ,"Group 2B"           AS "Group 2"  
              ,PIO."Fiscal Date"    AS "Fiscal Date" 
              ,PIO."Location ID"    AS "Location ID"
              ,PIO."Payment Type"   AS "Payment Type"
              ,PIO."Payment Method" AS "Payment Method"
              ,PIO."Card Brand"     AS "Card Brand"
              ,0                    AS "Count" 
              ,0                    AS "Tips" 
              ,0                    AS "Sales"
              ,PIO."Amount"         AS "Pay In"
              ,0                    AS "Deposit"
              ,0                    AS "Total1"
            FROM TEMP_PAYINOUT      PIO
           UNION ALL
            SELECT REF."Level"      AS "Level"
              ,REF."Group 1C"       AS "Group 1"
              ,REF."Group 2C"       AS "Group 2"              
              ,REF."Fiscal Date"    AS "Fiscal Date" 
              ,REF."Location ID"    AS "Location ID"
              ,REF."Payment Type"   AS "Payment Type"
              ,REF."Payment Method" AS "Payment Method"
              ,NULL                 AS "Card Brand"
              ,REF."Count"          AS "Count" 
              ,0                    AS "Tips" 
              ,REF."Refund Amount"  AS "Sales"
              ,0                    AS "Pay In"
              ,0                    AS "Deposit"
              ,0                    AS "Total1"
           FROM TEMP_REFUNDS       REF    
               UNION ALL
           SELECT ''Summary''         AS "Level"
              ,SPY."Group 1A"       AS "Group 1"
              ,SPY."Group 2A"       AS "Group 2"
              ,SPY."Fiscal Date"    AS "Fiscal Date" 
              ,SPY."Location ID"    AS "Location ID"
              ,NULL                 AS "Payment Type"
              ,NULL                 AS "Payment Method"
              ,NULL                 AS "Card Brand"
              ,0                    AS "Count" 
              ,0                    AS "Tips" 
              ,0                    AS "Sales"
              ,0                    AS "Pay In"
              ,0                    AS "Deposit"
              ,SPY."Total1"         AS "Total1"
            FROM TEMP_TABLE_PAYMENTS_1AA     SPY   
               UNION ALL   
           SELECT ''Summary''         AS "Level"
              ,SPI."Group 1A"       AS "Group 1"
              ,SPI."Group 2A"       AS "Group 2"
              ,SPI."Fiscal Date"    AS "Fiscal Date" 
              ,SPI."Location ID"    AS "Location ID"
              ,NULL                 AS "Payment Type"
              ,NULL                 AS "Payment Method"
              ,NULL                 AS "Card Brand"
              ,0                    AS "Count" 
              ,0                    AS "Tips" 
              ,0                    AS "Sales"
              ,0                    AS "Pay In"
              ,0                    AS "Deposit"
              ,SPI."Total1"         AS "Total1"
            FROM TEMP_TABLE_PAYMENTS_1AB    SPI  
               UNION ALL   
           SELECT ''Summary''         AS "Level"
              ,SPR."Group 1A"       AS "Group 1"
              ,SPR."Group 2A"       AS "Group 2"
              ,SPR."Fiscal Date"    AS "Fiscal Date" 
              ,SPR."Location ID"    AS "Location ID"
              ,NULL                 AS "Payment Type"
              ,NULL                 AS "Payment Method"
              ,NULL                 AS "Card Brand"
              ,0                    AS "Count" 
              ,0                    AS "Tips" 
              ,0                    AS "Sales"
              ,0                    AS "Pay In"
              ,0                    AS "Deposit"
              ,SPR."Total1"         AS "Total1"
            FROM TEMP_TABLE_PAYMENTS_1AC    SPR              
            
         ) INLT1
  GROUP BY INLT1."Location ID"   
     ,"Fiscal Date"
      ,"Level"     
      ,"Group 1"
      ,"Group 2"
  ;
-- SELECT * FROM TEMP_TABLE_PAYMENTS_1AA WHERE "Group 2A" = ''PayInOut'';
--SELECT * FROM TEMP_TABLE_PAYMENTS_1A WHERE "Group 2" = ''PayInOut'';
--=========================================================================================== 
 reportSet:= (
 -- SELECT ''SS''  AS TESTCIK
 -- SELECT * FROM TEMP_CHECK
 -- SELECT * FROM TEMP_GIFTCARDS
 -- SELECT * FROM  TEMP_PAYINOUTS
 -- SELECT * FROM TEMP_TAXDETAILSFEE
 -- SELECT * FROM TEMP_TAXDETAILSTENDER
 -- SELECT * FROM TEMP_REFUNDS  
 -- SELECT * FROM TEMP_FEES    
 -- SELECT * FROM TEMP_TABLE_PAYMENTS_1A
 SELECT ROW_NUMBER() OVER (ORDER BY "Location ID")            AS  "Support ID" 
   ,* from TEMP_TABLE_PAYMENTS_1A
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';