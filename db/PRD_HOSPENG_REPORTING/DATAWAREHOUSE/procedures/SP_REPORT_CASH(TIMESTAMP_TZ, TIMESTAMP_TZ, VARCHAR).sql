CREATE OR REPLACE PROCEDURE "SP_REPORT_CASH"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2026-11-20T14:48:37.661Z''; 
  -- locationid string      :=  ''[1,2,3,4,351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
-- =============================================================================================================
--Expected Amount Totals Documented Here
--https://github.com/nabancard/pos-api/blob/master/db/migrations/20231120103333_enhance_cash_trackability.sql#L62
-- ============================================================================================================
BEGIN

DROP TABLE IF EXISTS TEMP_SHIFT;

--Temp Table to  FILTER both payments and Cashbank Events with the same Shift 
--  acts as a filter on location, time frame, and employee
  SELECT SHD_cte.SHIFT_DIM_NK                                         AS SHIFT_DIM_NK
    ,SHD_cte.fiscal_day                                               AS FISCAL_DAY
    ,SHD_cte.CLOCKEDIN_AT::timestamp_ntz                              AS CLOCKEDIN_AT
    ,SHD_cte.CLOCKEDOUT_AT::timestamp_ntz                             AS CLOCKEDOUT_AT
    ,SHD_cte.EMPLOYEE_DIM_FK                                          AS EMPLOYEE_DIM_FK
    ,SHD_cte.SHIFT_START_AT                                           AS SHIFT_START_AT
    ,SHD_cte.SHIFT_END_AT                                             AS SHIFT_END_AT
    ,EMD.EMPLOYEE_NAME                                                AS EMPLOYEE_NAME
    ,LOC.LOCATIONNAME                                                 AS LOCATIONNAME
    ,LOC.LOCATION_DIM_NK                                              AS "Location ID"    
    ,LOC.TZ_NAME                                                      AS "Time Zone"
    ,CBD.CASHBANK                                                     AS "Cashbank ID"
    ,cbd.open_amount < cbd.close_amount                               AS IS_OPENHIGHERTHANCLOSE
    ,IFNULL(cbd.open_amount,0.0000) - IFNULL(cbd.close_amount,0.0000) AS OPENDELTA_AMOUNT
    ,IFNULL(cbd.EXPECTED_AMOUNT,0.0000)                               AS EXPECTED_AMOUNT
    ,IFNULL(cbd.open_amount,0.0000)                                   AS OPEN_AMOUNT
    ,IFNULL(cbd.close_amount,0.0000)                                  AS CLOSE_AMOUNT
    ,CBD.CASHBANK_DIM_NK                                              AS CASHBANK_DIM_NK
    ,cbd.status                                                       AS STATUS
    ,SHD_cte.FISCAL_DAY                                               AS "Fiscal Day" 
    FROM SHIFT_DIM                                        SHD_cte
      INNER JOIN DATAWAREHOUSE.LOCATION_DIM                   LOC
         ON SHD_cte.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
            AND LOC.DW_ISCURRENTROW
            AND SHD_cte.DW_ISCURRENTROW
            AND NOT SHD_cte.DW_ISDELETED
            ----------------------------------------------------------------------------------------------
            AND SHD_cte.fiscal_day
                   >= :startdate::date 
            AND  SHD_cte.fiscal_day  
                  <= :enddate::date    
            AND SHD_cte.LOCATION_DIM_FK IN (SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1)
            ----------------------------------------------------------------------------------------------
      INNER JOIN DATAWAREHOUSE.CASHBANK_DIM                   CBD
        ON CBD.SHIFT_DIM_FK = SHD_cte.SHIFT_DIM_NK
          AND CBD.DW_ISCURRENTROW
          AND NOT CBD.DW_ISDELETED
          
      INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM                   EMD
        ON SHD_cte.EMPLOYEE_DIM_FK = EMD.EMPLOYEE_DIM_NK
          AND EMD.DW_ISCURRENTROW
;

CREATE TEMP TABLE TEMP_SHIFT AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));       
;


reportSet   := (
SELECT INLT."Support ID"                            AS "Support ID"
, ''CSH-'' ||row_number() over (order by INLT."Support ID") 
                                                    AS "Detail ID"   
--status, category, level------------------------------------------------------------------------------
,INLT."Status"                                      AS "Status"
--geography--------------------------------------------------------------------------------------------
,INLT."Location"                                    AS "Location"
,INLT."Location ID"                                 as "Location ID"
--dates------------------------------------------------------------------------------------------------
,INLT."Time Zone"                                   AS "Time Zone"
,to_char(LEFT(INLT."Fiscal Day",4))                 AS "Year"
,to_char(YEAR(INLT."Fiscal Day")) || ''|'' || TO_CHAR(LEFT(''0'' || MONTH(INLT."Fiscal Day"),2))                                       
                                                    AS "Year and Month"
,to_char(INLT."Fiscal Day")                         AS "Fiscal Day"
,IFNULL(INLT."Daypart",''None'')                      AS "Daypart"
-- ,INLT."Transaction Date"::timestamp_ntz                        ------------------------ 
,to_char(CONVERT_TIMEZONE(''UTC'',INLT."Time Zone" ,INLT."Transaction Date"::timestamp_ntz )::timestamp)        
                                                    AS "Transacted At"
 ,IFNULL(DAYNAME(INLT."Fiscal Day"),''None'')         AS"Day of Week"
    ,CASE WHEN DAYNAME(INLT."Fiscal Day") IN (''Sat'',''Sun'')  
             THEN TRUE ELSE FALSE END               AS "Is Weekend"

,to_char(CONVERT_TIMEZONE(''UTC'',INLT."Time Zone" ,INLT."Paid At"::timestamp_ntz )::timestamp )            
-- ,INLT."Paid At"::timestamp_ntz                                      
                                                    AS "Paid At"

,to_char(CONVERT_TIMEZONE(''UTC'',INLT."Time Zone" ,INLT."Clocked In At"::timestamp_ntz )::timestamp )                                                    
-- ,INLT."Clocked In At"::timestamp_ntz                               
                                                    AS "Clocked In At"

,to_char(CONVERT_TIMEZONE(''UTC'',INLT."Time Zone" ,INLT."Clocked Out At"::timestamp_ntz )::timestamp )                                                    
-- ,INLT."Clocked Out At"::timestamp_ntz                              
                                                    AS "Clocked Out At"
--flags------------------------------------------------------------------------------------------------
--people-----------------------------------------------------------------------------------------------
,INLT."Employee Name"               AS "Employee"
--Descriptors------------------------------------------------------------------------------------------ 
,INLT."Cash Drawer ID"              AS "Cash Drawer ID" 
,to_char(INLT."Cashbank ID")        AS "Cashbank ID"
,INLT."Check ID"                    AS "Check ID"
,INLT."Check NO"                    AS "Check NO"
,INLT."Payment ID"                  AS "Payment ID"        
,''Shift '' || to_varchar(INLT."Shift ID")  
                                    AS "Shift ID"
,INLT."Event Type"                  AS "Event Type"
,INLT."Notes"                       AS "Notes"
,INLT."Payment Method"              AS "Payment Method"
,INLT."Payment Type"                AS "Payment Type"
,INLT."Terminal"                    AS "Terminal"
,INLT."Reason"                      AS "Reason"
--Facts------------------------------------------------------------------------------------------------
 ,1::NUMBER(18,0)                                 AS "Count"
 ,INLT."Adjusted Cash Open Amount"::NUMBER(18,2)  AS "Adjusted Cash Open Amount"--The opening amount of cash  
 ,INLT."Cash Close Amount"::NUMBER(18,2)          AS "Cash Close Amount"--The closing amount of cash
 ,INLT."Cash Open Amount"::NUMBER(18,2)           AS "Cash Open Amount"--The opening amount of cash
 ,INLT."Cash ReOpen Amount"::NUMBER(18,2)         AS "Cash ReOpen Amount"
 ,INLT."Deposit"::NUMBER(18,2)                    AS "Deposit"--The opening amount of cash
 ,INLT."Withdrawl"::NUMBER(18,2)                  AS "Withdrawl"--The opening amount of cash   
 ,INLT.open_amount::NUMBER(18,2)                  AS "Open Amount"
 ,INLT.close_amount::NUMBER(18,2)                 AS "Close Amount"
 ,INLT."Cash Due"::NUMBER(18,2)                   AS "Cash Due"
 ,INLT."Expected Amount"::NUMBER(18,2)            AS "Expected Amount"
 ,INLT."Cash Change Amount"::NUMBER(18,2)         AS "Cash Change Amount"    
 ,INLT."Cash Tendered Amount"::NUMBER(18,2)       AS "Cash Tendered Amount" 
 ,INLT."Cash Payment Amount"::NUMBER(18,2)        AS "Cash Payment Amount" 
-- ,0.0000  AS "Non Cash Payments"---This could be gift certificate or some other  per davis put on hold
-- ,0.0000  AS "All Payments"--sum cash PLUS non cash  --Possibly removed from requirements?
 ,INLT."Cash Tip Amount"::NUMBER(18,2)            AS "Cash Tip Amount" 
 ,INLT."Pay In Amount"::NUMBER(18,2)              AS "Pay In Amount"
 ,INLT."Pay Out Amount"::NUMBER(18,2)             AS "Pay Out Amount"
-------------------------------------------------------------------------------------------------------
  FROM (
        SELECT ''CBE:'' || TO_CHAR(CHF.CASHBANKEVENT_FACT_PK) AS "Support ID"    --unique id not for displayeing id
        ,shd."Cashbank ID"                        AS "Cashbank ID"
        ,shd.EMPLOYEE_NAME                        AS "Employee Name"
        ,shd."Fiscal Day"                         AS "Fiscal Day"
        ,chf.EVENT_TYPE                           AS "Event Type"
        ,shd.SHIFT_DIM_NK                         AS "Shift ID"
        ,''None''                                   AS "Check ID"
        ,''None''                                   AS "Check NO"
        ,shd.STATUS                               AS "Status"
        ,''None''                                   AS "Payment Method"
        ,''None''                                   AS "Payment Type"
        ,''None''                                   AS "Terminal"
        ,IFNULL(chf.notes,''None'')                 AS "Notes"
        ,''None''                                   AS "Reason"
        ,''None''                                   AS "Payment ID"

        ,shd."Time Zone"                          AS "Time Zone"  
        ,CASE chf.EVENT_TYPE 
          WHEN ''BankOpened'' THEN shd.CLOCKEDIN_AT
          WHEN ''BankClosed'' THEN shd.CLOCKEDOUT_AT
          ELSE chf.dw_startdate END
                                                  AS "Transaction Date"
        ,''None''                                   AS "Daypart"
        ,''None''                                   AS "Cash Drawer ID" --OPTIONAL
        ,shd.LOCATIONNAME                         AS "Location"
        ,shd."Location ID"                        AS "Location ID"
        ,chf.CREATED_AT                           AS "Paid At"
        ,shd.CLOCKEDIN_AT                         AS "Clocked In At"
        ,shd.CLOCKEDOUT_AT                        AS "Clocked Out At"
        --===================================================================================
        ,CASE WHEN chf.EVENT_TYPE = ''AdjustedOpenAmount'' 
          THEN IFNULL(chf.AMOUNT,0.0000) 
          ELSE 0.0000 END                         AS "Adjusted Cash Open Amount"--The opening amount of cash  
        ,CASE WHEN chf.EVENT_TYPE = ''BankClosed'' 
          THEN IFNULL(chf.AMOUNT,0.0000)
          ELSE 0.0000 END                         AS "Cash Close Amount"--The closing amount of cash
        ,CASE WHEN chf.EVENT_TYPE = ''BankOpened'' 
          THEN IFNULL(chf.AMOUNT ,0.0000)
          ELSE 0.0000 END 
                                                  AS "Cash Open Amount"--The opening amount of cash
        ,CASE WHEN chf.EVENT_TYPE = ''BankReopened''
          THEN IFNULL(chf.AMOUNT ,0.0000)
          ELSE 0.0000 END 
                                                  AS "Cash ReOpen Amount"
        ,CASE WHEN chf.EVENT_TYPE = ''Deposit'' 
          THEN IFNULL(chf.AMOUNT ,0.0000)
          ELSE 0.0000 END                         AS "Deposit"--The opening amount of cash
        ,CASE WHEN chf.EVENT_TYPE = ''Withdrawal'' 
          THEN IFNULL(chf.AMOUNT,0.0000) 
          ELSE 0.0000 END                         AS "Withdrawl"--The opening amount of cash
        
          ,shd.open_amount, shd.close_amount
        ,CASE WHEN chf.EVENT_TYPE = ''BankClosed'' AND shd.IS_OPENHIGHERTHANCLOSE   
          THEN IFNULL(shd.OPENDELTA_AMOUNT,0.0000)
         ELSE 0.00 END                            AS "Cash Due"--If the opening amount is less than closing amou            --NOTE:  1ST half of union is the open and close amounts so all 
         --=================
         --EXPECTED_AMOUNT
         ,CASE WHEN chf.EVENT_TYPE = ''BankOpened''    
          THEN IFNULL(shd.EXPECTED_AMOUNT,0.0000)
          ELSE 0.0000 END                         AS "Expected Amount"
        --===================================================================================
        ,0.0000                                   AS "Cash Change Amount"    
        ,0.0000                                   AS "Cash Tendered Amount" 
        ,0.0000                                   AS "Cash Payment Amount"  --Total Payments in Cash Received
        -- ,0.0000  AS "Non Cash Payments"---This could be gift certificate or some other  per davis put on hold
        -- ,0.0000  AS "All Payments"--sum cash PLUS non cash  --Possibly removed from requirements?
        ,0.0000                                   AS "Cash Tip Amount" 
        ,0.0000                                   AS "Pay In Amount"
        ,0.0000                                   AS "Pay Out Amount"
         -----------------------------------------------------------------------------------------  
        FROM TEMP_SHIFT                                        SHD
            INNER JOIN DATAWAREHOUSE.CASHBANKEVENT_FACT            CHF
              ON SHD.CASHBANK_DIM_NK = CHF.CASHBANK_DIM_FK
                  AND CHF.DW_ISCURRENTROW
                  AND NOT CHF.DW_ISDELETED
                  --AND CBD.STATUS <> ''Open''
--         -----------------------------------------------------------------------------------------
         UNION ALL
--         -- -----------------------------------------------------------------------------------------
        SELECT ''PAY:'' || TO_CHAR(pay.PAYMENTS_FACT_PK)               AS "Support ID"    --unique id 
        ,SHD."Cashbank ID"                        AS "Cashbank ID"
        ,shd.EMPLOYEE_NAME                        AS "Employee Name"
        ,shd."Fiscal Day"                         AS "Fiscal Day"
        ,''Cash Payment''                           AS "Event Type"
        ,shd.SHIFT_DIM_NK                         AS "Shift ID"
        ,to_char(pay.cheque_fact_fk)              AS "Check ID"
        ,to_char(pay.CHEQUENUMBER)                AS "Check"
        ,shd.STATUS                               AS "Status"
        ,pmd.PAYMENTMETHODNAME                    AS "Payment Method"
        ,pmd.PAYMENTMETHODTYPE                    AS "Payment Type"
        ,TED.TERMINALNAME                         AS "Terminal"
        ,''None''                                   AS "Notes"
        ,''None''                                   AS "Reason"
        ,TO_CHAR(pay.PAYMENTS_FACT_NK)            AS "Payment ID"
        ,shd."Time Zone"                          AS "Time Zone"          
        ,pay.PAID_AT::timestamp_ntz 
                                                  AS "Transaction Date"
        ,dpd.DAYPART                              AS "Daypart"
        ,null                                     AS "Cash Drawer ID" --OPTIONAL
        ,shd.LOCATIONNAME                         AS "Location"
        ,shd."Location ID"                        AS "Location ID"
        ,pay.PAID_AT::timestamp_ntz                              
                                                  AS "Paid At"
        ,shd.CLOCKEDIN_AT                         AS "Clocked In At"
        ,shd.CLOCKEDOUT_AT                        AS "Clocked Out At"
        --===================================================================================
        ,0.0000                                   AS "Adjusted Cash Open Amount"
        ,0.0000   
        AS "Cash Close Amount"--The closing amount of cash
        ,0.0000                                   AS "Cash Open Amount"--The opening amount of cash
        ,0.0000                                   AS "Cash ReOpen Amount"
        ,0.0000                                   AS "Deposit"--The opening amount of cash
        ,0.0000                                   AS "Withdrawl"--The opening amount of cash
        
        ,shd.open_amount
        , shd.close_amount
        ,0.0000                                   AS "Cash Due"
        ,0.0000                                   AS "Expected Amount"
        --===================================================================================
        ,IFNULL(CPL.AMOUNT_CHANGED,0.0000)         AS "Cash Change Amount"    
        ,IFNULL(CPL.AMOUNT_TENDERED,0.0000)       AS "Cash Tendered Amount"     
        ,IFNULL(PAY.TOTAL,0.0000)                 AS "Cash Payment Amount"  --Total Payments in Cash Received
        -- ,NULL AS "Non Cash Payments"---This could be gift certificate or some other per davis put on hold
        -- ,NULL AS "All Payments"--sum cash PLUS non cash
        ,IFNULL(PAY.TIP,0.0000)                    AS "Cash Tip Amount" 
        ,0.0000                                    AS "Pay In Amount"
        ,0.0000                                    AS "Pay Out Amount"
         ----------------------------------------------------------------------------------------- 
           FROM TEMP_SHIFT                                       SHD
            INNER JOIN  CHECKCASHPAYMENTLEDGER_FACT              CPL
              ON SHD.SHIFT_DIM_NK = CPL.SHIFT_DIM_FK
                AND CPL.DW_ISCURRENTROW
                AND NOT CPL.DW_ISDELETED
                AND NOT CPL.IS_VOID
            INNER JOIN DATAWAREHOUSE.PAYMENTS_FACT                   PAY    --465
              ON PAY.PAYMENTS_FACT_NK = CPL.PAYMENTS_FACT_FK
                -- AND pay.PAYMENTSTATUS = ''Success''    --No need to check payment method for cash as all ledger rows are cash
                AND pay.DW_ISCURRENTROW
                AND NOT pay.IS_TRAINING
            INNER JOIN DATAWAREHOUSE.PAYMENTMETHOD_DIM                PMD
               ON PMD.PAYMENTMETHOD_DIM_NK = PAY.PAYMENTMETHOD_DIM_FK
                AND PAY.DW_ISCURRENTROW
            INNER JOIN DATAWAREHOUSE.DAYPART_DIM                       DPD
               ON dpd.DAYPART_DIM_NK = pay.DAYPART_DIM_FK
                 AND dpd.dw_iscurrentrow
            LEFT JOIN DATAWAREHOUSE.TERMINAL_DIM                       TED
                ON TED.TERMINAL_DIM_NK = PAY.TERMINAL_DIM_FK
                  AND TED.DW_ISCURRENTROW
        -- -----------------------------------------------------------------------------------------
        UNION ALL
        -- -----------------------------------------------------------------------------------------
        SELECT ''PIO:'' || TO_CHAR(PIO.PAYINOUT_FACT_PK)      AS "Support ID"   
        ,shd."Cashbank ID"                        AS "Cashbank ID"
        ,shd.EMPLOYEE_NAME                        AS "Employee Name"
        ,shd."Fiscal Day"                         AS "Fiscal Day"
        ,CASE WHEN PAR.IS_PAY_IN THEN ''Pay In''
          ELSE ''Pay Out'' END                      AS "Event Type"
        ,shd.SHIFT_DIM_NK                         AS "Shift ID"
        ,''None''                                   AS "Check ID"
        ,''None''                                   AS "Check NO"
        ,PIO.STATUS                               AS "Status"
        ,pmd.PAYMENTMETHODNAME                    AS "Payment Method"
        ,pmd.PAYMENTMETHODTYPE                    AS "Payment Type"
        ,''None''                                   AS "Terminal"
        ,IFNULL(pio.NOTES,''None'')                 AS "Notes"
        ,IFNULL(par.PAYINPAYOUTREASON,''None'')     AS "Reason" 
        ,''None''                                   AS "Payment ID"
         ,shd."Time Zone"                         AS "Time Zone"          
        ,pio.dw_startdate                         AS "Transaction Date"
        ,''None''                                   AS "Daypart"
        ,''None''                                   AS "Cash Drawer ID" 
        ,shd.LOCATIONNAME                         AS "Location"
        ,shd."Location ID"                        AS "Location ID"
        ,pio.CREATED_AT::timestamp_ntz                           
                                                  AS "Paid At"
        ,shd.CLOCKEDIN_AT                         AS "Clocked In At"
        ,shd.CLOCKEDOUT_AT                        AS "Clocked Out At"
        --===================================================================================
        ,0.0000                                   AS "Adjusted Cash Open Amount"
        ,0.0000                                   AS "Cash Close Amount"
        ,0.0000                                   AS "Cash Open Amount"
        ,0.0000                                   AS "Cash ReOpen Amount"
        ,0.0000                                   AS "Deposit"
        ,0.0000                                   AS "Withdrawl"
        ,NULL, NULL
        ,0.0000                                   AS "Cash Due"--If the opening amount is less than closing amou  
        ,0.0000                                   AS "Expected Amount"
        --===================================================================================
        ,0.0000                                   AS "Cash Change Amount"    
        ,0.0000                                   AS "Cash Tendered Amount" 
        ,0.0000                                   AS "Cash Payment Amount"
        ,0.0000                                   AS "Cash Tip Amount"
        ,CASE WHEN PAR.IS_PAY_IN THEN PIO.AMOUNT 
          ELSE 0.0000 END                         AS "Pay In Amount"  --Total Payments in Cash Received
        ,CASE WHEN NOT PAR.IS_PAY_IN THEN PIO.AMOUNT 
          ELSE 0.0000 END                         AS "Pay Out Amount" 
         -----------------------------------------------------------------------------------------  
        FROM TEMP_SHIFT                                        SHD
            INNER JOIN DATAWAREHOUSE.PAYINOUT_FACT                 PIO
              ON SHD.SHIFT_DIM_NK = PIO.SHIFT_DIM_FK
                  AND PIO.DW_ISCURRENTROW
                  AND NOT PIO.DW_ISDELETED
                  AND NOT PIO.IS_VOID
                  AND PIO.STATUS = ''Success''
            INNER JOIN DATAWAREHOUSE.PAYMENTMETHOD_DIM             PMD
              ON PMD.PAYMENTMETHOD_DIM_NK = PIO.PAYMENTMETHOD_DIM_FK
                  AND PMD.DW_ISCURRENTROW
                  AND PMD.PAYMENTMETHODTYPE = ''Cash''
                  -- AND PMD.PAYMENTMETHODNAME <> ''Error''
            INNER JOIN DATAWAREHOUSE.PAYINPAYOUTREASON_DIM         PAR
              ON PAR.PAYINPAYOUTREASON_DIM_NK = PIO.PAYINPAYOUTREASON_DIM_FK
                  AND PAR.DW_ISCURRENTROW
                  
           ) INLT
-- =================================================================================================================
); 
 RETURN TABLE(reportSet); 
END';