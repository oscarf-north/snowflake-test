CREATE OR REPLACE PROCEDURE "SP_REPORT_TENDER"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[3,2,351]'';
  locationidS string        :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
-- -- --==========================================================================================
-- --ISSUE 1:  HOW DO WE DETERMINE IF SOMETHING IS AN NFC PAYMENT???
-- --ISSUE 2:  Location groups look like there may be many groups per location.  That could throw off totals..how to pick one?
-- --ISSUE 3:  What status to use?  Success only?   
-- --QUEST 1:  DO WE FILTER OUT SOME STATUSES?  SHOW SUCCESS ONLY? compare check.payments to check.paid ---see cheque.id = 4320 and example below  ref-> WORKBOOK:  Payment sum to cheque paid
--LOOK HERE FOR "card_entry_type"  CARD ENTRY TYPES  the cc_transaction table in the "card_entry_type"
-- [''None''],
  -- [''Swiped Track 2''],
  -- [''Fallback Swipe''],
  -- [''EMV Contact''],
  -- [''EMV Contactless''],
  -- [''Manual Entry''],
  -- [''Fallback With No App''],
  -- [''Unknown''],
-- --==========================================================================================
BEGIN
 reportSet   := (
 
        SELECT pay.payments_fact_nk                    as "Support ID" 
         , ''TEN-'' ||row_number() over (order by pay.payments_fact_nk) 
                                                       as "Detail ID"        
--status, category, level-------------------------------------------------------------------
          ,IFNULL(pay.paymentstatus,''None'')            as "Status"
--geography--------------------------------------------------------------------------------      
          ,IFNULL(loc.locationname,''None'')             as "Location"
          ,loc.location_dim_nk::decimal(36,0)          as "Location ID"
          ,IFNULL(pay.revenuecentername ,''None'')       as "Revenue Center"
 --dates-------------------------------------------------------------------------------------
     ,LOC.TZ_NAME                                      as "Time Zone"
    ,to_char(LEFT(pay.FISCALDATE,4))                   as "Year"
    ,to_char(YEAR(pay.FISCALDATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(pay.FISCALDATE),2))
                                                       as "Year and Month"
    ,IFNULL(dpd.DAYPART,''None'')                        as "Daypart" 
           
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.OPENED_AT::timestamp_ntz )::timestamp )         
                                                        as "Paid At"
    ,to_char(pay.FISCALDATE)                            as "Fiscal Day"
    ,IFNULL(DAYNAME(pay.FISCALDATE),''None'')             as "Day of Week"
    ,CASE WHEN DAYNAME(pay.FISCALDATE) IN (''Sat'',''Sun'')  
          THEN TRUE ELSE FALSE END                      as "Is Weekend"

--people-------------------------------------------------------------------------------------
          ,IFNULL(emd.EMPLOYEE_NAME,''None'')             as "Employee"
--Descriptors--------------------------------------------------------------------------------    
          ,pay.CHEQUENUMBER                             as "Check"
          ,IFNULL(ptd.paymentmethodname ,''None'')        as "Payment Method"
          ,PAYMENTTYPE                                  as "Payment Type"
          ,CASE WHEN  pay.PAYMENTTYPE in (''CC'',''EPX'') THEN pay.cardbrand ELSE ptd.paymentmethodname END                             
                                                        as "Payment Name"   
          ,IFNULL(pay.cardholderName,''None'')            as "Cardholder Name"
          ,IFNULL(pay.LASTFOURCCNUMBER,''None'')          as "Card Last 4 Digits"
          ,IFNULL(case pay.cardbrand when '''' 
             THEN ''Not a Credit Card'' 
            else pay.cardbrand end,''None'')  
                                                        as "Card Brand" 
         ,otd.ORDER_TYPE                                as "Order Type"                                                        
         ,to_char(pay.LOCATION_DIM_FK)                  as "Location ID"
         ,pay.LOCATION_DIM_FK                           as "Location ID INT"
 --Facts--------------------------------------------------------------------------------------  
         ,1::NUMBER(18,0)                               as "Count"
          ,case pay.PAYMENTTYPE when ''Cash''  
              then pay.AMOUNTAPPLIEDTOCHECK::DECIMAL(18,2)  end     as "Cash Sales" 

          ,case pay.PAYMENTTYPE when ''Other'' 
              then pay.TOTAL end::DECIMAL(18,2)         as "Other Sales"         --Gross of other trans
          ,case when pay.cardbrand = ''Visa'' and pay.PAYMENTTYPE in (''CC'',''EPX'')
               then pay.TOTAL end::DECIMAL(18,2)        as "Visa Sales"           --Gross sales of al Visa trans
          ,case when pay.cardbrand = ''American Express'' and pay.PAYMENTTYPE in (''CC'',''EPX'')
               then pay.TOTAL end::DECIMAL(18,2)        as "Amex Sales"           --Gross sales of all Amex transactions
          ,case when pay.cardbrand = ''Mastercard'' and pay.PAYMENTTYPE in (''CC'',''EPX'')
              then pay.TOTAL end::DECIMAL(18,2)         as "Mastercard Sales"     --Gross sales of all Mastercard transactions
          ,case when pay.cardbrand = ''Discover''and pay.PAYMENTTYPE in (''CC'',''EPX'')
            then pay.TOTAL end::DECIMAL(18,2)           as "Discover Sales"       --Gross sales of all Discover transactions
           ,case when pay.cardbrand= ''Citi'' and pay.PAYMENTTYPE in (''CC'',''EPX'')
               then pay.TOTAL end::DECIMAL(18,2)        as "Citi Sales"                                   
          ,case when pay.PAYMENTTYPE in (''CC'',''EPX'')
              then pay.TOTAL end::DECIMAL(18,2)         as "Credit Card Sales"   --Gross of credit card trans              
          ,IFF(cct.ISCARDPRESENT,pay.TOTAL 
            ,0.00 )::DECIMAL(18,2)                      as "Card Present Sales"   
          --Gross sales of all transactions where the processor identified the card as present (swiped, dipped, tapped)
          ,IFF(NOT cct.ISCARDPRESENT,pay.TOTAL 
            ,0.00 )::DECIMAL(18,2)                      as "Card Not Present Sales" --Total Card Not Present Sales
          ,IFF( pay.PAYMENTTYPE = ''GiftCard'',pay.TOTAL 
            ,0.00 )::DECIMAL(18,2)                      as "Gift Card"

          ,CASE WHEN pay.PAYMENTTYPE in (''EPX'',''CC'') then pay.Tip::DECIMAL(18,2) else 0 end 
                                                        as "Credit Card Tips"
            
          ,pay.Tip::DECIMAL(18,2)                       as "Tips"  

          ,to_numeric(CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN (pay.AMOUNTAPPLIEDTOCHECK)
                  else (pay.TOTAL ) end - ifnull(pay.TIP,0),16,2)as "Pre Tip Total" 

          
          ,CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK::DECIMAL(18,2) 
                  else pay.TOTAL ::DECIMAL(18,2) end     as "Amount"  
          ,CASE WHEN pay.PAYMENTTYPE = ''Cash'' THEN pay.AMOUNTAPPLIEDTOCHECK::DECIMAL(18,2) 
                  else pay.TOTAL::DECIMAL(18,2) end     as "Total"            
        FROM DATAWAREHOUSE.payments_FACT                                pay
          INNER JOIN DATAWAREHOUSE.location_DIM                         loc
            ON pay.location_DIM_FK = loc.location_DIM_NK
              AND pay.FISCALDATE::date >= :startdate::date 
              AND pay.FISCALDATE::date <= :enddate::date  
              AND pay.LOCATION_DIM_FK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
              AND pay.dw_iscurrentrow
              AND loc.dw_iscurrentrow
              AND NOT pay.IS_TRAINING
              AND NOT pay.dw_isdeleted
              AND pay.PAYMENTSTATUS = ''Success''
          INNER JOIN DATAWAREHOUSE.PaymentMethod_DIM                     ptd      
            ON pay.PaymentMethod_DIM_FK = ptd.PaymentMethod_DIM_NK
              AND ptd.dw_iscurrentrow
              AND NOT ptd.dw_isdeleted
          INNER JOIN DATAWAREHOUSE.organization_dim                      org
            ON loc.organization_DIM_FK = org.organization_DIM_NK
              AND org.dw_iscurrentrow
              AND NOT org.dw_isdeleted
          INNER JOIN DATAWAREHOUSE.daypart_dim                           dpd
            ON pay.daypart_dim_fk = dpd.daypart_dim_nk
              AND dpd.dw_iscurrentrow
              AND NOT dpd.dw_isdeleted
          INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                         otd
            ON otd.Ordertype_DIM_nK = pay.Ordertype_DIM_FK
              AND otd.dw_iscurrentrow
          LEFT JOIN DATAWAREHOUSE.employee_DIM                           emd
            ON emd.employee_DIM_NK = pay.EMPLOYEE_DIM_FK_AS_PAYEE
              AND emd.dw_iscurrentrow 
          LEFT JOIN DATAWAREHOUSE.CCTransaction_FACT                      cct   
            ON cct.cctransaction_fact_nk = pay.TRANSACTION_FACT_FK                     
              AND cct.dw_iscurrentrow                                                                
              AND cct.TRANSACTION_NUMBER = 1                             
    ORDER BY loc.locationname
--============================================================================
); 
 RETURN TABLE(reportSet); 
END';