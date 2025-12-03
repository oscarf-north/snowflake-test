CREATE OR REPLACE PROCEDURE "SP_REPORT_TENDER_0001"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- ====================================================================================================================
DECLARE 
  reportSet resultset;
  numeric_value number(38,2) := 0;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
 
-- -- --==========================================================================================
-- --ISSUE 1:  HOW DO WE DETERMINE IF SOMETHING IS AN NFC PAYMENT???
-- --ISSUE 2:  Location groups look like there may be many groups per location.  That could throw off totals..how to pick one?
-- --ISSUE 3:  What status to use?  Success only?   
-- --QUEST 1:  DO WE FILTER OUT SOME STATUSES?  SHOW SUCCESS ONLY? compare check.payments to check.paid ---see cheque.id = 4320 and example below  ref-> WORKBOOK:  Payment sum to cheque paid
-- --==========================================================================================
BEGIN
 reportSet   := (
 
        SELECT pay.payments_fact_nk                     as "Support ID" 
         , ''TEN-'' ||row_number() over (order by pay.payments_fact_nk) 
                                                    AS "Detail ID"        
--status, category, level-------------------------------------------------------------------
          ,IFNULL(pay.paymentstatus,''None'')             as "Status"
--geography--------------------------------------------------------------------------------      
          ,IFNULL(loc.locationname,''None'')              as "Location"
          ,IFNULL(pay.revenuecentername ,''None'')        as "Revenue Center"
 --dates-------------------------------------------------------------------------------------
           ,LOC.TZ_NAME                                 as "Time Zone" 
          ,to_char(LEFT(
          CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.OPENED_AT::timestamp_ntz )
          ,4))                                          as "Year"
          ,to_char(LEFT(
          CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.OPENED_AT::timestamp_ntz )
          ,7))                                          as "Year and Month"
          ,IFNULL(dpd.DAYPART,''None'')                   as "Daypart"
          ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.OPENED_AT::timestamp_ntz )                               
                                                        as "Paid At"
          ,IFNULL(DAYNAME(
          CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.OPENED_AT::timestamp_ntz )
          ),''None'')                                     as "Day of Week"
          ,CASE WHEN DAYNAME(
          CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.OPENED_AT::timestamp_ntz )
          ) IN (''Sat'',''Sun'')  
           THEN TRUE ELSE FALSE END                     as "Is Weekend"
--people-------------------------------------------------------------------------------------
          ,IFNULL(emd.EMPLOYEE_NAME,''None'')             as "Employee"
--Descriptors--------------------------------------------------------------------------------        
          ,IFNULL(ptd.paymentmethodname ,''None'')        as "Payment Method"
          ,IFNULL(pay.cardholderName,''None'')            as "Cardholder Name"
          ,IFNULL(pay.LASTFOURCCNUMBER,''None'')          as "Card Last 4 Digits"
          ,IFNULL(case pay.cardbrand when '''' 
             THEN ''Not a Credit Card'' 
            else pay.cardbrand end,''None'')  
                                                        as "Card Brand" 
         ,pay.LOCATION_DIM_FK                           as "Location ID"
 --Facts--------------------------------------------------------------------------------------  
         ,1::NUMBER(18,0)                               as "Count"
          ,case ptd.paymentmethodtype when ''Cash''  
              then pay.total end::DECIMAL(18,2)         as "Cash Sales" 
          ,case ptd.paymentmethodtype when ''EPX''   
              then pay.total end::DECIMAL(18,2)         as "Credit Card Sales"   --Gross of credit card trans
          ,case ptd.paymentmethodtype when ''Other'' 
              then pay.total end::DECIMAL(18,2)         as "Other Sales"         --Gross of other trans
          ,case pay.cardbrand when ''Visa'' 
               then pay.total end::DECIMAL(18,2)        as "Visa Sales"           --Gross sales of a
          ,case pay.cardbrand when ''Amex'' 
               then pay.total end::DECIMAL(18,2)        as "Amex Sales"           --Gross sales of alIl Amex transactions
          ,case pay.cardbrand when ''Mastercard'' 
              then pay.total end::DECIMAL(18,2)         as "Mastercard Sales"     --Gross sales of all Mastercard transactions
          ,case pay.cardbrand when ''Discover'' 
            then pay.total end::DECIMAL(18,2)           as "Discover Sales"       --Gross sales of all Discover transactions
          -- -----------------
          ,IFF(cct.ISCARDPRESENT,pay.Total 
            ,0.00 )::DECIMAL(18,2)                      as "Card Present Sales"   
          --Gross sales of all transactions where the processor identified the card as present (swiped, dipped, tapped)
          ,IFF(NOT cct.ISCARDPRESENT,pay.Total 
            ,0.00 )::DECIMAL(18,2)                      as "Card Not Present Sales" --Total Card Not Present Sales
          -- -----------------
          -- ,0.00::DECIMAL(18,2)                       as "NFC Sales"           
                                                        --Gross of tran where the processor identified the card  NO FIELD IN PRODUCTIO
          --                                                     --from an NFC devi.00ce (Apple Pay, Google Pay, Samsung Pay, etc.) or NFC card 
          ,pay.Refunds::DECIMAL(18,2)                    as "Refunds"  
          ,pay.Total::DECIMAL(18,2)                      as "Amount"  

        
        FROM DATAADMIN.payments_FACT                                pay
          INNER JOIN DATAADMIN.location_DIM                         loc
            ON pay.location_DIM_FK = loc.location_DIM_PK
              AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.opened_at::timestamp_ntz ) > CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:startdate::timestamp_ntz )
              AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,pay.opened_at::timestamp_ntz ) < CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:enddate::timestamp_ntz )
              AND pay.LOCATION_DIM_FK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
              AND pay.dw_iscurrentrow
              AND loc.dw_iscurrentrow
              AND NOT pay.IS_TRAINING
              AND NOT pay.dw_isdeleted
              AND pay.PAYMENTSTATUS = ''Success''
          INNER JOIN DATAADMIN.PaymentMethod_DIM                     ptd      
            ON pay.PaymentMethod_DIM_FK = ptd.PaymentMethod_DIM_PK
              AND ptd.dw_iscurrentrow
              AND NOT ptd.dw_isdeleted
          INNER JOIN DATAADMIN.organization_dim                      org
            ON loc.organization_DIM_FK = org.organization_DIM_NK
              AND org.dw_iscurrentrow
              AND NOT org.dw_isdeleted
          INNER JOIN DATAADMIN.daypart_dim                           dpd
            ON pay.daypart_dim_fk = dpd.daypart_dim_nk
              AND dpd.dw_iscurrentrow
              AND NOT dpd.dw_isdeleted
          INNER JOIN DATAADMIN.employee_DIM                          emd
            ON emd.employee_DIM_PK = pay.EMPLOYEE_DIM_FK_AS_PAYEE
              AND emd.dw_iscurrentrow 
              AND NOT emd.dw_isdeleted
          LEFT JOIN DATAADMIN.CCTransaction_FACT                    cct   --only need to get is present and nfc
            ON cct.cheque_fact_fk = pay.cheque_fact_fk                    --question:  can this data be put on 
              AND cct.dw_iscurrentrow                                     --payments so we don''t need to join thi                           
              AND cct.TRANSACTION_NUMBER = 1                              --huge table?
    ORDER BY loc.locationname
--============================================================================
); 
 RETURN TABLE(reportSet); 
END;
-- ';