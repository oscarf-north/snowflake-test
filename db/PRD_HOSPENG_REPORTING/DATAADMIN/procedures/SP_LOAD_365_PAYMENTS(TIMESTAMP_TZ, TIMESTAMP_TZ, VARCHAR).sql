CREATE OR REPLACE PROCEDURE "SP_LOAD_365_PAYMENTS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2024-03-26'';
  -- enddate timestamp_tz   := ''2025-01-26'';
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  --https://docs.restaurant365.com/docs/pos-data-export-file-guide  <====file layout
--=================================================================================================================================
--ISSUES
--Grats not separated out on payment
--no business day on spec for fiscal day
--=================================================================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_pay; 
  DROP TABLE IF EXISTS TEMP_header; 

-----------------------------------------------------------------------------------------------------------------------------------
--create the zero value data so that an ''empty'' file is created when there is no data for that fiscal day
SELECT ''Payment_CheckNumber''   --* string
,''Payment_BaseAmount''          --* decimal Amount for the payment, not including tips or gratuity.
,''Payment_TenderNumber''        --*string  
,''Payment_RevenueCenterName''   --string
,''Payment_Time''                --DateTime(mm/dd/yyyy hh:mm:ss)
;

CREATE TEMP TABLE TEMP_header AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-----------------------------------------------------------------------------------------------------------------------------------
SELECT to_char(PAY.chequenumber) || ''.'' || to_char(PAY.cheque_fact_fk)              
                                      AS Payment_CheckNumber  --* string
,TO_CHAR(IFNULL((CASE WHEN pay.PAYMENTTYPE = ''Cash'' 
          THEN pay.AMOUNTAPPLIEDTOCHECK
      ELSE pay.TOTAL END  - PAY.TIP),0.00) ::DECIMAL(18,2))
                                      AS Payment_BaseAmount   --* decimal Amount for the payment, not including tips or gratuity.
,TO_CHAR(PAY.PAYMENTMETHOD_DIM_FK)    AS Payment_TenderNumber --*string  
,IFNULL(REPLACE(PAY.REVENUECENTERNAME,'','','' ''),''None'') 
                                      AS Payment_RevenueCenterName --string
,TO_CHAR(TO_TIMESTAMP(PAY.PAID_AT),''MM/DD/YYYY HH24:MI:SS'') 
                                      AS Payment_Time     --DateTime(mm/dd/yyyy hh:mm:ss)

FROM DATAWAREHOUSE.payments_FACT                               pay
   WHERE pay.dw_iscurrentrow
     AND NOT pay.IS_TRAINING
     AND NOT pay.dw_isdeleted
     AND pay.PAYMENTSTATUS = ''Success''    
     AND pay.FISCALDATE::date >= :startdate::date 
     AND pay.FISCALDATE::date <= :enddate::date       
     AND pay.LOCATION_DIM_FK in (SELECT table1.value 
        FROM table(split_to_table(:locationidS, '',''))  table1) 
; 

CREATE TEMP TABLE TEMP_pay AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
--=================================================================================================================================
reportSet := (  

   SELECT * FROM TEMP_header
     UNION ALL
   SELECT * FROM TEMP_pay

);

RETURN TABLE(reportSet); 
END';