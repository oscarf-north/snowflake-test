CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_GIFTCARDS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
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
  DROP TABLE IF EXISTS TEMP_TABLE1;

 -- -------------------------------------------------------------------------------------------
   CREATE TEMP TABLE TEMP_TABLE1
      AS
    SELECT CHF.LOCATION_DIM_FK                                                AS "Location ID"
                    ,CHF.FISCAL_DATE                                          AS "Fiscal Day"
                    ,GCD.IS_LEGACY
                    ,(CASE WHEN GCF.COMMAND IN (''Reload'',''VoidReload'') THEN 
                       CASE WHEN GCF.OPENING_BALANCE IS  NULL OR GCF.GIFTCARD_DIM_Fk = 1 THEN GCF.TRANSACTION_AMOUNT 
                          ELSE GCF.CLOSING_BALANCE - GCF.OPENING_BALANCE  END  ELSE 0 END )::DECIMAL(36,2)
                                                                              AS "Reload Amount" 
                    ,(CASE WHEN GCF.COMMAND IN (''Adjust'',''NoNSFSale'',''VoidSale'') THEN 
                          CASE WHEN GCF.OPENING_BALANCE IS NULL OR GCF.GIFTCARD_DIM_Fk = 1  
                            THEN GCF.TRANSACTION_AMOUNT * (-1) ELSE GCF.CLOSING_BALANCE - GCF.OPENING_BALANCE  END  ELSE 0 END )  ::DECIMAL(36,2)
                                                                              AS "Redeemed Amount"  
                                                 
                FROM DATAWAREHOUSE.GIFTCARDTRANSACTION_FACT                   GCF  
                    INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                      CHF
                      ON CHF.CHEQUE_FACT_NK = GCF.CHEQUE_FACT_FK
                          AND CHF.FISCAL_DATE::date >= :startdate::date 
                          AND CHF.FISCAL_DATE::date <= :enddate::date  
                          AND CHF.LOCATION_DIM_FK in (
                             SELECT table1.value 
                               FROM table(split_to_table(:locationidS, '',''))  table1) 
                          AND CHF.STATUS = ''Closed''
                          AND GCF.DW_ISCURRENTROW
                          AND NOT GCF.DW_ISDELETED     
                          AND CHF.DW_ISCURRENTROW
                     INNER JOIN DATAWAREHOUSE.GIFTCARD_DIM                     GCD
                       ON GCD.GIFTCARD_DIM_NK = GCF.GIFTCARD_DIM_FK
                          AND GCD.DW_ISCURRENTROW
                          AND GCD.IS_ISSUED
                          AND NOT GCD.DW_ISDELETED  ;
  

--=========================================================================================
 reportSet:= (
 SELECT * from TEMP_TABLE1
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';