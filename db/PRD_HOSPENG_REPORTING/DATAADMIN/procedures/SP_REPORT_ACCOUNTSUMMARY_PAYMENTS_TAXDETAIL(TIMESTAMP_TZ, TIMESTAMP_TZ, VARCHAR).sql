CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_TAXDETAIL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet           resultset;
  -- startdate string    := ''2025-11-05'';  
  -- enddate string      := ''2025-11-05''; 
  -- locationid string   := ''[27]'';
  locationidS string  :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');

  -- CALL DATAADMIN.SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_TAXDETAIL(''2000-11-20T14:48:37.661Z'',''2026-11-20T14:48:37.661Z'',''[351,27]'');
-- =======================================================================================
-- CALL DATAWAREHOUSE.SP_REPORTDATAGROOM(''SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_TAXDETAIL'',2,3);
-- CALL dataadmin.SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_TAXDETAIL(''2001-01-20T14:48:37.661Z'',''2026-11-20T14:48:37.661Z'',''[361, 352, 353, 351, 574, 480, 433, 421, 408, 399, 390, 389, 379, 382, 385, 387, 388]'');
-- GRANT usage ON procedure DATAADMIN.SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_TAXDETAIL(timestamp_tz,timestamp_tz,string) TO ROLE DATA_REPLICATION_HOSPENG;
  
--=========================================================================================

BEGIN
  DROP TABLE IF EXISTS TEMP_TABLE1;
  DROP TABLE IF EXISTS TEMP_TABLE2;

 SELECT 
    tax.LOCATION_DIM_FK                                       as "Location ID"
    ,IFNULL(tax.TAXRATENAME,'' None'')                          as "Rate Name"
    ,tax.PERCENT::NUMBER(18,2)                                as "Rate"
    ,CASE WHEN is_tax_included THEN tax.AMOUNT ELSE 0 END     as "Inclusive"
    ,CASE WHEN noT is_tax_included THEN tax.AMOUNT ELSE 0 END as "Exclusive"
    ,0.00                                                     as "Fee Tax"    
    ,tax.AMOUNT::NUMBER(18,2)                                 as "Tax Total"

FROM DATAWAREHOUSE.TAX_FACT                                   tax
    INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                      chk
        ON chk.CHEQUE_FACT_NK = tax.CHEQUE_FACT_FK
          AND chk.DW_ISCURRENTROW
          AND NOT chk.IS_VOID
          AND NOT chk.DW_ISDELETED
          AND CHK.UNPAID = 0
     INNER JOIN DATAWAREHOUSE.ITEM_FACT                       itf
        ON itf.ITEM_FACT_NK = tax.ITEM_FACT_FK
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
                  FROM table(split_to_table(:locationidS, '',''))  table1);
  CREATE TEMP TABLE TEMP_TABLE1 AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

--=========================================================================================     
--get taxes for fees
   SELECT 
     tax.LOCATION_DIM_FK                                     as "Location ID"
    ,IFNULL(tax.TAXRATENAME,'' None'')                         as "Rate Name"
    ,tax.PERCENT::NUMBER(18,2)                               as "Rate"
    ,0::NUMBER(18,2)                                         as "Inclusive"
    ,0::NUMBER(18,2)                                         as "Exclusive"
    ,tax.TAX::NUMBER(18,2)                                   as "Fee Tax"
    ,0::NUMBER(18,2)                                         as "Tax Total"
   FROM DATAWAREHOUSE.FEETAX_FACT                            tax
     INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                    chk
        ON chk.CHEQUE_FACT_NK = tax.CHEQUE_FACT_FK
          AND chk.STATUS = ''Closed''
          AND chk.OPENED_AT is not null
          AND chk.DW_ISCURRENTROW          
          AND tax.DW_ISCURRENTROW  
          AND NOT tax.DW_ISDELETED
          AND NOT tax.IS_TRAINING
          AND tax.FISCAL_DATE::date
              >= :startdate::date 
          AND tax.FISCAL_DATE::date  
              <= :enddate::date 
          AND tax.LOCATION_DIM_FK IN ( 
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1);
                  
  CREATE TEMP TABLE TEMP_TABLE2 AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));                  
;                       

--=========================================================================================
 reportSet:= (
   SELECT 
     TO_CHAR(ROW_NUMBER() OVER (ORDER BY INLT."Location ID"))   AS  "Support ID" 
       ,INLT."Location ID"                                      AS "Location ID"
       ,INLT."Rate Name"                                        AS "Rate Name"
       ,INLT."Rate"                                             AS "Rate"
       ,SUM(INLT."Inclusive")::NUMBER(18,2)                     AS "Inclusive"
       ,SUM(INLT."Exclusive")::NUMBER(18,2)                     AS "Exclusive"
       ,SUM(INLT."Fee Tax")::NUMBER(18,2)                       AS "Fee Tax"
       ,SUM(IFNULL(INLT."Tax Total",0) + IFNULL(INLT."Fee Tax",0) )::NUMBER(18,2)   AS "Tax Total" 

     FROM (
     SELECT * from TEMP_TABLE1
       UNION ALL
     SELECT * from TEMP_TABLE2
     ) INLT
  GROUP BY "Location ID"
     ,"Rate Name"
     ,"Rate"
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';