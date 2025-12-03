CREATE OR REPLACE PROCEDURE "SP_LOAD_365_TAXDETAILS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2025-01-16'';  
  -- enddate timestamp_tz   := ''2025-01-16''; 
  -- locationid string      := ''[351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=================================================================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_tax; 
  DROP TABLE IF EXISTS TEMP_feetax;   
  DROP TABLE IF EXISTS TEMP_header; 

-----------------------------------------------------------------------------------------------------------------------------------
 --Create empty table This has a row with all zero values.Sent when there are no tax details because 365 requires a file even when there is no data.
 SELECT ''CheckNumber'' as "CheckNumber"
  ,''Tax_TaxNumber''  as "Tax_TaxNumber"
  ,''Tax_Amount''  as "Tax_Amount"
  ;
  
  CREATE TEMP TABLE TEMP_header AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-----------------------------------------------------------------------------------------------------------------------------------
--grab the actual data  
  SELECT 
  to_char(itf.chequenumber) || ''.'' || to_char(itf.cheque_fact_fk)  AS CheckNumber          --* string
  ,to_char(tax.TAX_RATE_ID)                                        AS Tax_TaxNumber        --* string
  ,to_char(tax.AMOUNT::NUMBER(36,2))                               AS Tax_Amount           --* decimal
  FROM DATAWAREHOUSE.TAX_FACT                               tax
     INNER JOIN DATAWAREHOUSE.ITEM_FACT                     itf
        ON itf.ITEM_FACT_NK = tax.ITEM_FACT_FK
          AND itf.DW_ISCURRENTROW
          AND tax.ITEMSTATUS IN (''Added'',''Sent'')
          AND tax.CHECKSTATUS = ''Closed''
          AND tax.OPENED_AT is not null
          AND tax.DW_ISCURRENTROW  
          AND NOT tax.DW_ISDELETED
          AND NOT tax.IS_TRAINING
          AND tax.AMOUNT > 0.00
          AND tax.FISCAL_DATE::date
              >= :startdate::date 
          AND tax.FISCAL_DATE::date  
              <= :enddate::date 
          AND tax.LOCATION_DIM_FK IN ( 
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)  
   ORDER BY tax.FISCAL_DATE;

  CREATE TEMP TABLE TEMP_tax AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
     
------------------------------------------------------------------------------------------------------------------------------------
SELECT 
  to_char(chk.chequenumber) || ''.'' || to_char(chk.CHEQUE_FACT_NK)  AS CheckNumber          --* string
  ,to_char(tax.TAXRATEDIM_DIM_FK::DECIMAL(18,0))                   AS Tax_TaxNumber        --* string
  ,to_char(tax.TAX::NUMBER(36,2))                                  AS Tax_Amount           --* decimal
  FROM DATAWAREHOUSE.FEETAX_FACT                                   tax
     INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                          chk
        ON chk.CHEQUE_FACT_NK = tax.CHEQUE_FACT_FK
          AND chk.DW_ISCURRENTROW
          AND chk.STATUS = ''Closed''
          AND tax.OPENED_AT is not null
          AND tax.DW_ISCURRENTROW  
          AND NOT tax.DW_ISDELETED
          AND NOT tax.IS_TRAINING
          AND tax.TAX > 0.00
          AND chk.FISCAL_DATE::date
              >= :startdate::date 
          AND chk.FISCAL_DATE::date  
              <= :enddate::date 
          AND chk.LOCATION_DIM_FK IN ( 
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)  
   ORDER BY chk.FISCAL_DATE;

  CREATE TEMP TABLE TEMP_feetax AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));     
--=================================================================================================================================
reportSet := (  
------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM TEMP_header  
  UNION ALL
SELECT * FROM TEMP_tax
  UNION ALL
SELECT * FROM TEMP_feetax

--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';