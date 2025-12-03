CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_PAYMENTS_FEETAX"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
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

 CREATE TEMP TABLE  TEMP_TABLE1 AS
 SELECT ''Tax Detail Fee''                                     as "Level"
    ,tax.FISCAL_DATE::date                                   as "Fiscal Day"
    ,tax.LOCATION_DIM_FK                                     as "Location ID"
    ,IFNULL(tax.TAXRATENAME,'' None'')                         as "Rate Name"
    ,tax.TAX::NUMBER(18,2)                                   as "Fee Tax Amount"
    ,tax.PERCENT::NUMBER(18,2)                               as "Tax Percent"
 FROM DATAWAREHOUSE.FEETAX_FACT                              tax
     INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                    chk
        ON chk.CHEQUE_FACT_NK = tax.CHEQUE_FACT_FK
          AND chk.TAX > 0.000
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
                  FROM table(split_to_table(:locationidS, '',''))  table1)
;                  
       
--=========================================================================================
 reportSet:= (
 SELECT * from TEMP_TABLE1
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';