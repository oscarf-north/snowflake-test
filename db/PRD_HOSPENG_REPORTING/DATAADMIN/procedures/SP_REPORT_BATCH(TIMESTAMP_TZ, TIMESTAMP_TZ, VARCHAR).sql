CREATE OR REPLACE PROCEDURE "SP_REPORT_BATCH"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,3,2]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--===============================================================================================================
BEGIN
 reportSet := (
        SELECT cct.CCTRANSACTION_fact_nk                 as "Support ID" 
         , to_char(cct.CCTRANSACTION_fact_nk)            as "Support ID STR" 
         , ''BAT-'' ||row_number() over (order by cct.CCTRANSACTION_fact_nk) 
                                                         as "Detail ID"        
--status, category, level-------------------------------------------------------------------
          ,IFNULL(cct.status,''None'')                      as "Status"       
-- --geography--------------------------------------------------------------------------------      
          ,IFNULL(loc.locationname,''None'')                as "Location"
          ,CCT.LOCATION_DIM_FK::decimal(36,0)             as "Location ID"          
 --dates-------------------------------------------------------------------------------------
     ,LOC.TZ_NAME                                         as "Time Zone"
    ,to_char(LEFT(cct.FISCAL_DAY,4))                      as "Year"
    ,to_char(YEAR(cct.FISCAL_DAY::DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(cct.FISCAL_DAY::DATE),2))
                                                          as "Year and Month"
    ,to_char(cct.FISCAL_DAY)                              as "Fiscal Day"
    ,IFNULL(DAYNAME(cct.FISCAL_DAY),''None'')               as "Day of Week"
    ,CASE WHEN DAYNAME(cct.FISCAL_DAY) IN (''Sat'',''Sun'')  
          THEN TRUE ELSE FALSE END                        as "Is Weekend"       
     ,left(to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CCT.AUTH_TRAN_GMT::timestamp_ntz )::timestamp ),16)
                                                          as "Transaction Date"
-- --people-------------------------------------------------------------------------------------
    ,IFNULL(emd.EMPLOYEE_NAME,''None'')                     as "Employee"
    ,CASE WHEN to_char(cct.COMMAND) = ''Capture''
      AND cct.status = ''Success''
     THEN TRUE ELSE FALSE END                             as "Valid Transaction"
-- --Descriptors--------------------------------------------------------------------------------    
          ,IFNULL(to_char(cct.BATCH_DIM_FK),''None'')       as "Batch"
          ,IFNULL(to_char(cct.COMMAND),''None'')            as "Type"
          ,IFNULL(to_char(cct.TRANSACTION_NUMBER),''None'') as "Transaction NO"
          ,IFNULL(to_char(cct.APPROVAL),''None'')           as "Approval"
          ,IFNULL(to_char(cct.AUTH_BRIC),''None'')          as "BRIC"
          ,IFNULL(to_char(cct.CARD_ENTRY_METHOD),''None'')  as "Card Entry"
          ,IFNULL(to_char(cct.POS_TERMINAL_ID),''None'')    as "Terminal ID"
          ,IFNULL(to_char(cct.REFERENCE_NUMBER),''None'')   as "Reference Number"
          ,IFNULL(to_char(cct.AUTHORIZATION_CODE),''None'') as "Auth Code"
          ,IFNULL(TO_CHAR(cct.CARDHOLDER_LOCATION),''None'')as "Cardholder Location"
          ,IFNULL( cct.CARD_TYPE ,''None'')                 as "Card Brand"            
          ,IFNULL(cct.CARDHOLDER_NAME,''None'')             as "Cardholder Name"
          ,IFNULL(cct.MASKED_CC_NUMBER,''None'')            as "Card Last 4 Digits"
                                                      
--  --Facts--------------------------------------------------------------------------------------  
         ,1::NUMBER(18,0)                                 as "Count"
         ----------------------------------------------------------------------------------------
         ,case when cct.COMMAND = ''Capture''
           THEN cct.TOTAL END::DECIMAL(18,2)              as "Credit Card Total"
          ,case when cct.STATUS = ''Declined''
           THEN cct.TOTAL END::DECIMAL(18,2)              as "Declined Total"
          ,case when cct.COMMAND = ''Refund'' AND cct.STATUS = ''Success''
           THEN cct.TOTAL END::DECIMAL(18,2)              as "Refund Total"
         ,case when cct.COMMAND = ''Void''   AND cct.STATUS = ''Success''
           THEN cct.TOTAL END::DECIMAL(18,2)              as "Void Total"           
         ----------------------------------------------------------------------------------------
         ,case when cct.CARD_TYPE = ''Visa'' and cct.COMMAND = ''Capture''
               then cct.TOTAL end::DECIMAL(18,2)          as "Visa Total"           --Gross sales of al Visa trans
          ,case when cct.CARD_TYPE = ''American Express'' and cct.COMMAND = ''Capture''
               then cct.TOTAL end::DECIMAL(18,2)          as "Amex Total"           --Gross sales of all Amex transactions
          ,case when cct.CARD_TYPE = ''Mastercard'' and cct.COMMAND = ''Capture''
              then cct.TOTAL end::DECIMAL(18,2)           as "Mastercard Total"     --Gross sales of all Mastercard transactions
          ,case when cct.CARD_TYPE = ''Discover'' and cct.COMMAND = ''Capture''
            then cct.TOTAL end::DECIMAL(18,2)             as "Discover Total"       --Gross sales of all Discover transactions
          ,case when cct.CARD_TYPE= ''Citi'' and cct.COMMAND = ''Capture''
               then cct.TOTAL end::DECIMAL(18,2)          as "Citi Total"   
         ----------------------------------------------------------------------------------------               
          ,cct.TIP::DECIMAL(18,2)                         as "Tip"   
          ,cct.TOTAL::DECIMAL(18,2)                       as "Total"     
        FROM DATAWAREHOUSE.CCTRANSACTION_FACT                 cct
          INNER JOIN DATAWAREHOUSE.location_DIM               loc
            ON cct.location_DIM_FK = loc.location_DIM_NK
              AND cct.FISCAL_DAY::date >= :startdate::date 
              AND cct.FISCAL_DAY::date <= :enddate::date  
              AND (cct.COMMAND in (''Capture'',''Refund'',''Void'',''GoodFaith'',''StoreAndForward'')
                AND cct.STATUS in (''Success'', ''Declined'')
                )
              AND cct.LOCATION_DIM_FK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
              AND cct.dw_iscurrentrow
              AND loc.dw_iscurrentrow
              AND NOT cct.dw_isdeleted
          LEFT JOIN DATAWAREHOUSE.employee_DIM                     emd
            ON emd.employee_DIM_NK = cct.EMPLOYEE_DIM_FK
              AND emd.dw_iscurrentrow                          
--===============================================================================================================
); 
 RETURN TABLE(reportSet); 
END';