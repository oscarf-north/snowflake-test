CREATE OR REPLACE PROCEDURE "SP_REPORT_REFUNDS"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[3,2,351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');

--===================================================================================================================
BEGIN
 reportSet   := (
 
        SELECT ref.refunds_fact_nk                      as "Support ID" 
         , ''REF-'' ||row_number() over (order by ref.refunds_fact_nk) 
                                                        as "Detail ID"        
--status, category, level-------------------------------------------------------------------
--geography---------------------------------------------------------------------------------   
          ,IFNULL(loc.locationname,''None'')              as "Location"
          ,loc.location_dim_nk                          as "Location ID"          
          ,IFNULL(ref.revenuecentername ,''None'')        as "Revenue Center"
 --dates-------------------------------------------------------------------------------------
           ,LOC.TZ_NAME                                 as "Time Zone" 
  ,to_char(LEFT(chk.FISCAL_DATE,4))                     as "Year"
          ,to_char(YEAR(chk.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(chk.FISCAL_DATE),2))                                       
                                                        as "Year and Month"
          ,IFNULL(dpd.DAYPART,''None'')                   as "Daypart"
          ,to_char(chk.FISCAL_DATE)                     as "Fiscal Date"
          ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,ref.REFUNDED_AT::timestamp_ntz )::timestamp) 
                                                        as "Refunded At"
          ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,ref.OPENED_AT::timestamp_ntz )::timestamp)                                                                            
                                                        as "Opened At"                                                           
          ,IFNULL(DAYNAME(chk.FISCAL_DATE),''None'')      as"Day of Week"
          ,CASE WHEN DAYNAME(chk.FISCAL_DATE) IN (''Sat'',''Sun'')  
             THEN TRUE ELSE FALSE END                   as "Is Weekend"
--people-------------------------------------------------------------------------------------
          ,IFNULL(replace(
            substring(ref.REFUNDED_BY,1,charindex(''('',ref.REFUNDED_BY) - 1)
          ,''User '',''''),''None'')               
                                                        as "Refunded By"    
                                                   
--Descriptors--------------------------------------------------------------------------------        
          ,IFNULL(ptd.paymentmethodname ,''None'')        as "Payment Method"
          ,BATCHNUMBER                                  as "Batch Number"
          ,IFNULL(ref.cardholderName,''None'')            as "Cardholder Name"
          ,IFNULL(ref.LASTFOURCCNUMBER,''None'')          as "Card Last 4 Digits"
          ,IFNULL(case ref.cardbrand when '''' 
             THEN ''Not a Credit Card'' 
            else ref.cardbrand end,''None'')  
                                                        as "Card Brand" 
         ,ref.CHEQUENUMBER                              as "Check"
         ,chk.CHEQUE_FACT_NK                            as "Check ID"         
 --Facts--------------------------------------------------------------------------------------  
         ,1::NUMBER(10,0)                               as "Count"
         ,TIP::DECIMAL(18,2)                            as "Tip Amount"    
         ,CHECK_TOTAL_AMOUNT::DECIMAL(18,2)             as "Check Amount"           
         ,REFUND_AMOUNT::DECIMAL(18,2)                  as "Refund Amount"  
        FROM DATAWAREHOUSE.REFUNDS_FACT                                 ref
          INNER JOIN DATAWAREHOUSE.location_DIM                         loc
            ON ref.location_DIM_FK = loc.location_DIM_NK
              -- AND ref.opened_at::timestamp_ntz  >= :startdate::timestamp_ntz 
              -- AND ref.opened_at::timestamp_ntz  <= :enddate::timestamp_ntz 
              AND ref.LOCATION_DIM_FK in (
                SELECT table1.value
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
              AND ref.dw_iscurrentrow
              AND loc.dw_iscurrentrow
              AND NOT ref.IS_TRAINING
              AND NOT ref.dw_isdeleted
              AND ref.PAYMENTSTATUS = ''Success''
          INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                           chk
            ON chk.CHEQUE_FACT_NK = ref.CHEQUE_FACT_FK
              AND chk.DW_ISCURRENTROW
              AND NOT chk.dw_isdeleted
              AND chk.STATUS = ''Closed''
              AND (chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date  <= :enddate::date)
          INNER JOIN DATAWAREHOUSE.LOCATION_DIM                          lod
             ON lod.LOCATION_DIM_NK = chk.LOCATION_DIM_FK
               AND lod.DW_ISCURRENTROW
          INNER JOIN DATAWAREHOUSE.PaymentMethod_DIM                     ptd      
            ON ref.PaymentMethod_DIM_FK = ptd.PaymentMethod_DIM_NK
              AND ptd.dw_iscurrentrow
              AND NOT ptd.dw_isdeleted
          INNER JOIN DATAWAREHOUSE.organization_dim                      org
            ON loc.organization_DIM_FK = org.organization_DIM_NK
              AND org.dw_iscurrentrow
              AND NOT org.dw_isdeleted
          INNER JOIN DATAWAREHOUSE.daypart_dim                           dpd
            ON ref.daypart_dim_fk = dpd.daypart_dim_nk
              AND dpd.dw_iscurrentrow
              AND NOT dpd.dw_isdeleted
          -- LEFT JOIN DATAADMIN.employee_DIM                            emd
          --   ON emd.SYSTEM_ID = ref.EMPLOYEE_ID_REFUNDER
          --     AND emd.dw_iscurrentrow 
          --     AND NOT emd.dw_isdeleted
--====================================================================================================================
); 
 RETURN TABLE(reportSet); 
END';