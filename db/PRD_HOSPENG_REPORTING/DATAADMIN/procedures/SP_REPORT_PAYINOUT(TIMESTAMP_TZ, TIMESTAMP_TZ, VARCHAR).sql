CREATE OR REPLACE PROCEDURE "SP_REPORT_PAYINOUT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-01-02T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-02T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--======================================================================================================
BEGIN
reportSet   := (
SELECT TO_CHAR(PIF.PAYINOUT_FACT_NK)                    AS "Support ID" 
        , ''PAY-'' ||row_number() over (order by PIF.PAYINOUT_FACT_NK) 
                                                        AS "Detail ID" 
--status, category, level-------------------------------------------------------------------------------
   ,PIF.STATUS                                          AS "Status"
   ,PIR.TYPE                                            AS "Pay Type"  --PAY IN or PAY OUT
--geography---------------------------------------------------------------------------------------------
   ,loc.LOCATIONNAME                                    AS "Location"   
   ,loc.LOCATION_DIM_NK                                 AS "Location ID"     
--dates-------------------------------------------------------------------------------------------------
  ,LOC.TZ_NAME                                          AS "Time Zone"
  ,to_char(LEFT(SHD.FISCAL_DAY,4))                      AS "Year"
  ,to_char(YEAR(SHD.FISCAL_DAY)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(SHD.FISCAL_DAY),2))                                       
                                                        AS "Year and Month"

   ,to_char(SHD.FISCAL_DAY)                             AS "Fiscal Date"      

    
          -- ,PIF.CREATED_AT::timestamp_ntz  
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,PIF.CREATED_AT::timestamp_ntz )::timestamp)             
                                                        AS "Applied At"

    ,DATE_PART(epoch_second,PIF.CREATED_AT)::DECIMAL(18,0)  
                                                        AS "Applied At UTC"
                                                           
    ,IFNULL(DAYNAME(SHD.FISCAL_DAY),''None'')             AS"Day of Week"
    ,CASE WHEN DAYNAME(SHD.FISCAL_DAY) IN (''Sat'',''Sun'')  
             THEN TRUE ELSE FALSE END                   AS "Is Weekend"
 --flags-------------------------------------------------------------------------------------------------
  ,PIF.IS_VOID                                          AS "Is Voided"           --Was it Voided 
 --people------------------------------------------------------------------------------------------------
 ,EMD.EMPLOYEE_NAME                                     AS "Employee"
 --Descriptors------------------------------------------------------------------------------------------- 
  ,PIR.PAYINPAYOUTREASON                                AS "Reason"              --Paid In or Out Reason
  ,PIF.NOTES                                            AS "Note"                --Associated Note
  ,PAD.PAYMENTMETHODNAME                                AS "Payment Method"      --Which Payment Method was used 
  ,CCT.CARD_TYPE                                        AS "Card Brand"
  ,REPLACE(PAD.PAYMENTMETHODTYPE,''EPX'',''Credit Card'')   AS "Type"
  ,TO_CHAR(PIF.SHIFT_ID)                                AS "Shift"
--Facts-------------------------------------------------------------------------------------------------  
  ,1::NUMBER(18,0)                                      AS "Count"
  ,PIF.AMOUNT::DECIMAL(18,2)                            AS "Amount"
  ,CASE WHEN PIF.IS_VOID 
    THEN PIF.AMOUNT ELSE 0 END ::DECIMAL(18,2)          AS "Void Amount"         --If applicable  
 --------------------------------------------------------------------------------------------------------  
FROM DATAADMIN.PAYINOUT_FACT                                                PIF
 INNER JOIN SHIFT_DIM                                                       SHD
     ON SHD.SHIFT_DIM_NK = PIF.SHIFT_DIM_FK
      AND SHD.DW_ISCURRENTROW
      AND PIF.STATUS = ''Success''      
      AND PIF.DW_ISCURRENTROW
      AND NOT PIF.IS_VOID
      AND NOT PIF.DW_ISDELETED
      AND SHD.FISCAL_DAY::DATE >= :startdate::DATE
      AND SHD.FISCAL_DAY::DATE <= :enddate::DATE
      AND SHD.LOCATION_DIM_FK in (
          SELECT table1.value 
            FROM table(split_to_table(:locationidS, '',''))  table1)
  INNER JOIN DATAADMIN.LOCATION_DIM                                         LOC
    ON LOC.LOCATION_DIM_NK = SHD.LOCATION_DIM_FK
      AND LOC.DW_ISCURRENTROW
  INNER JOIN DATAADMIN.PAYMENTMETHOD_DIM                                    PAD
    ON PIF.PAYMENTMETHOD_DIM_FK = PAD.PAYMENTMETHOD_DIM_NK 
      AND PAD.DW_ISCURRENTROW
   INNER JOIN PAYINPAYOUTREASON_DIM                                         PIR
     ON PIF.PAYINPAYOUTREASON_DIM_FK = PIR.PAYINPAYOUTREASON_DIM_PK
      AND PIR.DW_ISCURRENTROW 
   INNER JOIN EMPLOYEE_DIM                                                  EMD
     ON SHD.EMPLOYEE_DIM_FK = EMD.EMPLOYEE_DIM_NK
      AND EMD.DW_ISCURRENTROW
   LEFT JOIN CCTRANSACTION_FACT                                             CCT
     ON CCT.CCTRANSACTION_FACT_NK = PIF.CCTRANSATION_FACT_FK
       AND CCT.DW_ISCURRENTROW
--==========================================================================================
); 
 RETURN TABLE(reportSet); 
END';