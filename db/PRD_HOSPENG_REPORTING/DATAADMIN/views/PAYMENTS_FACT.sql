create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.PAYMENTS_FACT(
	PAYMENTS_FACT_PK,
	PAYMENTS_FACT_NK,
	PAYMENTNUMBER,
	DW_STARTDATE,
	DW_ENDDATE,
	DW_ISDELETED,
	DW_ISCURRENTROW,
	MTLN_CDC_LAST_CHANGE_TYPE,
	MTLN_CDC_LAST_COMMIT_TIMESTAMP,
	MTLN_CDC_SEQUENCE_NUMBER,
	MTLN_CDC_LOAD_BATCH_ID,
	MTLN_CDC_LOAD_TIMESTAMP,
	MTLN_CDC_PROCESSED_DATE_HOUR,
	MTLN_CDC_SRC_VERSION,
	MTLN_CDC_FILENAME,
	MTLN_CDC_FILEPATH,
	MTLN_CDC_SRC_DATABASE,
	MTLN_CDC_SRC_SCHEMA,
	MTLN_CDC_SRC_TABLE,
	CHEQUE_FACT_FK,
	DAYPART_DIM_FK,
	EMPLOYEE_DIM_FK_AS_CREATOR,
	EMPLOYEE_DIM_FK_AS_PAYEE,
	LOCATION_DIM_FK,
	ORDERTYPE_DIM_FK,
	PAYMENTMETHOD_DIM_FK,
	REVENUECENTER_DIM_FK,
	TRANSACTION_FACT_FK,
	TERMINAL_DIM_FK,
	SHOULD_VERIFYID,
	IS_PARTIALAPPROVAL,
	IS_TRAINING,
	CREATED_AT,
	UPDATED_AT,
	OPENED_AT,
	PAID_AT,
	FISCALDATE,
	SEQ,
	BATCHNUMBER,
	CARDBRAND,
	CARDHOLDERNAME,
	CHEQUENUMBER,
	CURRENCY_ID,
	FLOORPLAN_ID,
	LASTFOURCCNUMBER,
	NEXTFOURCCNUMBER,
	PAYMENTTYPE,
	PAYMENTSTATUS,
	REVENUECENTERNAME,
	TABLENAME,
	PARTYSIZE,
	TIP,
	AMOUNTAPPLIEDTOCHECK,
	CHANGE,
	TOTAL
) as
-- --============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------ 
   CHK.ID || '.'  || REPLACE(PAY.value:id,'"','')                        AS PAYMENTS_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,CHK.ID || '.'  || REPLACE(PAY.value:id,'"','')                        AS PAYMENTS_FACT_NK
--name---------------------------------------------------------------------------------------
  ,RIGHT('000000000000000' || PAY.INDEX, 16)                             AS PAYMENTNUMBER
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY 
        CHK.ID || '.'  || REPLACE(PAY.value:id,'"','') 
        ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                         AS DW_STARTDATE       
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
    CHK.ID || '.'  || REPLACE(PAY.value:id,'"','') 
    ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY CHK.ID || '.'  || REPLACE(PAY.value:id,'"','') ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                         AS DW_ENDDATE          
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                             AS DW_ISDELETED             
  ,CASE WHEN RANK() OVER(PARTITION BY  CHK.ID || '.'  || REPLACE(PAY.value:id,'"','') 
    ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                          AS DW_ISCURRENTROW     
--CDC Meta data-------------------------------------------------------------------------------
  ,CHK.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,CHK.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,CHK.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,CHK.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,CHK.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,CHK.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,CHK.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,CHK.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,CHK.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,CHK.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,CHK.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(CHK.ID,-1)                               AS CHEQUE_FACT_FK
  ,IFNULL(CHK.DAY_PART_ID,-1)                      AS DAYPART_DIM_FK
  ,IFNULL(CHK.EMPLOYEE_ID,-1)                      AS EMPLOYEE_DIM_FK_AS_CREATOR
  ,IFNULL(TO_NUMBER(PAY.value:employeeId),-1)      AS EMPLOYEE_DIM_FK_AS_PAYEE
  ,IFNULL(CHK.LOCATION_ID,-1)                      AS LOCATION_DIM_FK
  ,IFNULL(CHK.order_type_id,-1)                    AS Ordertype_DIM_FK    
  ,IFNULL(TO_NUMBER(PAY.value:paymentMethodId),-1) AS PAYMENTMETHOD_DIM_FK
  ,IFNULL(TO_NUMBER(replace(TRY_PARSE_JSON(CHK.info):revenueCenterId,'"','')),-1)
                                                   AS REVENUECENTER_DIM_FK 
  ,IFNULL(TO_NUMBER(replace(PAY.value:ccData:transactionID,'"','')),-1)  
                                                   AS TRANSACTION_FACT_FK
  ,IFNULL(PAY.value:applicationSettings.posTerminalId::INT,-1)  
                                                   AS TERMINAL_DIM_FK                                
-- --flags---------------------------------------------------------------------------------------
  ,replace(PAY.value:ccData:shouldVerifyId,'"','')   ::boolean          AS SHOULD_VERIFYID
  ,replace(PAY.value:ccData:isPartialApproval,'"','')::boolean          AS IS_PARTIALAPPROVAL
  ,CHK.IS_TRAINING                                                      AS IS_TRAINING
-- -- --Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.CREATED_AT)                                      AS CREATED_AT   
  ,to_timestamp_tz(CHK.UPDATED_AT)                                      AS UPDATED_AT
 -- ,to_timestamp_tz(replace(value:ccData:expirationDate,'"',''))       AS EXPIRATIONDATE   
  ,to_timestamp_tz(CHK.opened_at)                                       AS OPENED_AT
  ,to_timestamp_tz(to_char((PAY.value:paidAt)))                         AS PAID_AT
  ,TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate)::DATE                   AS FISCALDATE
-- --names, options, etc-------------------------------------------------------------------------
  ,seq                                                                  AS SEQ
  ,replace(value:ccData:batchNumber,'"','')                             AS BATCHNUMBER
  -- ,initcap(replace(value:ccData:cardBrand,'"',''))   

  ,case when value:ccData:cardBrand = '' or value:ccData:cardBrand is null then 'None' 
    else initcap(replace(value:ccData:cardBrand,'"','')) end 
  
  
                                                                        AS CARDBRAND
  ,replace(value:ccData:cardholderName,'"','')                          AS CARDHOLDERNAME
  ,CHK.NUMBER                                                           AS CHEQUENUMBER
  ,replace(TRY_PARSE_JSON(info):currencyId,'"','')                      AS CURRENCY_ID  
  ,TO_NUMBER(replace(TRY_PARSE_JSON(info):floorplanId,'"',''))          AS FLOORPLAN_ID
  ,replace(value:ccData:lastFourCcNumber,'"','')                        AS LASTFOURCCNUMBER  
  ,replace(value:ccData:nextFourCcNumber,'"','')                        AS NEXTFOURCCNUMBER
  ,replace(value:paymentType,'"','')                                    AS PAYMENTTYPE
  ,replace(value:paymentStatus,'"','')                                  AS PAYMENTSTATUS
  ,replace(TRY_PARSE_JSON(info):revenueCenterName,'"','')               AS REVENUECENTERNAME
  ,replace(TRY_PARSE_JSON(info):tableName,'"','')                       AS TABLENAME
-- --Counts and Amounts--------------------------------------------------------------------------
   ,replace(TRY_PARSE_JSON(info):partySize,'"','')::NUMBER(38)       
                                                                        AS PARTYSIZE
   ,PAY.value:tip::NUMBER(38,4)                                         AS TIP
   ,PAY.value:amountAppliedToCheck::NUMBER(38,4)                        AS AMOUNTAPPLIEDTOCHECK
   ,PAY.value:change::NUMBER(38,4)                                      AS CHANGE   
   ,PAY.value:total::NUMBER(38,4)                                       AS TOTAL 
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE             CHK 
    ,LATERAL FLATTEN(INPUT => 
    PARSE_JSON( '{PAYMENTS:' || CHK.PAYMENTS || '}' ),  path => 'PAYMENTS')
                                                                        PAY
  WHERE CHK.payments <> '' 
   AND CHK.PAYMENTS <> '__value_not_modified__'  
   AND NOT COALESCE(TRUNCATED,FALSE)
   AND MTLN_CDC_LAST_CHANGE_TYPE <> 'd'
;
