create or replace view REFUNDS_FACT(
	REFUNDS_FACT_PK,
	REFUNDS_FACT_NK,
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
	CCTRANSACTION_FACT_FK,
	CHEQUE_FACT_FK,
	DAYPART_DIM_FK,
	EMPLOYEE_DIM_FK_AS_CREATOR,
	EMPLOYEE_DIM_FK_AS_PAYEE,
	LOCATION_DIM_FK,
	PAYMENTMETHOD_DIM_FK,
	REVENUECENTER_DIM_FK,
	TRANSACTION_FACT_FK,
	TERMINAL_DIM_FK,
	IS_TRAINING,
	OPENED_AT,
	PAID_AT,
	REFUNDED_AT,
	FISCALDATE,
	BATCHNUMBER,
	REFUNDED_BY,
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
	CHECK_TOTAL_AMOUNT,
	REFUND_AMOUNT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------ 
   REPLACE(PAY.value:id,'"','') || '.' || REF.index                      AS REFUNDS_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,REPLACE(PAY.value:id,'"','')  || '.' || REF.index                     AS REFUNDS_FACT_NK
--name---------------------------------------------------------------------------------------
  ,RIGHT('000000000000000' || PAY.INDEX, 16)                             AS PAYMENTNUMBER
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY 
        REPLACE(PAY.value:id,'"','')  || '.' || REF.index
        ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                         AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
    REPLACE(PAY.value:id,'"','')  || '.' || REF.index
    
    ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY REPLACE(PAY.value:id,'"','')  || '.' || REF.index ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                         AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                             AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY  REPLACE(PAY.value:id,'"','')  || '.' || REF.index ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                         AS DW_ISCURRENTROW     --REQUIRED
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
  ,IFNULL(REF.value:ccTransactionId::NUMBER(38,0),-1)         
                                                   AS CCTRANSACTION_FACT_FK
  ,IFNULL(CHK.ID,-1)                               AS CHEQUE_FACT_FK
  ,IFNULL(CHK.DAY_PART_ID,-1)                      AS DAYPART_DIM_FK
  ,IFNULL(CHK.EMPLOYEE_ID,-1)                      AS EMPLOYEE_DIM_FK_AS_CREATOR
  ,IFNULL(TO_NUMBER(PAY.value:employeeId),-1)      AS EMPLOYEE_DIM_FK_AS_PAYEE
  ,IFNULL(CHK.LOCATION_ID,-1)                      AS LOCATION_DIM_FK
  ,IFNULL(TO_NUMBER(PAY.value:paymentMethodId),-1) AS PAYMENTMETHOD_DIM_FK
  ,IFNULL(TO_NUMBER(replace(TRY_PARSE_JSON(CHK.info):revenueCenterId,'"','')),-1)
                                                   AS REVENUECENTER_DIM_FK 
  ,IFNULL(TO_NUMBER(replace(PAY.value:ccData:transactionID,'"','')),-1)  
                                                   AS TRANSACTION_FACT_FK
  ,IFNULL(PAY.value:applicationSettings.posTerminalId::NUMBER,-1)  
                                                   AS TERMINAL_DIM_FK                                
--flags---------------------------------------------------------------------------------------
  ,CHK.IS_TRAINING                                                      AS IS_TRAINING
--Dates--------------.------------------------------------------------------------------------

  ,to_timestamp_tz(CHK.opened_at)                                       AS OPENED_AT
  ,to_timestamp_tz(to_char((PAY.value:paidAt)))                         AS PAID_AT
  ,to_timestamp_tz(REF.value:refundedAt::VARCHAR)                       AS REFUNDED_AT 
  ,to_timestamp_tz(replace(try_PARSE_JSON(info):fiscalDate,'"',''))     AS FISCALDATE
--names, options, etc----------------------------------------------------------- ------------- 
  ,replace(PAY.value:ccData:batchNumber,'"','')                         AS BATCHNUMBER
  ,REF.value:sessionMoniker::VARCHAR                                    AS REFUNDED_BY                        
  ,replace(PAY.value:ccData:cardBrand,'"','')                           AS CARDBRAND
  ,replace(PAY.value:ccData:cardholderName,'"','')                      AS CARDHOLDERNAME
  ,CHK.NUMBER                                                           AS CHEQUENUMBER
  ,replace(TRY_PARSE_JSON(info):currencyId,'"','')                      AS CURRENCY_ID  
  ,TO_NUMBER(replace(TRY_PARSE_JSON(info):floorplanId,'"',''))          AS FLOORPLAN_ID
  ,replace(PAY.value:ccData:lastFourCcNumber,'"','')                    AS LASTFOURCCNUMBER  
  ,replace(PAY.value:ccData:nextFourCcNumber,'"','')                    AS NEXTFOURCCNUMBER
  ,replace(PAY.value:paymentType,'"','')                                AS PAYMENTTYPE
  ,replace(PAY.value:paymentStatus,'"','')                              AS PAYMENTSTATUS
  ,replace(TRY_PARSE_JSON(info):revenueCenterName,'"','')               AS REVENUECENTERNAME
  ,replace(TRY_PARSE_JSON(info):tableName,'"','')                       AS TABLENAME
--Counts and Amounts--------------------------------------------------------------------------
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):total), 38,4)      AS CHECK_TOTAL_AMOUNT
  ,REF.value:refundAmount::NUMBER(38,4)                                 AS REFUND_AMOUNT   
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE             CHK 
    ,LATERAL FLATTEN(INPUT => 
     TRY_PARSE_JSON( '{PAYMENTS:' || CHK.PAYMENTS || '}' ),  path => 'PAYMENTS')
                                                                        PAY
 ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON('{refunds:' || TRY_PARSE_JSON(PAY.value):refunds || '}'), PATH => 'refunds')
                                                                        REF  
WHERE REF.value IS NOT NULL and CHK.PAYMENTS <> '__value_not_modified__'                                                                       
                                                                        ;
;