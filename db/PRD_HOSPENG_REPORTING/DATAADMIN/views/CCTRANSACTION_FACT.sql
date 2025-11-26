create or replace view CCTRANSACTION_FACT(
	CCTRANSACTION_FACT_PK,
	CCTRANSACTION_FACT_NK,
	TRANSACTION_NUMBER,
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
	BATCH_DIM_FK,
	LOCATION_DIM_FK,
	EMPLOYEE_DIM_FK,
	PAYMENTMETHOD_DIM_FK,
	CHEQUE_FACT_FK,
	ISCARDPRESENT,
	ISFIRSTTRANSACTION,
	FISCAL_DAY,
	AUTH_TRAN_GMT,
	CREATED_AT,
	UPDATED_AT,
	AUTH_TRAN_ID,
	BATCH_ID,
	AUTH_BRIC,
	CARDHOLDER_LOCATION,
	CARD_ENTRY_METHOD,
	BATCH_NUMBER,
	AUTHORIZATION,
	AUTHORIZATION_CODE,
	APPROVAL,
	POS_TERMINAL_ID,
	MASKED_CC_NUMBER,
	CARDHOLDER_NAME,
	CARD_TYPE,
	CARD_TYPE_RAW,
	REFERENCE_NUMBER,
	COMMAND,
	STATUS,
	AUTHAMOUNTREQUESTED,
	AUTH_AMOUNT,
	TOTAL_INCREMENT,
	TIP_INCREMENT,
	TOTAL,
	TIP
) as
--============================================================================================
-- SELECT * FROM DATAADMIN.CCTRANSACTION_FACT WHERE MTLN_CDC_LAST_CHANGE_TYPE = 'd' ;
SELECT 
--primary keys------------------------------------------------------------------------------
  cct.ID                                                     AS CCTRANSACTION_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,cct.ID                                                    AS CCTRANSACTION_FACT_NK
--name---------------------------------------------------------------------------------------
  ,TO_NUMBER(cct.TRANSACTION_NUMBER)                         AS TRANSACTION_NUMBER
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY cct.ID ORDER BY cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,cct.MTLN_CDC_SEQUENCE_NUMBER,cct.MTLN_CDC_SRC_VERSION
      ,cct.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE        
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY cct.ID ORDER BY cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,cct.MTLN_CDC_SEQUENCE_NUMBER,cct.MTLN_CDC_SRC_VERSION,cct.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY cct.ID ORDER BY 
    cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP,cct.MTLN_CDC_SEQUENCE_NUMBER,cct.MTLN_CDC_SRC_VERSION
   ,cct.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE           
  
  ,CASE WHEN cct.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED              
  ,CASE WHEN RANK() OVER(PARTITION BY cct.ID ORDER BY  
    cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,cct.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,cct.MTLN_CDC_SRC_VERSION DESC
   ,cct.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW      
--CDC Meta data-------------------------------------------------------------------------------
  ,cct.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,cct.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,cct.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,cct.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,cct.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,cct.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,cct.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,cct.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,cct.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,cct.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,cct.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,cct.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,CASE WHEN cct.BATCH_NUMBER IS NULL THEN -1
    ELSE  cct.BATCH_NUMBER || '.' || TO_CHAR(cct.LOCATION_ID) 
    END
                                       AS BATCH_DIM_FK  
  ,IFNULL(cct.LOCATION_ID,-1)          AS LOCATION_DIM_FK
  ,IFNULL(cct.EMPLOYEE_ID,-1)          AS EMPLOYEE_DIM_FK
  ,IFNULL(cct.PAYMENT_METHOD_ID,-1)    AS PAYMENTMETHOD_DIM_FK
  ,IFNULL(cct.CHECK_ID,-1)             AS CHEQUE_FACT_FK
--flags---------------------------------------------------------------------------------------
  ,CASE WHEN cct.cardholder_location = 'ManualPhone' THEN FALSE ELSE TRUE END
                                       AS ISCARDPRESENT
  ,CASE WHEN 1=1 THEN TRUE ELSE FALSE END 
                                       AS ISFIRSTTRANSACTION
--Dates--------------.------------------------------------------------------------------------

,'20' || SUBSTRING(REPLACE(parse_json(parse_json(payment_provider_response_body):data):batchID,'"',''),1,2) 
 || '-' || SUBSTRING(REPLACE(parse_json(parse_json(payment_provider_response_body):data):batchID,'"',''),3,2)
 || '-' || SUBSTRING(REPLACE(parse_json(parse_json(payment_provider_response_body):data):batchID,'"',''),5,2)
                                        AS FISCAL_DAY
                                                                                    , to_timestamp_ntz(REPLACE(parse_json(parse_json(payment_provider_response_body):data):authTranDateGMT,'"',''),'MM/DD/YYYY HH12:MI:SS PM') 
                                       AS AUTH_TRAN_GMT
  ,to_timestamp_tz(cct.CREATED_AT)     AS CREATED_AT   
  ,to_timestamp_tz(cct.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,REPLACE(parse_json(parse_json(payment_provider_response_body):data):authTranID,'"','') AS AUTH_TRAN_ID
  ,REPLACE(parse_json(parse_json(payment_provider_response_body):data):batchID,'"','')    AS BATCH_ID
  ,REPLACE(parse_json(parse_json(payment_provider_response_body):data):authBric,'"','')   AS AUTH_BRIC
  ,cct.CARDHOLDER_LOCATION             AS CARDHOLDER_LOCATION
  ,cct.CARD_ENTRY_METHOD               AS CARD_ENTRY_METHOD
  ,cct.BATCH_NUMBER                    AS BATCH_NUMBER
  ,REPLACE(parse_json(parse_json(payment_provider_response_body):data):authorization,'"','') 
                                       AS AUTHORIZATION
  ,cct.AUTHORIZATION_CODE              AS AUTHORIZATION_CODE
  ,REPLACE(parse_json(parse_json(payment_provider_response_body):data):text,'"','') 
                                       AS APPROVAL
  ,cct.POS_TERMINAL_ID                 AS POS_TERMINAL_ID
  ,cct.MASKED_CC_NUMBER                AS MASKED_CC_NUMBER
  ,cct.CARDHOLDER_NAME                 AS CARDHOLDER_NAME
  ,cct.CARD_TYPE                       AS CARD_TYPE
  ,cct.CARD_TYPE_RAW                   AS CARD_TYPE_RAW
  ,cct.REFERENCE_NUMBER                AS REFERENCE_NUMBER
  ,cct.COMMAND                         AS COMMAND
  ,cct.STATUS                          AS STATUS
  
--Counts and Amounts--------------------------------------------------------------------------
  ,TO_DECIMAL(TRY_PARSE_JSON(payment_provider_response_body):data:authAmountRequested,38,4)
                                                                        AS AUTHAMOUNTREQUESTED
  ,REPLACE(parse_json(parse_json(payment_provider_response_body):data):authAmount,'"','') ::DECIMAL(38,4)
                                                                        AS AUTH_AMOUNT                                                                        
  ,cct.total_increment/1000000 ::DECIMAL(38,4)                          AS TOTAL_INCREMENT
  ,cct.tip_increment/1000000 ::DECIMAL(38,4)                            AS TIP_INCREMENT
  ,cct.Total/1000000  ::DECIMAL(38,4)                                   AS TOTAL
  ,cct.Tip  /1000000  ::DECIMAL(38,4)                                   AS TIP
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CC_TRANSACTION     cct
  WHERE NOT COALESCE(cct.TRUNCATED,FALSE)
       AND cct.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;