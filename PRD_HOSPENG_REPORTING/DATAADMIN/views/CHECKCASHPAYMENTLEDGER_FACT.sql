create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.CHECKCASHPAYMENTLEDGER_FACT(
	CHECKCASHPAYMENTLEDGER_FACT_PK,
	CHECKCASHPAYMENTLEDGER_FACT_NK,
	PAYMENT,
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
	SHIFT_DIM_FK,
	PAYMENTMETHOD_DIM_FK,
	PAYMENTS_FACT_FK,
	IS_VOID,
	CREATED_AT,
	UPDATED_AT,
	AMOUNT_TENDERED,
	AMOUNT_CHANGED
) as
-- --===============================================================================================
SELECT 
--primary keys-------------------------------------------------------------------------------------
  CCP.ID    
                                                                 AS CHECKCASHPAYMENTLEDGER_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,CCP.ID  
                                                                 AS CHECKCASHPAYMENTLEDGER_FACT_NK
--name---------------------------------------------------------------------------------------
  ,CCP.ID                                                        AS PAYMENT
--data warehouse rows------------------------------------------------------------------------
,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY CCP.ID ORDER BY CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CCP.MTLN_CDC_SEQUENCE_NUMBER,CCP.MTLN_CDC_SRC_VERSION
      ,CCP.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY CCP.ID ORDER BY CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CCP.MTLN_CDC_SEQUENCE_NUMBER,CCP.MTLN_CDC_SRC_VERSION,CCP.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY CCP.ID ORDER BY 
    CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CCP.MTLN_CDC_SEQUENCE_NUMBER,CCP.MTLN_CDC_SRC_VERSION
   ,CCP.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          
  
  ,CASE WHEN CCP.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED            
  ,CASE WHEN RANK() OVER(PARTITION BY CCP.ID ORDER BY  
    CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CCP.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CCP.MTLN_CDC_SRC_VERSION DESC
   ,CCP.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW      
--CDC Meta data-------------------------------------------------------------------------------
  ,CCP.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,CCP.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,CCP.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,CCP.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,CCP.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,CCP.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,CCP.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,CCP.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,CCP.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,CCP.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,CCP.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,CCP.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(CCP.CHECK_id,-1)             AS CHEQUE_FACT_FK     
  ,IFNULL(CCP.SHIFT_ID,-1)             AS SHIFT_DIM_FK 
  ,IFNULL(CCP.PAYMENT_METHOD_ID,-1)    AS PAYMENTMETHOD_DIM_FK  
  ,IFNULL(CCP.CHECK_id || '.' || CCP.CHECK_PAYMENT_UID,'-1')  
                                       AS PAYMENTS_FACT_FK
--flags---------------------------------------------------------------------------------------
  ,CCP.IS_VOIDED                       AS IS_VOID
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CCP.CREATED_AT)     AS CREATED_AT   
  ,to_timestamp_tz(CCP.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
  ,CCP.AMOUNT_TENDERED/1000000         AS AMOUNT_TENDERED 
  ,CCP.AMOUNT_CHANGED/1000000          AS AMOUNT_CHANGED  
  -------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHECK_CASH_PAYMENT_LEDGER  CCP
;
