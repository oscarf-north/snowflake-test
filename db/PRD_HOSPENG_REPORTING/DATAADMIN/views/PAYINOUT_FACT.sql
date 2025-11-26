create or replace view PAYINOUT_FACT(
	PAYINOUT_FACT_PK,
	PAYINOUT_FACT_NK,
	PAYID,
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
	CCTRANSATION_FACT_FK,
	PAYMENTMETHOD_DIM_FK,
	PAYINPAYOUTREASON_DIM_FK,
	SHIFT_DIM_FK,
	IS_VOID,
	VOIDED_AT,
	CREATED_AT,
	UPDATED_AT,
	STATUS,
	VOIDED_BY,
	NOTES,
	CC_TRANSACTION_ID,
	PAY_IN_OUT_REASON_ID,
	SHIFT_ID,
	AMOUNT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  PIO.ID                                                      AS PAYINOUT_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,PIO.ID                                                     AS PAYINOUT_FACT_NK
--name---------------------------------------------------------------------------------------
  ,PIO.ID                                                     AS PAYID
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY PIO.ID ORDER BY PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,PIO.MTLN_CDC_SEQUENCE_NUMBER,PIO.MTLN_CDC_SRC_VERSION
      ,PIO.MTLN_CDC_FILENAME)),6))
                                                             AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY PIO.ID ORDER BY PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,PIO.MTLN_CDC_SEQUENCE_NUMBER,PIO.MTLN_CDC_SRC_VERSION,PIO.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY PIO.ID ORDER BY 
    PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP,PIO.MTLN_CDC_SEQUENCE_NUMBER,PIO.MTLN_CDC_SRC_VERSION
   ,PIO.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN PIO.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY PIO.ID ORDER BY  
    PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,PIO.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,PIO.MTLN_CDC_SRC_VERSION DESC
   ,PIO.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,PIO.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,PIO.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,PIO.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,PIO.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,PIO.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,PIO.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,PIO.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,PIO.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,PIO.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,PIO.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,PIO.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,PIO.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(PIO.CC_TRANSACTION_ID,-1)               AS CCTRANSATION_FACT_FK
  ,IFNULL(PIO.PAYMENT_METHOD_ID,-1)               AS PAYMENTMETHOD_DIM_FK
  ,IFNULL(PIO.PAY_IN_OUT_REASON_ID,-1)            AS PAYINPAYOUTREASON_DIM_FK
  ,IFNULL(PIO.SHIFT_ID,-1)                        AS SHIFT_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,PIO.IS_VOID                         AS IS_VOID
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(PIO.VOIDED_AT)      AS VOIDED_AT
  ,TO_TIMESTAMP_TZ(PIO.CREATED_AT)     AS CREATED_AT
  ,TO_TIMESTAMP_TZ(PIO.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,PIO.STATUS                          AS STATUS
  ,PIO.VOIDED_BY                       AS VOIDED_BY
  ,PIO.NOTES                           AS NOTES
  ,PIO.CC_TRANSACTION_ID               AS CC_TRANSACTION_ID
  ,PIO.PAY_IN_OUT_REASON_ID            AS PAY_IN_OUT_REASON_ID
  ,PIO.SHIFT_ID                        AS SHIFT_ID
--Counts and Amounts--------------------------------------------------------------------------
   ,PIO.AMOUNT/1000000                 AS AMOUNT
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_PAY_IN_OUT_TX     PIO
order by TO_TIMESTAMP_TZ(PIO.CREATED_AT)  desc
;