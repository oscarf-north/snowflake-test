create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.GIFTCARD_DIM(
	GIFTCARD_DIM_PK,
	GIFTCARD_DIM_NK,
	GIFTCARD,
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
	MERCHANT_DIM_FK,
	IS_LEGACY,
	IS_ISSUED,
	CREATED_AT,
	EXPIRES_AT,
	UPDATED_AT,
	ISSUED_AT,
	BALANCE_REQUESTED_AT,
	BATCH_ID,
	BALANCE_REQUEST_COUNT,
	START_BALANCE,
	BALANCE
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  dad.ID                                                       AS GIFTCARD_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,dad.ID                                                      AS GIFTCARD_DIM_NK
--name---------------------------------------------------------------------------------------
  ,dad.ACCOUNT                                                 AS GIFTCARD
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY dad.ID ORDER BY dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,dad.MTLN_CDC_SEQUENCE_NUMBER,dad.MTLN_CDC_SRC_VERSION
      ,dad.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY dad.ID ORDER BY dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,dad.MTLN_CDC_SEQUENCE_NUMBER,dad.MTLN_CDC_SRC_VERSION,dad.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY dad.ID ORDER BY 
    dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP,dad.MTLN_CDC_SEQUENCE_NUMBER,dad.MTLN_CDC_SRC_VERSION
   ,dad.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN dad.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY dad.ID ORDER BY  
    dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,dad.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,dad.MTLN_CDC_SRC_VERSION DESC
   ,dad.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,dad.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,dad.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,dad.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,dad.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,dad.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,dad.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,dad.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,dad.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,dad.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,dad.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,dad.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(dad.MERCHANT_ID,'-1')       AS MERCHANT_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,dad.IS_LEGACY::boolean              AS IS_LEGACY
  ,dad.IS_ISSUED::boolean              AS IS_ISSUED
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(dad.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(dad.EXPIRES_AT)     AS EXPIRES_AT
  ,to_timestamp_tz(dad.UPDATED_AT)     AS UPDATED_AT
  ,to_timestamp_tz(dad.ISSUED_AT)      AS ISSUED_AT
  ,to_timestamp_tz(dad.BALANCE_REQUESTED_AT)  as BALANCE_REQUESTED_AT
--names, options, etc-------------------------------------------------------------------------
  ,dad.BATCH_ID                        AS BATCH_ID
--Counts and Amounts--------------------------------------------------------------------------
  ,dad.BALANCE_REQUEST_COUNT           AS BALANCE_REQUEST_COUNT    ----
  ,dad.start_balance                   AS START_BALANCE
  ,dad.BALANCE                         AS BALANCE

----------------------------------------------------------------------------------------------
FROM DATALANDING.GIFTCARD_PUBLIC_GIFTCARDS     DAD
;
