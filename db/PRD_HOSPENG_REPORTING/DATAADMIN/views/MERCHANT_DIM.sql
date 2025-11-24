create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.MERCHANT_DIM(
	MERCHANT_DIM_PK,
	MERCHANT_DIM_NK,
	MERCHANT,
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
	IS_GIFTCARDDISABED,
	CREATED_AT,
	UPDATED_AT,
	GIFTCARD_CREDIT_CEILING,
	GIFTCARD_DEBIT_CEILING,
	GIFTCARD_CREDIT_FLOOR
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  dad.ID                                                       AS MERCHANT_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,dad.ID                                                      AS MERCHANT_DIM_NK
--name---------------------------------------------------------------------------------------
  ,dad.NAME                                                    AS MERCHANT
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
  -- ,IFNULL(dad.MERCHANT_ID,'-1')       AS MERCHANT_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,dad.GIFTCARD_DISABLED::boolean      AS IS_GIFTCARDDISABED
  -- ,dad.IS_ISSUED::boolean              AS IS_ISSUED
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(dad.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(dad.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  -- ,dad.BATCH_ID                        AS BATCH_ID
--Counts and Amounts--------------------------------------------------------------------------
 --none
  ,dad.GIFTCARD_CREDIT_CEILING        AS GIFTCARD_CREDIT_CEILING
  ,dad.GIFTCARD_DEBIT_CEILING         AS GIFTCARD_DEBIT_CEILING
  ,dad.GIFTCARD_CREDIT_FLOOR          AS GIFTCARD_CREDIT_FLOOR
----------------------------------------------------------------------------------------------
FROM DATALANDING.GIFTCARD_PUBLIC_MERCHANTS    DAD
;
