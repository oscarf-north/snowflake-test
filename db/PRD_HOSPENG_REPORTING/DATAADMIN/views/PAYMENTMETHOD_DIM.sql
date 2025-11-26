create or replace view PAYMENTMETHOD_DIM(
	PAYMENTMETHOD_DIM_PK,
	PAYMENTMETHOD_DIM_NK,
	PAYMENTMETHODNAME,
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
	ORGANIZATION_DIM_FK,
	ARCHIVED,
	IS_ENABLED,
	IS_TIPPABLE,
	ARCHIVED_AT,
	CREATED_AT,
	UPDATED_AT,
	PAYMENTMETHODTYPE
) as
--select * from DATAADMIN.PAYMENTMETHOD_DIM:
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  pmm.ID                                                      AS PAYMENTMETHOD_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,pmm.ID                                                     AS PAYMENTMETHOD_DIM_NK
--name---------------------------------------------------------------------------------------
  ,pmm.NAME                                                   AS PAYMENTMETHODNAME
--data warehouse rows------------------------------------------------------------------------
,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY pmm.ID ORDER BY pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,pmm.MTLN_CDC_SEQUENCE_NUMBER,pmm.MTLN_CDC_SRC_VERSION
      ,pmm.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY pmm.ID ORDER BY pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,pmm.MTLN_CDC_SEQUENCE_NUMBER,pmm.MTLN_CDC_SRC_VERSION,pmm.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY pmm.ID ORDER BY 
    pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP,pmm.MTLN_CDC_SEQUENCE_NUMBER,pmm.MTLN_CDC_SRC_VERSION
   ,pmm.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN pmm.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY pmm.ID ORDER BY  
    pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,pmm.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,pmm.MTLN_CDC_SRC_VERSION DESC
   ,pmm.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,pmm.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,pmm.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,pmm.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,pmm.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,pmm.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,pmm.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,pmm.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,pmm.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,pmm.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,pmm.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,pmm.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,pmm.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
   ,IFNULL(pmm.ORGANIZATION_ID,-1)     AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,pmm.archived                        AS ARCHIVED
  ,pmm.is_enabled                      AS IS_ENABLED
  ,pmm.is_tippable                     AS IS_TIPPABLE
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(pmm.ARCHIVED_AT)    AS ARCHIVED_AT
  ,TO_TIMESTAMP_TZ(pmm.CREATED_AT)     AS CREATED_AT   
  ,TO_TIMESTAMP_TZ(pmm.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
   ,pmm.Type                           AS PAYMENTMETHODTYPE
--Counts and Amounts--------------------------------------------------------------------------
  --none
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_PAYMENT_METHOD     pmm
;