create or replace view LOCATION_DIM(
	LOCATION_DIM_PK,
	LOCATION_DIM_NK,
	LOCATIONNAME,
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
	ADDRESS_DIM_FK,
	ORGANIZATION_DIM_FK,
	CREATED_AT,
	UPDATED_AT,
	FISCAL_DAY_START,
	TZ_NAME
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  LOC.ID                                                     AS LOCATION_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,LOC.ID                                                    AS LOCATION_DIM_NK
--name---------------------------------------------------------------------------------------
  ,LOC.name                                                  AS LOCATIONNAME
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY LOC.ID ORDER BY LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,LOC.MTLN_CDC_SEQUENCE_NUMBER,LOC.MTLN_CDC_SRC_VERSION
      ,LOC.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY LOC.ID ORDER BY LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,LOC.MTLN_CDC_SEQUENCE_NUMBER,LOC.MTLN_CDC_SRC_VERSION,LOC.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY LOC.ID ORDER BY 
    LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP,LOC.MTLN_CDC_SEQUENCE_NUMBER,LOC.MTLN_CDC_SRC_VERSION
   ,LOC.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN LOC.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY LOC.ID ORDER BY  
    LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,LOC.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,LOC.MTLN_CDC_SRC_VERSION DESC
   ,LOC.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED 
--CDC Meta data-------------------------------------------------------------------------------
  ,LOC.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,LOC.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,LOC.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,LOC.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,LOC.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,LOC.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,LOC.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,LOC.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,LOC.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,LOC.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,LOC.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,LOC.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(LOC.address_id,-1)           AS ADDRESS_DIM_FK
  ,IFNULL(LOC.organization_id,-1)      AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--Dates--------------.------------------------------------------------------------------------
   --,LOC.auto_eod_time                AS AUTO_EOD_TIME
   ,TO_TIMESTAMP_TZ(LOC.created_at)    AS CREATED_AT
   ,TO_TIMESTAMP_TZ(LOC.updated_at)    AS UPDATED_AT
--Names--------------------------------------------------------------------------------------
  ,LOC.FISCAL_DAY_START /1000 ::NUMBER          AS FISCAL_DAY_START
  -- ,CASE LOC.FISCAL_DAY_START 
  --   WHEN 0 THEN 'Sunday'
  --   WHEN 1 THEN 'Monday'
  --   WHEN 2 THEN 'Tuesday'
  --   WHEN 3 THEN 'Wednesday'
  --   WHEN 4 THEN 'Thursday'
  --   WHEN 5 THEN 'Fridayday'
  --   WHEN 6 THEN 'Saturday'
  --   END                             AS FISCAL_DAY_START
  ,LOC.TZ_NAME                         AS TZ_NAME
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_LOCATION     LOC
;