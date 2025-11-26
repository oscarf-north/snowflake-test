create or replace view DAYPARTSCHEDULE_DIM(
	DAYPARTSCHEDULE_DIM_PK,
	DAYPARTSCHEDULE_DIM_NK,
	DAYPARTSCHEDULE,
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
	DAYPART_DIM_FK,
	LOCATION_DIM_FK,
	CREATED_AT,
	UPDATED_AT,
	TXID,
	DAY_OF_WEEK,
	START_TIME,
	SYS_PERIOD
) as
--============================================================================================
--SELECT * FROM DATAADMIN.DAYPARTSCHEDULE_DIM;
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  dad.ID                                                       AS DAYPARTSCHEDULE_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,dad.ID                                                      AS DAYPARTSCHEDULE_DIM_NK
--name---------------------------------------------------------------------------------------
  ,dad.ID                                                      AS DAYPARTSCHEDULE
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
  -- ,IFNULL(dad.ORGANIZATION_ID,-1)      AS ORGANIZATION_DIM_FK
  ,dad.DAYPART_ID                      AS DAYPART_DIM_FK
  ,dad.LOCATION_ID                     AS LOCATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--none
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(dad.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(dad.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
 --none
--Counts and Amounts--------------------------------------------------------------------------
 ,dad.TXID                            AS TXID
 ,dad.DAY_OF_WEEK                     AS DAY_OF_WEEK
 ,dad.START_TIME                      AS START_TIME
 ,dad.SYS_PERIOD                      AS SYS_PERIOD
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_DAYPART_SCHEDULE     dad
;