create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.JOBCATEGORY_DIM(
	JOBCATEGORY_DIM_PK,
	JOBCATEGORY_DIM_NK,
	JOBCATEGORY,
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
	CREATED_AT,
	UPDATED_AT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  TBL.ID                                                     AS JOBCATEGORY_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,TBL.ID                                                    AS JOBCATEGORY_DIM_NK
--name---------------------------------------------------------------------------------------
  ,TBL.NAME                                                  AS JOBCATEGORY
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY TBL.ID ORDER BY TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,TBL.MTLN_CDC_SEQUENCE_NUMBER,TBL.MTLN_CDC_SRC_VERSION
      ,TBL.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY TBL.ID ORDER BY TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,TBL.MTLN_CDC_SEQUENCE_NUMBER,TBL.MTLN_CDC_SRC_VERSION,TBL.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY TBL.ID ORDER BY 
    TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP,TBL.MTLN_CDC_SEQUENCE_NUMBER,TBL.MTLN_CDC_SRC_VERSION
   ,TBL.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN TBL.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY TBL.ID ORDER BY  
    TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,TBL.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,TBL.MTLN_CDC_SRC_VERSION DESC
   ,TBL.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,TBL.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,TBL.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,TBL.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,TBL.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,TBL.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,TBL.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,TBL.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,TBL.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,TBL.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,TBL.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,TBL.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,TBL.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys--------------------------------------------------------------------------------
  ,TBL.ORGANIZATION_ID                 AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--none
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(TBL.CREATED_AT)     AS CREATED_AT   
  ,to_timestamp_tz(TBL.UPDATED_AT)     AS UPDATED_AT  
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
--none 
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_JOB_POSITION_CATEGORY     TBL
;
