create or replace view REPORTCATEGORY_DIM(
	REPORTCATEGORY_DIM_PK,
	REPORTCATEGORY_DIM_NK,
	REPORTCATEGORY,
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
	COGSCATEGORY_DIM_FK,
	ORGANIZATION_DIM_FK,
	CREATED_AT,
	UPDATED_AT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  rec.ID                                                     AS REPORTCATEGORY_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,rec.ID                                                    AS REPORTCATEGORY_DIM_NK
--name---------------------------------------------------------------------------------------
  ,rec.name                                                  AS REPORTCATEGORY
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY rec.ID ORDER BY rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,rec.MTLN_CDC_SEQUENCE_NUMBER,rec.MTLN_CDC_SRC_VERSION
      ,rec.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY rec.ID ORDER BY rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,rec.MTLN_CDC_SEQUENCE_NUMBER,rec.MTLN_CDC_SRC_VERSION,rec.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY rec.ID ORDER BY 
    rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP,rec.MTLN_CDC_SEQUENCE_NUMBER,rec.MTLN_CDC_SRC_VERSION
   ,rec.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN rec.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY rec.ID ORDER BY  
    rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,rec.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,rec.MTLN_CDC_SRC_VERSION DESC
   ,rec.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,rec.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,rec.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,rec.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,rec.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,rec.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,rec.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,rec.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,rec.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,rec.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,rec.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,rec.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,rec.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------  
 ,IFNULL(COGS_CATEGORY_ID,-1)       AS COGSCATEGORY_DIM_FK
  ,IFNULL(rec.ORGANIZATION_ID,-1)      AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--none
--Dates--------------------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(rec.CREATED_AT)     AS CREATED_AT  
  ,TO_TIMESTAMP_TZ(rec.UPDATED_AT)     AS UPDATED_AT
--names, options, etc---------------------------------------------------------
 --none
--Counts and Amounts--------------------------------------------------------------------------
 --none
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_REPORTING_CATEGORY    rec
;