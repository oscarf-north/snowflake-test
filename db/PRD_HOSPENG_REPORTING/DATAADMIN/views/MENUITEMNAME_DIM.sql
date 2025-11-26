create or replace view MENUITEMNAME_DIM(
	MENUITEMNAME_DIM_PK,
	MENUITEMNAME_DIM_NK,
	MENUITEMNAME,
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
	REPORTCATEGORY_DIM_FK,
	ARCHIVED,
	ARCHIVED_AT,
	CREATED_AT,
	UPDATED_AT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  min.ID                                                      AS MENUITEMNAME_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,min.ID                                                     AS MENUITEMNAME_DIM_NK
--name---------------------------------------------------------------------------------------
   ,min.name                                                  AS MENUITEMNAME
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(min.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY min.ID ORDER BY min.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,min.MTLN_CDC_SEQUENCE_NUMBER,min.MTLN_CDC_SRC_VERSION
      ,min.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(min.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY min.ID ORDER BY min.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,min.MTLN_CDC_SEQUENCE_NUMBER,min.MTLN_CDC_SRC_VERSION,min.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY min.ID ORDER BY 
    min.MTLN_CDC_LAST_COMMIT_TIMESTAMP,min.MTLN_CDC_SEQUENCE_NUMBER,min.MTLN_CDC_SRC_VERSION
   ,min.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN min.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY min.ID ORDER BY  
    min.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,min.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,min.MTLN_CDC_SRC_VERSION DESC
   ,min.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,min.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,min.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,min.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,min.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,min.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,min.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,min.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,min.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,min.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,min.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,min.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,min.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(min.ORGANIZATION_ID,-1)      AS ORGANIZATION_DIM_FK
  ,IFNULL(min.reporting_category_id,-1)AS REPORTCATEGORY_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,min.ARCHIVED                        AS ARCHIVED
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(min.ARCHIVED_AT)   AS ARCHIVED_AT
  ,TO_TIMESTAMP_TZ(min.CREATED_AT)    AS CREATED_AT   
  ,TO_TIMESTAMP_TZ(min.UPDATED_AT)    AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
--none
--Counts and Amounts--------------------------------------------------------------------------
--none
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_MENU_ITEM_NAME     min
;