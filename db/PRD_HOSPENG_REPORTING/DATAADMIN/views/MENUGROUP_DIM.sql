create or replace view MENUGROUP_DIM(
	MENUGROUP_DIM_PK,
	MENUGROUP_DIM_NK,
	MENUGROUP,
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
	UPDATED_AT,
	PARENT_ID
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  meg.ID                                                      AS MENUGROUP_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,meg.ID                                                     AS MENUGROUP_DIM_NK
--name---------------------------------------------------------------------------------------
    ,meg.NAME                                                     AS MENUGROUP
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY meg.ID ORDER BY meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,meg.MTLN_CDC_SEQUENCE_NUMBER,meg.MTLN_CDC_SRC_VERSION
      ,meg.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY meg.ID ORDER BY meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,meg.MTLN_CDC_SEQUENCE_NUMBER,meg.MTLN_CDC_SRC_VERSION,meg.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY meg.ID ORDER BY 
    meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP,meg.MTLN_CDC_SEQUENCE_NUMBER,meg.MTLN_CDC_SRC_VERSION
   ,meg.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN meg.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY meg.ID ORDER BY  
    meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,meg.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,meg.MTLN_CDC_SRC_VERSION DESC
   ,meg.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED 
--CDC Meta data-------------------------------------------------------------------------------
  ,meg.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,meg.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,meg.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,meg.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,meg.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,meg.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,meg.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,meg.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,meg.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,meg.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,meg.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,meg.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(meg.organization_ID,-1)      AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(meg.created_at)     AS CREATED_AT
  ,TO_TIMESTAMP_TZ(meg.updated_at)     AS UPDATED_AT      
--names, options, etc-------------------------------------------------------------------------
  ,MEG.parent_id                       AS PARENT_ID
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_MENU_GROUP     meg
;