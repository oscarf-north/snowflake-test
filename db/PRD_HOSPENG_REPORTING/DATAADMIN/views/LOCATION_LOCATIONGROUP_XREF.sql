create or replace view LOCATION_LOCATIONGROUP_XREF(
	LOCATION_LOCATIONGROUP_XREF_PK,
	LOCATION_LOCATIONGROUP_XREF_NK,
	LOCATION_LOCATION_GROUP,
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
	LOCATIONGROUP_DIM_FK,
	LOCATION_DIM_FK,
	CREATED_AT,
	UPDATED_AT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  LGL.ID                                                     AS LOCATION_LOCATIONGROUP_XREF_PK
--natural keys------------------------------------------------------------------------------
  ,LGL.ID                                                    AS LOCATION_LOCATIONGROUP_XREF_NK
--name---------------------------------------------------------------------------------------
 ,TO_DECIMAL(TO_VARCHAR(LGL.LOCATION_ID) || '.'  || RIGHT('000000000000000' || LGL.LOCATION_GROUP_ID , 16) ,38,16) 
                                                             AS LOCATION_LOCATION_GROUP
--data warehouse rows------------------------------------------------------- -----------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY LGL.ID ORDER BY LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,LGL.MTLN_CDC_SEQUENCE_NUMBER,LGL.MTLN_CDC_SRC_VERSION
      ,LGL.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY LGL.ID ORDER BY LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,LGL.MTLN_CDC_SEQUENCE_NUMBER,LGL.MTLN_CDC_SRC_VERSION,LGL.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY LGL.ID ORDER BY 
    LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP,LGL.MTLN_CDC_SEQUENCE_NUMBER,LGL.MTLN_CDC_SRC_VERSION
   ,LGL.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN LGL.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY LGL.ID ORDER BY  
    LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,LGL.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,LGL.MTLN_CDC_SRC_VERSION DESC
   ,LGL.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,LGL.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,LGL.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,LGL.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,LGL.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,LGL.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,LGL.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,LGL.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,LGL.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,LGL.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,LGL.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,LGL.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,LGL.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(LGL.LOCATION_GROUP_ID,-1)    AS LOCATIONGROUP_DIM_FK
  ,IFNULL(LGL.LOCATION_ID,-1)          AS LOCATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--NONE
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(LGL.CREATED_AT)     AS CREATED_AT   
  ,TO_TIMESTAMP_TZ(LGL.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
--NONE
--Counts and Amounts--------------------------------------------------------------------------
--NONE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_LOCATION_GROUP_LOCATIONS     LGL
;