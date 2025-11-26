create or replace view LOCATIONGROUP_DIM(
	LOCATIONGROUP_DIM_PK,
	LOCATIONGROUP_DIM_NK,
	LOCATIONGROUP,
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
	IS_DEFAULT,
	CREATED_AT,
	UPDATED_AT,
	TYPE
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  LCG.ID                                                     AS LOCATIONGROUP_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,LCG.ID                                                    AS LOCATIONGROUP_DIM_NK
--name---------------------------------------------------------------------------------------
  ,LCG.NAME                                                  AS LOCATIONGROUP
--data warehouse rows------------------------------------------------------------------------
   ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY LCG.ID ORDER BY LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,LCG.MTLN_CDC_SEQUENCE_NUMBER,LCG.MTLN_CDC_SRC_VERSION
      ,LCG.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY LCG.ID ORDER BY LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,LCG.MTLN_CDC_SEQUENCE_NUMBER,LCG.MTLN_CDC_SRC_VERSION,LCG.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY LCG.ID ORDER BY 
    LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP,LCG.MTLN_CDC_SEQUENCE_NUMBER,LCG.MTLN_CDC_SRC_VERSION
   ,LCG.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN LCG.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY LCG.ID ORDER BY  
    LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,LCG.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,LCG.MTLN_CDC_SRC_VERSION DESC
   ,LCG.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,LCG.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,LCG.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,LCG.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,LCG.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,LCG.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,LCG.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,LCG.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,LCG.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,LCG.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,LCG.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,LCG.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,LCG.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(LCG.ORGANIZATION_ID,-1)      AS ORGANIZATION_DIM_FK   
--flags---------------------------------------------------------------------------------------
  ,LCG.IS_DEFAULT                      AS IS_DEFAULT
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP(LCG.CREATED_AT)        AS CREATED_AT   
  ,TO_TIMESTAMP(LCG.UPDATED_AT)        AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,LCG.TYPE                            AS TYPE
--Counts and Amounts--------------------------------------------------------------------------
--NONE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_LOCATION_GROUP     LCG
;