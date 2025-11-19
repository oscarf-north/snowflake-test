create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.MODIFIEROPTIONS_DIM(
	MODIFIEROPTIONS_DIM_PK,
	MODIFIEROPTIONS_DIM_NK,
	MODIFIEROPTION,
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
	MODIFIER_DIM_FK,
	MODIFIER_COMMAND_DIM_FK,
	CREATED_AT,
	UPDATED_AT,
	PRICE
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  vao.ID                                                     AS MODIFIEROPTIONS_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,vao.ID                                                    AS MODIFIEROPTIONS_DIM_NK
--name---------------------------------------------------------------------------------------
  ,vao.ID                                                    AS MODIFIEROPTION
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY vao.ID ORDER BY vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,vao.MTLN_CDC_SEQUENCE_NUMBER,vao.MTLN_CDC_SRC_VERSION
      ,vao.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY vao.ID ORDER BY vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,vao.MTLN_CDC_SEQUENCE_NUMBER,vao.MTLN_CDC_SRC_VERSION,vao.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY vao.ID ORDER BY 
    vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP,vao.MTLN_CDC_SEQUENCE_NUMBER,vao.MTLN_CDC_SRC_VERSION
   ,vao.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN vao.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY vao.ID ORDER BY  
    vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,vao.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,vao.MTLN_CDC_SRC_VERSION DESC
   ,vao.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,vao.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,vao.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,vao.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,vao.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,vao.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,vao.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,vao.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,vao.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,vao.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,vao.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,vao.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,vao.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(vao.MODIFIER_ID,-1)          AS MODIFIER_DIM_FK
  ,IFNULL(vao.MODIFIER_CMD_ID,-1)      AS MODIFIER_COMMAND_DIM_FK
--flags---------------------------------------------------------------------------------------
  --none--
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(vao.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(vao.UPDATED_AT)     AS UPDATED_AT  
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
  ,vao.PRICE/1000000                    AS PRICE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_MODIFIER_OPTIONS     vao
;
