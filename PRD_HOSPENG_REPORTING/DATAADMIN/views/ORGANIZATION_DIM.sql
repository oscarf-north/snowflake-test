create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.ORGANIZATION_DIM(
	ORGANIZATION_DIM_PK,
	ORGANIZATION_DIM_NK,
	ORGANIZATION,
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
	DO_AUTO_END_SHIFTS,
	FISCAL_DAY_START,
	START_OF_PAYROLL_WEEK_INT,
	START_OF_BIZ_WEEK_INT,
	CREATED_AT,
	UPDATED_AT,
	STATUS,
	VERSION,
	SERVICE_LEVEL,
	DEFAULT_CURRENCY,
	SUPPORTED_CURRENCIES
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  org.ID                                                     AS ORGANIZATION_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,org.ID                                                    AS ORGANIZATION_DIM_NK
--name---------------------------------------------------------------------------------------
  ,org.NAME                                                  AS ORGANIZATION
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(org.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY org.ID ORDER BY org.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,org.MTLN_CDC_SEQUENCE_NUMBER,org.MTLN_CDC_SRC_VERSION
      ,org.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE        
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(org.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY org.ID ORDER BY org.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,org.MTLN_CDC_SEQUENCE_NUMBER,org.MTLN_CDC_SRC_VERSION,org.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY org.ID ORDER BY 
    org.MTLN_CDC_LAST_COMMIT_TIMESTAMP,org.MTLN_CDC_SEQUENCE_NUMBER,org.MTLN_CDC_SRC_VERSION
   ,org.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          
  
  ,CASE WHEN org.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED             
  ,CASE WHEN RANK() OVER(PARTITION BY org.ID ORDER BY  
    org.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,org.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,org.MTLN_CDC_SRC_VERSION DESC
   ,org.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW      
--CDC Meta data-------------------------------------------------------------------------------
  ,org.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,org.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,org.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,org.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,org.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,org.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,org.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,org.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,org.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,org.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,org.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,org.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  --none
--flags---------------------------------------------------------------------------------------
  ,org.DO_AUTO_END_SHIFTS              AS DO_AUTO_END_SHIFTS
--Dates--------------.------------------------------------------------------------------------
-- ,TIMESTAMPADD('Seconds',LOC.FISCAL_DAY_START,'')
  ,TO_NUMBER(org.fiscal_day_start)/1000 ::NUMBER(38,0) 
                                       AS FISCAL_DAY_START 
  ,TO_NUMBER(org.start_of_payroll_week)AS START_OF_PAYROLL_WEEK_INT
  ,TO_NUMBER(org.start_of_biz_week)    AS START_OF_BIZ_WEEK_INT
  ,TO_TIMESTAMP_TZ(org.created_at)     AS CREATED_AT
  ,TO_TIMESTAMP_TZ(org.updated_at)     AS UPDATED_AT  
--names, options, etc-------------------------------------------------------------------------
  ,org.status                          AS STATUS
  ,org.version                         AS VERSION
  ,org.service_level                   AS SERVICE_LEVEL
  ,org.default_currency                AS DEFAULT_CURRENCY
  ,replace(replace(replace(
        supported_currencies,'[',' '),']',''),'"','') 
                                       AS SUPPORTED_CURRENCIES    
--Counts and Amounts--------------------------------------------------------------------------
--none
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_ORGANIZATION    org
;
