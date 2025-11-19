create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.EMPLOYEE_DIM(
	EMPLOYEE_DIM_PK,
	EMPLOYEE_DIM_NK,
	EMPLOYEE_NAME,
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
	IS_DISABLED,
	IS_TRAINING,
	TERMINATE_DATE,
	BIRTH_DATE,
	CREATED_AT,
	UPDATED_AT,
	FIRST_NAME,
	LAST_NAME,
	IMPORT_GUID,
	SYSTEM_ID,
	PAYROLL_ID,
	PIN,
	EMAIL,
	INITIALS,
	NICKNAME,
	UI_COLOR,
	CARD_CODE,
	AVATAR,
	GENDER,
	FLSA_STATUS
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  EMP.ID                                                                  AS EMPLOYEE_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,EMP.ID                                                                 AS EMPLOYEE_DIM_NK
--name---------------------------------------------------------------------------------------
  ,EMP.first_name || ' ' || EMP.last_name                                 AS EMPLOYEE_NAME
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY EMP.ID ORDER BY EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,EMP.MTLN_CDC_SEQUENCE_NUMBER,EMP.MTLN_CDC_SRC_VERSION
      ,EMP.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY EMP.ID ORDER BY EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,EMP.MTLN_CDC_SEQUENCE_NUMBER,EMP.MTLN_CDC_SRC_VERSION,EMP.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY EMP.ID ORDER BY 
    EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP,EMP.MTLN_CDC_SEQUENCE_NUMBER,EMP.MTLN_CDC_SRC_VERSION
   ,EMP.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN EMP.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY EMP.ID ORDER BY  
    EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,EMP.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,EMP.MTLN_CDC_SRC_VERSION DESC
   ,EMP.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW    
--CDC Meta data-------------------------------------------------------------------------------
  ,EMP.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,EMP.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,EMP.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,EMP.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,EMP.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,EMP.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,EMP.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,EMP.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,EMP.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,EMP.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                     
  ,EMP.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,EMP.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                          
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(EMP.organization_id,-1)      AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,EMP.is_disabled                     AS IS_DISABLED
  ,EMP.is_training                     AS IS_TRAINING
--Dates--------------.------------------------------------------------------------------------
  ,to_date(EMP.terminated_at)          AS TERMINATE_DATE
  ,to_date(EMP.date_of_birth)          AS BIRTH_DATE  
  ,to_timestamp_tz(EMP.CREATED_AT)     AS CREATED_AT
  ,to_timestamp_tz(UPDATED_AT)         AS UPDATED_AT
-- --names, options, etc-----------------------------------------------------------------------
   ,EMP.first_name                     AS FIRST_NAME
   ,EMP.last_name                      AS LAST_NAME
   ,EMP.import_id                      AS IMPORT_GUID
   ,EMP.SYSTEM_USER_UUID               AS SYSTEM_ID
   ,EMP.payroll_id                     AS PAYROLL_ID
   ,EMP.pin                            AS PIN
   ,EMP.email                          AS EMAIL
   ,EMP.initials                       AS INITIALS
   ,EMP.nickname                       AS NICKNAME
   ,EMP.ui_color                       AS UI_COLOR
   ,EMP.card_code                      AS CARD_CODE
   ,EMP.avatar                         AS AVATAR
   ,EMP.gender                         AS GENDER
   ,EMP.flsa_status                    AS FLSA_STATUS
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_EMPLOYEE                          EMP 
;
