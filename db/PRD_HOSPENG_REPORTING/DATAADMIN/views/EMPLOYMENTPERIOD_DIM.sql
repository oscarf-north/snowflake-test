create or replace view EMPLOYMENTPERIOD_DIM(
	EMPLOYMENTPERIOD_DIM_PK,
	EMPLOYMENTPERIOD_DIM_NK,
	EMPLOYMENTPERIOD_ID,
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
	EMPLOYEE_DIM_FK,
	JOBPOSITION_DIM_FK,
	LOCATION_DIM_FK,
	LOCATION_GROUP_DIM_FK,
	CREATED_AT,
	UPDATED_AT,
	STARTED_AT,
	ENDED_AT,
	PAY_RATE_AMOUNT,
	PAY_RATE_PER_SECOND
) as
--============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  tab.ID                                                      AS EMPLOYMENTPERIOD_DIM_PK   
--natural keys--------------------------------------------------------------------------------
  ,tab.ID                                                     AS EMPLOYMENTPERIOD_DIM_NK   
--name-----------------------------------------------------------------------------------------
  ,tab.ID                                                     AS EMPLOYMENTPERIOD_ID
--data warehouse REQUIRED rows-----------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY tab.ID ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
      ,tab.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE      
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY tab.ID ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION,tab.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY tab.ID ORDER BY 
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
   ,tab.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE         
  
  ,CASE WHEN tab.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED            
  ,CASE WHEN RANK() OVER(PARTITION BY tab.ID ORDER BY  
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,tab.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,tab.MTLN_CDC_SRC_VERSION DESC
   ,tab.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     
--CDC Meta data REQUIRED rows----------------------------------------------------------------------------
  ,tab.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE                   
  ,tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP             
  ,tab.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER                    
  ,tab.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID                     
  ,tab.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP                    
  ,tab.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR              
  ,tab.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION                        
  ,tab.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME                          
  ,tab.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH                    
  ,tab.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                     
  ,tab.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA                         
  ,tab.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                          
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(tab.EMPLOYEE_ID,-1)                     AS EMPLOYEE_DIM_FK
  ,IFNULL(tab.JOB_POSITION_ID,-1)                 AS JOBPOSITION_DIM_FK
  ,IFNULL(tab.LOCATION_ID,-1)                     AS LOCATION_DIM_FK 
  ,IFNULL(tab.LOCATION_GROUP_ID,-1)               AS LOCATION_GROUP_DIM_FK
--flags---------------------------------------------------------------------------------------
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(tab.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)     AS UPDATED_AT
  ,to_timestamp_tz(tab.START_DATE)     AS STARTED_AT
  ,to_timestamp_tz(tab.END_DATE)       AS ENDED_AT  
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
  ,TO_NUMBER(tab.PAY_RATE,38,0) / 1000000           AS PAY_RATE_AMOUNT
  ,(TO_NUMBER(tab.PAY_RATE,38,0) / 1000000) / (60)  AS PAY_RATE_PER_SECOND  
-- -------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_EMPLOYMENT_PERIOD     tab
;