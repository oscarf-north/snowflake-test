create or replace view SHIFT_DIM(
	SHIFT_DIM_PK,
	SHIFT_DIM_NK,
	SHIFT,
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
	LOCATION_DIM_FK,
	JOBPOSITION_DIM_FK,
	WAS_SYSTEM_CLOCKOUT,
	IS_ARCHIVED,
	IS_SHIFTCOMPLETE,
	PAY_RATE_BASIS,
	GETS_PAID_BREAK,
	CREATED_AT,
	UPDATED_AT,
	ORIGINAL_CLOCKIN_AT,
	FISCAL_DAY,
	CLOCKEDIN_AT,
	CLOCKEDOUT_AT,
	ARCHIVED_AT,
	SHIFT_START_AT,
	SHIFT_END_AT,
	GENERAL_LEDGER,
	REGULAR_RATE,
	SHIFT_SECONDS,
	SHIFT_MINUTES,
	SHIFT_HOURS,
	SHIFT_DAYS
) as
--============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  tab.ID                                                      AS SHIFT_DIM_PK  
--natural keys--------------------------------------------------------------------------------
  ,tab.ID                                                     AS SHIFT_DIM_NK  
--name-----------------------------------------------------------------------------------------
  ,tab.ID                                                     AS SHIFT
--data warehouse REQUIRED rows----------------------------------------------------------------
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
--CDC Meta data REQUIRED rows----------------------------------------------------------------
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
  ,IFNULL(tab.EMPLOYEE_ID,-1)          AS EMPLOYEE_DIM_FK
  ,IFNULL(tab.LOCATION_ID,-1)          AS LOCATION_DIM_FK 
  ,IFNULL(tab.JOB_POSITION_ID,-1)      AS JOBPOSITION_DIM_FK
--flags---------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
  ,tab.WAS_SYSTEM_CLOCKOUT             AS WAS_SYSTEM_CLOCKOUT
  ,tab.ARCHIVED                        AS IS_ARCHIVED
  ,tab.CLOCK_OUT IS NOT NULL           AS IS_SHIFTCOMPLETE
  ,TRIM(tab.PAY_RATE_BASIS)::VARCHAR(50)   
                                       AS PAY_RATE_BASIS
  ,TRUE                                AS GETS_PAID_BREAK          --GET NEW FROM DATA STRUCTURE
--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
--,to_timestamp_tz(created_at) 
  ,to_timestamp_tz(tab.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)     AS UPDATED_AT
  ,to_timestamp_tz(tab.ORIG_CLOCK_IN)  AS ORIGINAL_CLOCKIN_AT
  ,to_date(to_timestamp(fiscal_date * 86400))
                                       AS FISCAL_DAY              
  ,to_timestamp_tz(tab.CLOCK_IN)       AS CLOCKEDIN_AT
  ,to_timestamp_tz(tab.CLOCK_OUT)      AS CLOCKEDOUT_AT  
  ,to_timestamp_tz(tab.ARCHIVED_AT)    AS ARCHIVED_AT   
  ,to_timestamp_tz(REPLACE(REPLACE(SPLIT_PART(tab.TIME_RANGE,',',1),'"',''),'[',''))    
                                       AS SHIFT_START_AT
  ,TRY_TO_TIMESTAMP_TZ(REPLACE(REPLACE(SPLIT_PART(tab.TIME_RANGE,',',2),'"',''),')','')) 
    
                                       AS SHIFT_END_AT 

                                   
--names, options, etc-------------------------------------------------------------------------
,tab.GENERAL_LEDGER                   AS GENERAL_LEDGER
--Counts and Amounts--------------------------------------------------------------------------
,tab.pay_rate/1000000                 AS REGULAR_RATE         
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.CLOCK_IN),to_timestamp_tz(tab.CLOCK_OUT))             ::Number(38,0),0)
                                      AS SHIFT_SECONDS
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.CLOCK_IN),to_timestamp_tz(tab.CLOCK_OUT)) /60         ::Number(38,0),0)
                                      AS SHIFT_MINUTES
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.CLOCK_IN),to_timestamp_tz(tab.CLOCK_OUT))  /60/60     ::Number(38,0),0)
                                      AS SHIFT_HOURS
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.CLOCK_IN),to_timestamp_tz(tab.CLOCK_OUT))  /60/60/60  ::Number(38,0),0)
                                      AS SHIFT_DAYS                                      
-- --------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_EMPLOYEE_SHIFT     tab
  WHERE NOT COALESCE(tab.TRUNCATED,FALSE)
    AND tab.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;