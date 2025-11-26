create or replace view SHIFTBREAK_FACT(
	SHIFTBREAK_FACT_PK,
	SHIFTBREAK_FACT_NK,
	SHIFTBREAK,
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
	SHIFT_DIM_FK,
	IS_BREAKCOMPLETE,
	IS_ARCHIVED,
	CREATED_AT,
	UPDATED_AT,
	START_AT,
	ORIG_BEGIN_AT,
	END_AT_INT,
	END_AT,
	ORIG_END_AT,
	TIME_RANGE,
	BREAK_SECONDS,
	BREAK_MINUTES,
	BREAK_HOURS,
	BREAK_DAYS
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  tab.ID                                                      AS SHIFTBREAK_FACT_PK  
--natural keys------------------------------------------------------------------------------
  ,tab.ID                                                     AS SHIFTBREAK_FACT_NK  
--name-------------------------------------------------------------------------------------------
  ,tab.TIME_RANGE                                             AS SHIFTBREAK
--data warehouse REQUIRED rows-------------------------------------------------------------------
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
--CDC Meta data REQUIRED rows-------------------------------------------------------------------------------
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
  ,tab.SHIFT_id                        AS SHIFT_DIM_FK  
--flags---------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
--Alphabetize between the lines
 ,tab.END_AT IS NOT NULL                   AS IS_BREAKCOMPLETE             
 ,tab.ARCHIVED                             AS IS_ARCHIVED
--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
--,to_timestamp_tz(created_at) 
--Alphebetize between the lines
  ,try_to_timestamp_tz(tab.CREATED_AT)      AS CREATED_AT  
  ,try_to_timestamp_tz(tab.UPDATED_AT)      AS UPDATED_AT
  ,try_to_timestamp_tz(tab.BEGIN_AT)        AS START_AT
  ,try_to_timestamp_tz(tab.ORIG_BEGIN_AT)   AS ORIG_BEGIN_AT
  ,tab.END_AT as END_AT_int
  ,try_to_timestamp_tz(tab.END_AT)          AS END_AT
  ,try_to_timestamp_tz(tab.ORIG_END_AT)     AS ORIG_END_AT
  ,tab.TIME_RANGE                           AS TIME_RANGE
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
--ALL COLUMNS IN THIS SECTION SHOULD END IN COUNT OR AMOUNT
--Alphebetize between the lines
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.BEGIN_AT),to_timestamp_tz(tab.END_AT))             ::Number(38,0),0)
                                           AS BREAK_SECONDS
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.BEGIN_AT),to_timestamp_tz(tab.END_AT)) /60         ::Number(38,0),0)
                                           AS BREAK_MINUTES
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.BEGIN_AT),to_timestamp_tz(tab.END_AT))  /60/60     ::Number(38,0),0)
                                           AS BREAK_HOURS
,ROUND(TIMEDIFF(second,to_timestamp_tz(tab.BEGIN_AT),to_timestamp_tz(tab.END_AT))  /60/60/60  ::Number(38,0),0)
                                           AS BREAK_DAYS                                         
-- ----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_EMPLOYEE_SHIFT_BREAK tab 
  WHERE NOT COALESCE(tab.TRUNCATED,FALSE)
      AND tab.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;
--=====================DAT======================================================================
--End of View