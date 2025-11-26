create or replace view JOBPOSITION_DIM(
	JOBPOSITION_DIM_PK,
	JOBPOSITION_DIM_NK,
	JOB_POSITION,
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
	JOBCATEGORY_DIM_FK,
	ORGANIZATION_DIM_FK,
	IS_ARCHIVED,
	IS_CASH_DRAWER_ASSIGNMENT_REQUIRED,
	IS_TRAINING,
	HAS_SKIP_AUTO_CLOCKOUT,
	CREATED_AT,
	UPDATED_AT,
	ARCHIVED_AT,
	DEFAULT_PAY_RATE
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  tab.ID                                                       AS JOBPOSITION_DIM_PK   
--natural keys------------------------------------------------------------------------------
  ,tab.ID                                                      AS JOBPOSITION_DIM_NK   
--name-------------------------------------------------------------------------------------------
  ,tab.NAME                                                    AS JOB_POSITION
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
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN tab.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY tab.ID ORDER BY  
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,tab.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,tab.MTLN_CDC_SRC_VERSION DESC
   ,tab.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED       
--CDC Meta data REQUIRED rows-----------------------------------------------------------------------------
  ,tab.MTLN_CDC_LAST_CHANGE_TYPE          AS MTLN_CDC_LAST_CHANGE_TYPE                    
  ,tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP     AS MTLN_CDC_LAST_COMMIT_TIMESTAMP               
  ,tab.MTLN_CDC_SEQUENCE_NUMBER           AS MTLN_CDC_SEQUENCE_NUMBER                     
  ,tab.MTLN_CDC_LOAD_BATCH_ID             AS MTLN_CDC_LOAD_BATCH_ID                       
  ,tab.MTLN_CDC_LOAD_TIMESTAMP            AS MTLN_CDC_LOAD_TIMESTAMP                      
  ,tab.MTLN_CDC_PROCESSED_DATE_HOUR       AS MTLN_CDC_PROCESSED_DATE_HOUR                
  ,tab.MTLN_CDC_SRC_VERSION               AS MTLN_CDC_SRC_VERSION                         
  ,tab.MTLN_CDC_FILENAME                  AS MTLN_CDC_FILENAME                            
  ,tab.MTLN_CDC_FILEPATH                  AS MTLN_CDC_FILEPATH                            
  ,tab.MTLN_CDC_SRC_DATABASE              AS MTLN_CDC_SRC_DATABASE                        
  ,tab.MTLN_CDC_SRC_SCHEMA                AS MTLN_CDC_SRC_SCHEMA                          
  ,tab.MTLN_CDC_SRC_TABLE                 AS MTLN_CDC_SRC_TABLE                           
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(tab.JOB_POSITION_CATEGORY_ID,-1)AS JOBCATEGORY_DIM_FK
  ,IFNULL(tab.ORGANIZATION_ID,-1)         AS ORGANIZATION_DIM_FK
--flags---------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
  ,tab.ARCHIVED                           AS IS_ARCHIVED
  ,tab.IS_CASH_DRAWER_ASSIGNMENT_REQUIRED AS IS_CASH_DRAWER_ASSIGNMENT_REQUIRED
  ,tab.IS_TRAINING                        AS IS_TRAINING 
  ,tab.SKIP_AUTO_CLOCKOUT                 AS HAS_SKIP_AUTO_CLOCKOUT

--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
--,to_timestamp_tz(created_at) 
  ,to_timestamp_tz(tab.CREATED_AT)       AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)       AS UPDATED_AT
  ,to_timestamp_tz(tab.ARCHIVED_AT)      AS ARCHIVED_AT
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
--ALL COLUMNS IN THIS SECTION SHOULD END IN COUNT OR AMOUNT
  ,TO_NUMBER(tab.DEFAULT_PAY_RATE,38,4)/1000000 
                                         AS DEFAULT_PAY_RATE
-- ----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_JOB_POSITION     tab
;