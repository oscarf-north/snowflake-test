create or replace view DRAWER_DIM(
	DRAWER_DIM_PK,
	DRAWER_DIM_NK,
	DRAWER,
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
	TERMINAL_DIM_FK,
	CREATED_AT,
	UPDATED_AT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  TO_NUMBER(dwr.DRAWER_ID,38,0)                              AS DRAWER_DIM_PK  
--natural keys------------------------------------------------------------------------------
  ,TO_NUMBER(dwr.DRAWER_ID,38,0)                             AS DRAWER_DIM_NK  
--name-------------------------------------------------------------------------------------------
  ,TO_NUMBER(dwr.DRAWER_ID,38,0)                             AS DRAWER
--data warehouse REQUIRED rows-------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY TO_NUMBER(dwr.DRAWER_ID,38,0) ORDER BY dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,dwr.MTLN_CDC_SEQUENCE_NUMBER,dwr.MTLN_CDC_SRC_VERSION
      ,dwr.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY TO_NUMBER(dwr.DRAWER_ID,38,0) ORDER BY dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,dwr.MTLN_CDC_SEQUENCE_NUMBER,dwr.MTLN_CDC_SRC_VERSION,dwr.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY TO_NUMBER(dwr.DRAWER_ID,38,0) ORDER BY 
    dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP,dwr.MTLN_CDC_SEQUENCE_NUMBER,dwr.MTLN_CDC_SRC_VERSION
   ,dwr.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN dwr.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY TO_NUMBER(dwr.DRAWER_ID,38,0)ORDER BY  
    dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,dwr.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,dwr.MTLN_CDC_SRC_VERSION DESC
   ,dwr.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED 
--CDC Meta data REQUIRED rows-------------------------------------------------------------------------------
  ,dwr.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE                   
  ,dwr.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP              
  ,dwr.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER                    
  ,dwr.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID                      
  ,dwr.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP                     
  ,dwr.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR                
  ,dwr.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION                        
  ,dwr.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME                           
  ,dwr.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH                           
  ,dwr.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                       
  ,dwr.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA                         
  ,dwr.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                          
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(dwr.employee_id,-1)                     AS EMPLOYEE_DIM_FK
  ,IFNULL(dwr.LOCATION_id,-1)                     AS LOCATION_DIM_FK 
  ,IFNULL(dwr.TERMINAL_ID,-1)                     AS TERMINAL_DIM_FK
--flags---------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create dwrle from postgres
--,to_timestamp_tz(created_at) 
  ,to_timestamp_tz(dwr.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(dwr.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
--Counts and Amounts--------------------------------------------------------------------------
--ALL COLUMNS IN THIS SECTION SHOULD END IN COUNT OR AMOUNT
-- ----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_DRAWER   dwr
;