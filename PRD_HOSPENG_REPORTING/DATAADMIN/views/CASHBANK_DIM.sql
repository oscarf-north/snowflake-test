create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.CASHBANK_DIM(
	CASHBANK_DIM_PK,
	CASHBANK_DIM_NK,
	CASHBANK,
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
	CREATED_AT,
	UPDATED_AT,
	STATUS,
	EXPECTED_AMOUNT,
	OPEN_AMOUNT,
	CLOSE_AMOUNT
) as
--============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  tab.ID                                                      AS CASHBANK_DIM_PK  
--natural keys--------------------------------------------------------------------------------
  ,tab.ID                                                     AS CASHBANK_DIM_NK 
--name-----------------------------------------------------------------------------------------
  ,tab.ID                                                     AS CASHBANK
--data warehouse REQUIRED rows-----------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY tab.ID ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
      ,tab.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
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
--CDC Meta data REQUIRED rows---------------------------------------------------------------
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
   ,IFNULL(tab.EMPLOYEE_SHIFT_ID,-1)   AS SHIFT_DIM_FK
--flags--------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
--Dates--------------.-----------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
--,to_timestamp_tz(created_at) 
  ,to_timestamp_tz(tab.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
 ,tab.STATUS                           AS STATUS
--Counts and Amounts--------------------------------------------------------------------------
--ALL COLUMNS IN THIS SECTION SHOULD END IN COUNT OR AMOUNT
  ,TO_NUMBER(tab.EXPECTED_AMOUNT,38,4)/1000000 AS EXPECTED_AMOUNT
  ,TO_NUMBER(tab.OPEN_AMOUNT,38,0)/1000000     AS OPEN_AMOUNT
  ,TO_NUMBER(tab.CLOSE_AMOUNT,38,4)/1000000    AS CLOSE_AMOUNT
  
-- ----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CASH_BANK     tab
;
