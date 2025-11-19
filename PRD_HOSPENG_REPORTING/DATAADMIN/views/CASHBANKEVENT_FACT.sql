create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.CASHBANKEVENT_FACT(
	CASHBANKEVENT_FACT_PK,
	CASHBANKEVENT_FACT_NK,
	EVENT_TYPE,
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
	CASHBANK_DIM_FK,
	CREATED_AT,
	NOTES,
	AMOUNT
) as
--==========================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  cbf.ID                                                      AS CASHBANKEVENT_FACT_PK  
--natural keys------------------------------------------------------------------------------
  ,cbf.ID                                                     AS CASHBANKEVENT_FACT_NK   
--name---------------------------------------------------------------------------------------
  ,EVENT_TYPE                                                 AS EVENT_TYPE 
--data warehouse REQUIRED rows---------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY cbf.ID ORDER BY cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,cbf.MTLN_CDC_SEQUENCE_NUMBER,cbf.MTLN_CDC_SRC_VERSION
      ,cbf.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY cbf.ID ORDER BY cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,cbf.MTLN_CDC_SEQUENCE_NUMBER,cbf.MTLN_CDC_SRC_VERSION,cbf.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY cbf.ID ORDER BY 
    cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP,cbf.MTLN_CDC_SEQUENCE_NUMBER,cbf.MTLN_CDC_SRC_VERSION
   ,cbf.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN cbf.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY cbf.ID ORDER BY  
    cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,cbf.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,cbf.MTLN_CDC_SRC_VERSION DESC
   ,cbf.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED    
--CDC Meta data REQUIRED rows--------------------------------------------------------------------
  ,cbf.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE                    
  ,cbf.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP               
  ,cbf.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER                     
  ,cbf.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID                       
  ,cbf.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP                      
  ,cbf.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR                 
  ,cbf.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION                         
  ,cbf.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME                            
  ,cbf.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH                            
  ,cbf.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                        
  ,cbf.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA                         
  ,cbf.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                           
--foreign keys-------------------------------------------------------------------------------
-- Note:  if the foreign key comes from JSON, cast it to a number as JSON values are typically text
  ,IFNULL(TO_NUMBER(cbf.EMPLOYEE_ID, 10, 0),-1)    AS EMPLOYEE_DIM_FK
  ,IFNULL(cbf.CASH_BANK_ID,-1)                     AS CASHBANK_DIM_FK              
--flags---------------------------------------------------------------------------------------
--Note:  ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--    columns created by the source app should be specifically cast as boolean 
--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
  ,to_timestamp_tz(cbf.CREATED_AT)     AS CREATED_AT 
--names, options, etc-------------------------------------------------------------------------
  ,cbf.NOTES                           AS NOTES
--Counts and Amounts--------------------------------------------------------------------------
--ALL COLUMNS IN THIS SECTION SHOULD END IN COUNT OR AMOUNT
--cOUNTS ARE INTEGERS, AMOUNTS ARE CONINUOUS OR DECIMAL
--if a count and amount exist for the same fact, they should be together with the count first
  ,TO_NUMBER(AMOUNT,38,4)/1000000       AS AMOUNT
-------------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CASH_BANK_EVENT     cbf

;
