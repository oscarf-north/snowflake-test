create or replace view PAYINPAYOUTREASON_DIM(
	PAYINPAYOUTREASON_DIM_PK,
	PAYINPAYOUTREASON_DIM_NK,
	PAYINPAYOUTREASON,
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
	IS_ENABLED,
	IS_PAY_IN,
	CREATED_AT,
	UPDATED_AT,
	TYPE,
	DESCRIPTION
) as
--============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  vdr.ID                                                      AS PayInPayOutReason_DIM_PK  
--natural keys--------------------------------------------------------------------------------
  ,vdr.ID                                                     AS PayInPayOutReason_DIM_NK  
--name-----------------------------------------------------------------------------------------
  ,vdr.NAME                                                   AS PayInPayOutReason
--data warehouse REQUIRED rows-----------------------------------------------------------------
  ,TO_TIMESTAMP(TO_CHAR(MTLN_CDC_LAST_COMMIT_TIMESTAMP)  
    || RIGHT(vdr.MTLN_CDC_SEQUENCE_NUMBER,6)) 
    
                                                              AS DW_STARTDATE         
  ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(TO_CHAR(LEAD(MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY vdr.ID ORDER BY vdr.MTLN_CDC_SEQUENCE_NUMBER)  
    || RIGHT(LEAD(vdr.MTLN_CDC_SEQUENCE_NUMBER) OVER (PARTITION 
    BY vdr.ID ORDER BY vdr.MTLN_CDC_SEQUENCE_NUMBER)
   ,6)),'9999-09-09 09:09:09.000')))
                                                              AS DW_ENDDATE        
  ,CASE WHEN vdr.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                    
                                                              AS DW_ISDELETED       
  ,CASE vdr.MTLN_CDC_SEQUENCE_NUMBER 
        WHEN MAX(vdr.MTLN_CDC_SEQUENCE_NUMBER) 
        OVER (PARTITION BY vdr.ID) THEN TRUE ELSE FALSE END 
                                                              AS DW_ISCURRENTROW     
-- --CDC Meta data REQUIRED rows------------------------------------------------------------------
  ,vdr.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE                  
  ,vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP              
  ,vdr.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER                   
  ,vdr.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID                      
  ,vdr.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP                    
  ,vdr.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR                
  ,vdr.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION                        
  ,vdr.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME                           
  ,vdr.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH                           
  ,vdr.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                       
  ,vdr.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA                        
  ,vdr.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                          
-- --foreign keys--------------------------------------------------------------------------------
  ,vdr.ORGANIZATION_ID                 AS ORGANIZATION_DIM_FK
-- --flags---------------------------------------------------------------------------------------
  ,vdr.IS_ENABLED                      AS IS_ENABLED
  ,vdr.IS_PAY_IN                       AS IS_PAY_IN
-- --Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(vdr.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(vdr.UPDATED_AT)     AS UPDATED_AT
-- --names, options, etc-------------------------------------------------------------------------
  ,CASE WHEN vdr.IS_PAY_IN THEN 'Pay In'
    ELSE 'Pay Out'  END                AS TYPE
  ,vdr.description                     AS DESCRIPTION
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_PAY_IN_OUT_REASON    vdr
;