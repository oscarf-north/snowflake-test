create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.DISCOUNTREASON_DIM(
	DISCOUNTREASON_DIM_PK,
	DISCOUNTREASON_DIM_NK,
	DISCOUNTREASON,
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
	IS_ENABLED,
	CREATED_AT,
	UPDATED_AT,
	DESCRIPTION
) as
--============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  vdr.ID                                                      AS DiscountReason_DIM_PK  
--natural keys--------------------------------------------------------------------------------
  ,vdr.ID                                                     AS DiscountReason_DIM_NK  
--name-----------------------------------------------------------------------------------------
  ,vdr.REASON                                                 AS DiscountReason
--data warehouse REQUIRED rows-----------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY vdr.ID ORDER BY vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,vdr.MTLN_CDC_SEQUENCE_NUMBER,vdr.MTLN_CDC_SRC_VERSION
      ,vdr.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY vdr.ID ORDER BY vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,vdr.MTLN_CDC_SEQUENCE_NUMBER,vdr.MTLN_CDC_SRC_VERSION,vdr.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY vdr.ID ORDER BY 
    vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP,vdr.MTLN_CDC_SEQUENCE_NUMBER,vdr.MTLN_CDC_SRC_VERSION
   ,vdr.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN vdr.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY vdr.ID ORDER BY  
    vdr.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,vdr.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,vdr.MTLN_CDC_SRC_VERSION DESC
   ,vdr.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED 
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
-- --flags---------------------------------------------------------------------------------------
  ,vdr.IS_ENABLED                      AS IS_ENABLED
-- --Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(vdr.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(vdr.UPDATED_AT)     AS UPDATED_AT
-- --names, options, etc-------------------------------------------------------------------------
  ,'None'                              AS DESCRIPTION
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_DISCOUNT_REASON    vdr
;
