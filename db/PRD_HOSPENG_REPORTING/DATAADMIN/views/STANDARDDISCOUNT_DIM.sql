create or replace view STANDARDDISCOUNT_DIM(
	STANDARDDISCOUNT_DIM_PK,
	STANDARDDISCOUNT_DIM_NK,
	STANDARDDISCOUNTNAME,
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
	IS_TAXABLE,
	DO_AUTO_APPLY,
	REQUIRE_APPROVAL,
	CREATED_AT,
	UPDATED_AT,
	STANDARDDISCOUNTSHORTNAME,
	RECEIPT_NAME,
	OPEN_LIMIT,
	DISCOUNTTYPE,
	DISCOUNTAPPLICATION,
	FIXED_VALUE,
	MIN_CHECK_AMOUNT,
	MAX_CHECK_AMOUNT
) as
--============================================================================================
SELECT 
--primary keys---------------------------------------------------------------------------------
  tab.ID                                                      AS STANDARDDISCOUNT_DIM_PK   
--natural keys---------------------------------------------------------------------------------
  ,tab.ID                                                     AS STANDARDDISCOUNT_DIM_NK   
-- --name--------------------------------------------------------------------------------------
  ,name                                                       AS STANDARDDISCOUNTNAME
--data warehouse REQUIRED rows-----------------------------------------------------------------
  ,TO_TIMESTAMP(TO_CHAR(MTLN_CDC_LAST_COMMIT_TIMESTAMP)  
    || RIGHT(TAB.MTLN_CDC_SEQUENCE_NUMBER,6)) 
                                                              AS DW_STARTDATE         
  ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(TO_CHAR(LEAD(MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY tab.ID ORDER BY tab.MTLN_CDC_SEQUENCE_NUMBER)  
    || RIGHT(LEAD(tab.MTLN_CDC_SEQUENCE_NUMBER) OVER (PARTITION 
    BY tab.ID ORDER BY tab.MTLN_CDC_SEQUENCE_NUMBER)
   ,6)),'9999-09-09 09:09:09.000')))
                                                              AS DW_ENDDATE         
  ,CASE WHEN tab.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                    
                                                              AS DW_ISDELETED         
  ,CASE tab.MTLN_CDC_SEQUENCE_NUMBER 
        WHEN MAX(tab.MTLN_CDC_SEQUENCE_NUMBER) 
        OVER (PARTITION BY tab.ID) THEN TRUE ELSE FALSE END 
                                                              AS DW_ISCURRENTROW     
--CDC Meta data REQUIRED rows-----------------------------------------------------------------
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
  ,tab.ORGANIZATION_id                 AS ORGANIZATION_DIM_FK
-- --flags-----------------------------------------------------------------------------------
  ,tab.IS_ENABLED                      AS IS_ENABLED
  ,tab.is_taxable                      AS IS_TAXABLE
  ,tab.DO_AUTO_APPLY                   AS DO_AUTO_APPLY
  ,tab.REQUIRE_APPROVAL                AS REQUIRE_APPROVAL
-- --Dates--------------.--------------------------------------------------------------------
  ,to_timestamp_tz(tab.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)     AS UPDATED_AT
-- --names, options, etc---------------------------------------------------------------------
  ,tab.short_name                      AS STANDARDDISCOUNTSHORTNAME
  ,tab.receipt_name                    AS RECEIPT_NAME
  ,tab.open_limit                      AS OPEN_LIMIT
  ,tab.type                            AS DISCOUNTTYPE
  ,tab.application                     AS DISCOUNTAPPLICATION
  ,tab.fixed_value                     AS FIXED_VALUE
  ,tab.MIN_CHECK_AMOUNT                AS MIN_CHECK_AMOUNT              
  ,tab.MAX_CHECK_AMOUNT                AS MAX_CHECK_AMOUNT
-- ------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_STANDARD_DISCOUNT     tab
;