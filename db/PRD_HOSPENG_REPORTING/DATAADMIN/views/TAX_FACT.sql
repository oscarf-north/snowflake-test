create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.TAX_FACT(
	TAX_FACT_PK,
	TAX_FACT_NK,
	TAXRATENAME,
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
	CHEQUE_FACT_FK,
	DAYPART_DIM_FK,
	EMPLOYEE_DIM_FK,
	ITEM_FACT_FK,
	LOCATION_DIM_FK,
	MENUITEM_DIM_FK,
	MENUITEMNAME_DIM_FK,
	ORDERTYPE_DIM_FK,
	TAX_RATE_ID,
	IS_TRAINING,
	IS_TAX_INCLUDED,
	CLOSED_AT,
	CREATED_AT,
	FISCAL_DATE,
	OPENED_AT,
	UPDATED_AT,
	CHECKSTATUS,
	ITEMSTATUS,
	REVENUECENTERNAME,
	ROUNDINGMETHOD,
	CHEQUENUMBER,
	ITEM_ID,
	CHECKTOTALINCLUSIVETAX,
	CHECKTOTALTAX,
	TAXBASIS,
	AMOUNT,
	PERCENT
) as

--============================================================================================
SELECT  
TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(TAX.index)                        
                                                                             AS TAX_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(TAX.index)                     
                                                                             AS TAX_FACT_NK
--name---------------------------------------------------------------------------------------
 , REPLACE(TAX.value:taxRateName,'"','')                                     AS TAXRATENAME
-- --data warehouse rows--------------------------------------------------------------------------
   ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(row_number() OVER (
      PARTITION BY 
      
     TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(TAX.index)
      
       ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                             AS DW_STARTDATE       
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
    
    TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(TAX.index)
    
     ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(row_number() OVER (
    
    PARTITION BY 
    TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(TAX.index)
    
     ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                           AS DW_ENDDATE          
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                               AS DW_ISDELETED               
  ,CASE WHEN row_number() OVER(
  PARTITION BY 
   TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(TAX.index)
  
  ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                           AS DW_ISCURRENTROW     
--CDC Meta data-------------------------------------------------------------------------------
  ,CHK.MTLN_CDC_LAST_CHANGE_TYPE                     AS MTLN_CDC_LAST_CHANGE_TYPE
  ,CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP                AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,CHK.MTLN_CDC_SEQUENCE_NUMBER                      AS MTLN_CDC_SEQUENCE_NUMBER
  ,CHK.MTLN_CDC_LOAD_BATCH_ID                        AS MTLN_CDC_LOAD_BATCH_ID
  ,CHK.MTLN_CDC_LOAD_TIMESTAMP                       AS MTLN_CDC_LOAD_TIMESTAMP
  ,CHK.MTLN_CDC_PROCESSED_DATE_HOUR                  AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,CHK.MTLN_CDC_SRC_VERSION                          AS MTLN_CDC_SRC_VERSION
  ,CHK.MTLN_CDC_FILENAME                             AS MTLN_CDC_FILENAME
  ,CHK.MTLN_CDC_FILEPATH                             AS MTLN_CDC_FILEPATH
  ,CHK.MTLN_CDC_SRC_DATABASE                         AS MTLN_CDC_SRC_DATABASE
  ,CHK.MTLN_CDC_SRC_SCHEMA                           AS MTLN_CDC_SRC_SCHEMA
  ,CHK.MTLN_CDC_SRC_TABLE                            AS MTLN_CDC_SRC_TABLE
-- --foreign keys-------------------------------------------------------------------------------
  ,IFNULL(CHK.ID,-1)                                 AS CHEQUE_FACT_FK 
  ,IFNULL(CHK.day_part_id,-1)                        AS DAYPART_DIM_FK  
  ,IFNULL(CHK.EMPLOYEE_ID,-1)                        AS EMPLOYEE_DIM_FK
  ,CHK.ID || '.'  || replace(ITM.VALUE:id,'"','')    AS ITEM_FACT_FK
  ,IFNULL(CHK.LOCATION_ID,-1)                        AS LOCATION_DIM_FK  
  -- ,IFNULL(TO_NUMBER(replace(replace(split_part(
  --   replace(ITM.value:menuItem:path,'',''),'.',2),'{',''),'}','') ),-1)
  --                                                   AS MENUGROUP_DIM_FK
  ,IFNULL(TO_NUMBER(ITM.value:menuItem:menuItemId),-1)       
                                                     AS MENUITEM_DIM_FK     
  ,IFNULL(TO_NUMBER(ITM.value:menuItem:menuItemNameId),-1)   
                                                     AS MENUITEMNAME_DIM_FK
  ,IFNULL(CHK.order_type_id,-1)                      AS ORDERTYPE_DIM_FK                                                
  ,TAX.value:taxRateId::NUMBER(38,0)                 AS TAX_RATE_ID  



-- --flags---------------------------------------------------------------------------------------
   ,CHK.IS_TRAINING::BOOLEAN                         AS IS_TRAINING 
   ,TAX.value:isTaxIncluded::BOOLEAN                 AS IS_TAX_INCLUDED
-- --Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.closed_at)                    AS CLOSED_AT
  ,to_timestamp_tz(CHK.CREATED_AT)                   AS CREATED_AT 
  ,TO_CHAR(max(TRY_PARSE_JSON(CHK.info):fiscalDate) over (partition by chk.id))::DATE    
                                                     AS FISCAL_DATE 
  ,to_timestamp_tz(CHK.OPENED_AT)                    AS OPENED_AT 
  ,to_timestamp_tz(CHK.UPDATED_AT)                   AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,CHK.STATUS                                        AS CHECKSTATUS
  ,replace(ITM.value:status,'"','')                  AS ITEMSTATUS
  ,IFNULL(replace(TRY_PARSE_JSON(CHK.info):revenueCenterName,'"','') ,'None')               
                                                     AS REVENUECENTERNAME
  ,IFNULL(replace(TRY_PARSE_JSON(CHK.info):taxSettings:roundingMethod,'"',''),'None')       
                                                     AS ROUNDINGMETHOD
-- --name---------------------------------------------------------------------------------------
  ,CHK.NUMBER                                                                AS CHEQUENUMBER  
  ,replace(ITM.VALUE:id,'"','')                                              AS ITEM_ID
--Counts and Amounts--------------------------------------------------------------------------
 ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):inclusiveTax), 38,4)     AS CHECKTOTALINCLUSIVETAX
 ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):tax), 38,4)              AS CHECKTOTAlTAX
 ,TAX.value:taxBasis::NUMBER(38,4)                                           AS TAXBASIS
 ,TAX.value:tax::NUMBER(38,4)                                                AS AMOUNT
 ,TAX.value:percent::NUMBER(38,4)                                            AS PERCENT
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                          CHK 
    ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON( '{ITEMS:' || CHK.ITEMS || '}' ), PATH => 'ITEMS')
                                                               ITM 
  ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON('{taxes:' || TRY_PARSE_JSON(ITM.value):taxes || '}'), PATH => 'taxes')
                                                               TAX 
WHERE NOT COALESCE(TRUNCATED,FALSE)
  AND MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
   -- and TAX_FACT_NK = '53368.5a713e5d-7a4a-4e4b-a69f-1669bdb86e96.0'
   -- order by chk.MTLN_CDC_SEQUENCE_NUMBER
;