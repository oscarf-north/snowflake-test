create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.ITEMMODIFIER_DIM(
	ITEMMODIFIER_DIM_PK,
	ITEMMODIFIER_DIM_NK,
	MODIFICATION_ID,
	DW_STARTDATE,
	FISCALDATE,
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
	MODIFIERGROUP_DIM_FK,
	MODIFIER_DIM_FK,
	IS_TRAINING,
	CREATED_AT,
	FISCAL_DATE,
	OPENED_AT,
	UPDATED_AT,
	CHEQUESTATUS,
	MODIFIER_GROUP,
	MODIFIER,
	MODIFIER_TYPE,
	REVENUECENTERNAME,
	ROUNDINGMETHOD,
	CHEQUENUMBER,
	PRICE
) as
-- select * from ITEMMODIFIER_DIM where MTLN_CDC_LAST_CHANGE_TYPE ='d' 
--============================================================================================
-- select * From ITEMMODIFIER_DIM;
----------------------------------------------------------------------------------------------
SELECT 
TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(MOD.index)                        
                                                                          AS ITEMMODIFIER_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(MOD.index)                     
                                                                          AS ITEMMODIFIER_DIM_NK
--name---------------------------------------------------------------------------------------
   ,TO_CHAR(MOD.index)                                                    AS MODIFICATION_ID
--data warehouse rows--------------------------------------------------------------------------
   ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY 
         TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(MOD.index)     
          ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                           AS DW_STARTDATE 
     ,TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate)::DATE                   AS FISCALDATE                                                                           
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
      TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(MOD.index)   
      ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY 
    TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(MOD.index) 
    ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                           AS DW_ENDDATE          
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                               AS DW_ISDELETED               
  ,CASE WHEN RANK() OVER(PARTITION BY 
  TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(MOD.index)  
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
  ,IFNULL(CHK.ID || '.'  ||replace(ITM.VALUE:id,'"',''),'-1')                           
                                                     AS ITEM_FACT_FK
  ,IFNULL(CHK.LOCATION_ID,-1)                        AS LOCATION_DIM_FK                                                      
  ,IFNULL(MOD.value:groupId,-1)::number(38,0)        AS MODIFIERGROUP_DIM_FK                          
  ,IFNULL(MOD.index,-1)::number(38,0)             AS MODIFIER_DIM_FK

-- --flags---------------------------------------------------------------------------------------
   ,CHK.IS_TRAINING                                  AS IS_TRAINING  
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.CREATED_AT)                   AS CREATED_AT 
   ,TO_DATE(TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate),'YYYY-MM-DD')   
                                                     AS FISCAL_DATE 
  ,to_timestamp_tz(CHK.OPENED_AT)                    AS OPENED_AT 
  ,to_timestamp_tz(CHK.UPDATED_AT)                   AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,CHK.STATUS                                                                AS CHEQUESTATUS  
  ,REPLACE(MOD.value:groupName,'"','')                                       AS MODIFIER_GROUP
  ,REPLACE(MOD.value:name,'"','')                                            AS MODIFIER
  ,REPLACE(MOD.value:type,'"','')                                            AS MODIFIER_TYPE
  ,IFNULL(replace(TRY_PARSE_JSON(CHK.info):revenueCenterName,'"','') ,'None')AS REVENUECENTERNAME
  ,IFNULL(replace(TRY_PARSE_JSON(CHK.info):taxSettings:roundingMethod,'"',''),'None')       
                                                                             AS ROUNDINGMETHOD
--name---------------------------------------------------------------------------------------
  ,CHK.NUMBER                                                                AS CHEQUENUMBER  
--Counts and Amounts--------------------------------------------------------------------------
  ,MOD.value:price::decimal(38,2)                                             AS PRICE
----------------------------------------------------------------------------------------------
-- select
-- --12478.a661b719-79c6-4a53-8e8c-3c4e7aa95cee.689
-- TO_VARCHAR(CHK.ID) ,replace(ITM.VALUE:id,'"',''), TO_CHAR(MOD.index),mod.value,mod.index
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                                        CHK 
   ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON( '{ITEMS:' || CHK.ITEMS || '}' ), PATH => 'ITEMS')
                                                                             ITM                                                                                               
    ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON('{modifiers:' || TRY_PARSE_JSON(ITM.value):modifiers || '}'), PATH => 'modifiers')
                                                                             MOD
 WHERE NOT COALESCE(TRUNCATED,FALSE)
   and MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
 --CHK.ID
--    and chk.id = 12478 and MTLN_CDC_SEQUENCE_NUMBER = 900815384
--      and replace(ITM.VALUE:id,'"','') = 'a661b719-79c6-4a53-8e8c-3c4e7aa95cee'
-- order by  TO_VARCHAR(CHK.ID) ,replace(ITM.VALUE:id,'"',''), TO_CHAR(MOD.index),mod.value    
;