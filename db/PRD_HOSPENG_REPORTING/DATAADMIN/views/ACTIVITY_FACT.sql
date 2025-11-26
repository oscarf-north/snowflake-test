create or replace view ACTIVITY_FACT(
	ACTIVITY_FACT_PK,
	ACTIVITY_FACT_NK,
	ACTIVITY,
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
	VOIDSUMMARY_FACT_FK,
	CHEQUE_FACT_FK,
	DISCOUNT_FACT_FK,
	EMPLOYEE_DIM_FK,
	EMPLOYEE_DIM_FK_AS_PERFORMING_EMPLOYEE,
	EMPLOYEE_DIM_FK_AS_APPROVING_EMPLOYEE,
	ITEM_FACT_FK,
	LOCATION_DIM_FK,
	MENUITEM_DIM_FK,
	IS_DISCOUNT,
	IS_TRAINING,
	IS_VOID,
	IS_VOID_ITEM,
	CREATED_AT,
	UPDATED_AT,
	PERFORMED_AT,
	TABLENAME,
	PERFORMEDBYUSERUUID,
	MENUITEMNAME,
	MENUITEMID,
	TYPE,
	ITEM_ID,
	OVERRIDE,
	LEVEL,
	REASON,
	PRICE
) as
-- --============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  TO_DECIMAL(TO_VARCHAR(CHK.ID) || '.'  || RIGHT('000000000000000' || ACT.INDEX, 16) ,38,16)   
                                                                          AS ACTIVITY_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,TO_DECIMAL(TO_VARCHAR(CHK.ID) || '.'  || RIGHT('000000000000000' || ACT.INDEX, 16) ,38,16)  
                                                                          AS ACTIVITY_FACT_NK
--name---------------------------------------------------------------------------------------
  ,ACT.INDEX                                                              AS ACTIVITY
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY TO_DECIMAL(TO_VARCHAR(CHK.ID) || '.'  || RIGHT('000000000000000' || ACT.INDEX, 16) ,38,16) ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY TO_DECIMAL(TO_VARCHAR(CHK.ID) || '.'  || RIGHT('000000000000000' || ACT.INDEX, 16) ,38,16) ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY TO_DECIMAL(TO_VARCHAR(CHK.ID) || '.'  || RIGHT('000000000000000' || ACT.INDEX, 16) ,38,16) ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY TO_DECIMAL(TO_VARCHAR(CHK.ID) || '.'  || RIGHT('000000000000000' || ACT.INDEX, 16) ,38,16) ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,CHK.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,CHK.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,CHK.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,CHK.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,CHK.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,CHK.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,CHK.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,CHK.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,CHK.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,CHK.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,CHK.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,CASE WHEN TO_CHAR(ACT.value:type) in ('ItemVoided','ItemAdded')
    THEN 
    '.'  ||TO_VARCHAR(REPLACE(ACT.value:updatedUuid,'"',''))
    ELSE '-1'
    END
                                                  AS VOIDSUMMARY_FACT_FK
  ,IFNULL(CHK.id,-1)                              AS CHEQUE_FACT_FK     
  ,IFNULL(IFNULL(CASE WHEN ACT.value:type ILIKE ('%discount%') 
    THEN TO_VARCHAR(CHK.ID) || '.' 
      || TO_VARCHAR(REPLACE(ACT.value:updatedUuid,'"',''))
    END,NULL),'-1')
                                                  AS DISCOUNT_FACT_FK
  ,IFNULL(CHK.employee_id,-1)                     AS EMPLOYEE_DIM_FK   
  ,IFNULL(TO_NUMBER(value:actor.performedByEmployeeId),-1)
                                       AS EMPLOYEE_DIM_FK_AS_PERFORMING_EMPLOYEE
  ,IFNULL(TO_NUMBER(value:actor.approvedByEmployeeId),-1)
                                       AS EMPLOYEE_DIM_FK_AS_APPROVING_EMPLOYEE                                       
,IFNULL(CASE WHEN ACT.value:type ILIKE ANY ('%item%','%DiscountAdded%')THEN
  CHK.ID || '.'  ||TO_VARCHAR(REPLACE(COALESCE(ACT.value:discountInfo.itemId,ACT.value:updatedUuid),'"',''))--THIS PK IS TEXT BECAUSE OF ITEM GUID
  ELSE '-1' END,'-1') 
                                       AS ITEM_FACT_FK

                                       
  ,IFNULL(CHK.location_id ,-1)         AS LOCATION_DIM_FK
  ,IFNULL(TO_NUMBER(value:itemInfo.menuItem.menuItemId),-1)  
                                       AS MENUITEM_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,CASE WHEN ACT.value:type ILIKE ('%discount%') 
        THEN TRUE ELSE FALSE END                       AS IS_DISCOUNT
  ,CHK.IS_TRAINING                                     AS IS_TRAINING
  ,CASE WHEN ACT.value:type ILIKE ('%void%') 
        THEN TRUE ELSE FALSE END                       AS IS_VOID
  ,MAX(CASE WHEN ACT.value:type ILIKE ('%void%') 
        THEN TRUE ELSE FALSE END) 
        OVER (PARTITION BY CHK.MTLN_CDC_SEQUENCE_NUMBER
          ,CHK.ID || '.'  ||TO_VARCHAR(REPLACE(ACT.value:updatedUuid,'"',''))
        )
                                                       AS IS_VOID_ITEM        
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.CREATED_AT)                     AS CREATED_AT   
  ,to_timestamp_tz(CHK.UPDATED_AT)                     AS UPDATED_AT
  ,TO_TIMESTAMP(TO_VARCHAR(value:performedAt))         AS PERFORMED_AT
--names, options, etc-------------------------------------------------------------------------
  ,REPLACE(ACT.value:tableName,'"','')                 AS TABLENAME
  ,REPLACE(ACT.value:performedByUserUuid,'"','')       AS PERFORMEDBYUSERUUID
  ,REPLACE(ACT.value:itemInfo.menuItem.name,'"','')    AS MENUITEMNAME 
  ,TO_NUMBER(ACT.value:itemInfo.menuItem.menuItemId)   AS MENUITEMID  
  ,REPLACE(ACT.value:type,'"','')                      AS TYPE
  ,CASE WHEN TO_CHAR(ACT.value:type) ILIKE ANY ('%item%','%DiscountAdded%')
     THEN TO_VARCHAR(REPLACE(COALESCE(ACT.value:discountInfo.itemId,ACT.value:updatedUuid),'"','')) 
     ELSE NULL END
                                                       AS ITEM_ID
  ,REPLACE(ACT.value:itemInfo.menuItem.override,'"','')AS OVERRIDE
  ,CASE WHEN ACT.value:type ILIKE ('%Item%') THEN 'Item' 
        ELSE CASE WHEN ACT.value:type ILIKE ('%Discount%') 
          THEN ACT.value:discountInfo.application
          ELSE 'None'
  END END                                               AS LEVEL
  ,REPLACE(REPLACE(value:info,'""',''),'reason: ','')   AS REASON
  -- ,AS REASON_ID
--Counts and Amounts--------------------------------------------------------------------------
  ,ACT.value:itemInfo.menuItem.price::NUMBER(38,4)      AS PRICE  
  -------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                  CHK 
    ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON( '{ACTIVITIES:' || CHK.ACTIVITIES || '}' ), PATH => 'ACTIVITIES')
                                                        ACT
WHERE CHK.ACTIVITIES IS NOT NULL
  AND NOT COALESCE(TRUNCATED,FALSE)
  AND CHK.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;