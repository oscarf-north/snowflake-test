create or replace view ITEM_FACT(
	ITEM_FACT_PK,
	ITEM_FACT_NK,
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
	LOCATION_DIM_FK,
	ORDERTYPE_DIM_FK,
	MENUITEM_DIM_FK,
	MENUITEMNAME_DIM_FK,
	VOIDREASON_DIM_FK,
	VARIANT_DIM_FK,
	HASMODIFIERS,
	IS_TRAINING,
	IS_VOID,
	FISCAL_DATE_INT,
	FISCAL_DATE,
	OPENED_AT,
	BEGIN_PREP_AT,
	CLOSED_AT,
	SCHEDULED_AT,
	CREATED_AT,
	UPDATED_AT,
	SPLITBY,
	ITEMSTATUS,
	CHECKSTATUS,
	STATUSREASON_ID,
	STATUSREASON,
	COMBINEDNAME,
	MODIFIERS,
	NOTE,
	DESCRIPTION,
	NAME,
	PATH,
	VARIANTNAME,
	REVENUECENTERNAME,
	CHECK_ID,
	EMPLOYEE_ID,
	ITEM_ID,
	LOCATION_ID,
	REVENUECENTER_ID,
	ORDER_TYPE_ID,
	CHEQUENUMBER,
	APPLIEDAMOUNT,
	QUANTITY,
	REPORTQUANTITY,
	BASEPRICE,
	INCLUSIVETAX,
	TAB,
	DISCOUNTCHECK,
	DISCOUNTITEM,
	PRICE,
	GROSS,
	NET,
	TAX,
	TOTAL,
	CHECKGROSS,
	CHECKTOTAL
) as
--===========================================================================================
SELECT
--primary keys-------------------------------------------------------------------------------
   CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','')--THIS PK IS TEXT BEreCAUSE OF ITEM GUID
                                                              AS ITEM_FACT_PK
--natural keys-f------------------------------------------------------------------------------
  ,CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','')              AS ITEM_FACT_NK
--name---------------------------------------------------is-----------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(row_number() OVER (
      PARTITION BY  CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','') ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY  CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','') ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(row_number() OVER (PARTITION BY  CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','') ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN row_number() OVER(PARTITION BY  CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','') ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESc
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END        AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data------------------------------------------------------------------------------
  ,CHK.MTLN_CDC_LAST_CHANGE_TYPE        AS MTLN_CDC_LAST_CHANGE_TYPE
  ,CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP   AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,CHK.MTLN_CDC_SEQUENCE_NUMBER         AS MTLN_CDC_SEQUENCE_NUMBER
  ,CHK.MTLN_CDC_LOAD_BATCH_ID           AS MTLN_CDC_LOAD_BATCH_ID
  ,CHK.MTLN_CDC_LOAD_TIMESTAMP          AS MTLN_CDC_LOAD_TIMESTAMP
  ,CHK.MTLN_CDC_PROCESSED_DATE_HOUR     AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,CHK.MTLN_CDC_SRC_VERSION             AS MTLN_CDC_SRC_VERSION
  ,CHK.MTLN_CDC_FILENAME                AS MTLN_CDC_FILENAME
  ,CHK.MTLN_CDC_FILEPATH                AS MTLN_CDC_FILEPATH
  ,CHK.MTLN_CDC_SRC_DATABASE            AS MTLN_CDC_SRC_DATABASE
  ,CHK.MTLN_CDC_SRC_SCHEMA              AS MTLN_CDC_SRC_SCHEMA
  ,CHK.MTLN_CDC_SRC_TABLE               AS MTLN_CDC_SRC_TABLE
-- --foreign keys-------------------------------------------------------------------------------
  ,IFNULL(CHK.ID,-1)                    AS CHEQUE_FACT_FK      --one checque,many items
  ,IFNULL(CHK.day_part_id,-1)           AS DAYPART_DIM_FK  
  ,IFNULL(CHK.employee_id,-1)           AS EMPLOYEE_DIM_FK     
  ,IFNULL(CHK.location_id,-1)           AS LOCATION_DIM_FK  
  ,IFNULL(CHK.order_type_id,-1)         AS ORDERTYPE_DIM_FK  

  -- ,IFNULL(TO_NUMBER(replace(replace(split_part(
  --   replace(ITM.value:menuItem:path,'',''),'.',2),'{',''),'}','') ),-1)
  --                                       AS MENUGROUP_DIM_FK
  ,IFNULL(TO_NUMBER(ITM.value:menuItem:menuItemId),-1)       
                                        AS MENUITEM_DIM_FK     
  ,IFNULL(TO_NUMBER(ITM.value:menuItem:menuItemNameId),-1)   
                                        AS MENUITEMNAME_DIM_FK 
  ,IFNULL(CASE WHEN replace(ITM.value:status,'"','') 
     = 'Voided' 
    THEN TO_NUMBER(replace(ITM.value:statusReasonId,'"',''),38,0 )
    ELSE -1 END,-1)                     AS VoidReason_DIM_FK
  ,IFNULL(TO_NUMBER(replace(ITM.value:menuItem:variantId,'','')),-1)       
                                        AS VARIANT_DIM_FK      
-- --flags---------------------------------------------------------------------------------------
  ,CASE ITM.value:modifiers
    WHEN NULL THEN FALSE
    ELSE TRUE END                        AS HASMODIFIERS 
 ,CHK.IS_TRAINING                        AS IS_TRAINING
 ,CASE WHEN replace(ITM.value:status,'"','')
   = 'Voided' THEN TRUE ELSE FALSE END   AS IS_VOID
-- --Dates--------------.-----------------------------------------------------------------------
  ,CHK.FISCAL_DATE_INT                 AS FISCAL_DATE_INT                    
  ,(TO_CHAR(max(TRY_PARSE_JSON(CHK.info):fiscalDate) over (partition by chk.id)))::DATE   
                                       AS FISCAL_DATE              
  ,to_timestamp_tz(CHK.opened_at)      AS OPENED_AT
  ,to_timestamp_tz(CHK.begin_prep_at)  AS BEGIN_PREP_AT
  ,to_timestamp_tz(CHK.closed_at)      AS CLOSED_AT
  ,to_timestamp_tz(CHK.scheduled_at)   AS SCHEDULED_AT
  ,to_timestamp_tz(CHK.created_at)     AS CREATED_AT
  ,to_timestamp_tz(CHK.UPDATED_AT)     AS UPDATED_AT
-- --names,etc---------------------------------------------------------------------------------------
  ,TO_NUMBER(ITM.value:splitBy  )                              AS SPLITBY
  ,replace(ITM.value:status,'"','')                            AS ITEMSTATUS
  ,CHK.STATUS                                                  AS CHECKSTATUS
  ,replace(ITM.value:statusReasonId,'"','')                    AS STATUSREASON_ID
  ,ifnull(replace(ITM.value:statusReason,'"',''),'None')       AS STATUSREASON
  ,replace(ITM.value:menuItem:combinedName,'','')              AS COMBINEDNAME
  ,replace(ITM.value:modifiers,'"','')                         AS MODIFIERS
  ,ifnull(replace(ITM.value:note,'"',''),' ')                  AS NOTE
  ,replace(ITM.value:menuItem:description,'','')               AS DESCRIPTION
  ,replace(ITM.value:menuItem:name,'','')                      AS NAME
  ,replace(ITM.value:menuItem:path,'','')                      AS PATH    
  ,ifnull(replace(ITM.value:menuItem:variantName,'',''),' ')   AS VARIANTNAME
  ,replace(TRY_PARSE_JSON(CHk.info):revenueCenterName,'"','')  AS REVENUECENTERNAME
  ,CHK.ID                                                      AS CHECK_ID
  ,CHK.EMPLOYEE_ID                                             AS EMPLOYEE_ID  
  ,replace(ITM.VALUE:id,'"','')                                AS ITEM_ID
  ,CHK.LOCATION_ID                                             AS LOCATION_ID
  ,TO_NUMBER(replace(TRY_PARSE_JSON(CHK.info):revenueCenterId,'"',''))    AS REVENUECENTER_ID  
  ,CHK.ORDER_TYPE_ID                                           AS ORDER_TYPE_ID
  ,CHK.NUMBER                                                  AS CHEQUENUMBER
-- --counts and amounts--------------------------------------------------------------------------------
  ,CAST(ITM.value:appliedAmount AS DECIMAL(38,4))              AS APPLIEDAMOUNT
  ,CAST(ITM.value:quantity AS DECIMAL(38,4))                   AS QUANTITY
  ,IFNULL(CAST(ITM.value:reportQuantity AS DECIMAL(38,4)),1)   AS REPORTQUANTITY
  ,CAST(ITM.value:basePrice AS DECIMAL(38,4))                  AS BASEPRICE
  ,CAST(ITM.value:inclusiveTax AS DECIMAL(38,4))               AS INCLUSIVETAX
  ,CAST(ITM.value:discount AS DECIMAL(38,4))                   AS TAB
  ,CAST(ITM.value:discountCheck AS DECIMAL(38,4))              AS DISCOUNTCHECK
  ,CAST(ITM.value:discountItem AS DECIMAL(38,4))               AS DISCOUNTITEM
  ,CAST(ITM.value:menuItem:price AS DECIMAL(38,4))             AS PRICE
  ,CAST(ITM.value:gross AS DECIMAL(38,4))                      AS GROSS  
  ,CAST(ITM.value:net AS DECIMAL(38,4))                        AS NET  
  ,CAST(ITM.value:tax AS DECIMAL(38,4))                        AS TAX
  ,CAST(ITM.value:total AS DECIMAL(38,4))                      AS TOTAL 
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):gross), 38, 4)           
                                                               AS CHECKGROSS
 ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):total), 38, 4)           
                                                               AS CHECKTOTAL                                                               
  -------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                          CHK 
    ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON( '{ITEMS:' || CHK.ITEMS || '}' ), PATH => 'ITEMS')
                                                               ITM 
 WHERE NOT COALESCE(CHK.TRUNCATED,FALSE)   
   AND CHK.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
       -- and item_fact_nk = '53344.3f6e9f4e-f9ed-47ff-875f-580df5f695c2'            
;