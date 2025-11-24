create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.DISCOUNTITEM_FACT(
	DISCOUNTITEM_FACT_PK,
	DISCOUNTITEM_FACT_NK,
	DISCOUNTNAME,
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
	EMPLOYEE_DIM_FK_AS_ADDED_BY,
	EMPLOYEE_DIM_FK_AS_APPROVED_BY,
	EMPLOYEE_DIM_FK,
	LOCATION_DIM_FK,
	ITEM_FACT_FK,
	STANDARDDISCOUNT_DIM_FK,
	DO_AUTOAPPLY,
	IS_TRAINING,
	ADDED_AT,
	CREATED_AT,
	FISCAL_DATE,
	OPENED_AT,
	UPDATED_AT,
	APPLICATION,
	STATUS,
	CHEQUESTATUS,
	DISCOUNTREASON,
	DISCOUNTLEVEL,
	RECEIPTNAME,
	PROMOCODE,
	PROMODESCRIPTION,
	REVENUECENTERNAME,
	ROUNDINGMETHOD,
	CHEQUENUMBER,
	ITEM_ID,
	PROMOCODE_ID,
	DISCOUNT_TYPE,
	DISCOUNT_PERCENT,
	APPLIED_AMOUNT,
	DISCOUNT_AMOUNT,
	VALUE,
	DISCOUNT,
	DISCOUNTCHECK,
	DISCOUNTITEM,
	GROSS,
	NET
) as
--============================================================================================
SELECT 
TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(DIS.value:id)                        
                                                                          AS DISCOUNTITEM_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,TO_VARCHAR(CHK.ID) || '.'  ||replace(ITM.VALUE:id,'"','') || '.' || TO_CHAR(DIS.value:id)                     
                                                                          AS DISCOUNTITEM_FACT_NK
--name---------------------------------------------------------------------------------------
   ,TO_CHAR(DIS.value:id)                                                 AS DISCOUNTNAME
--data warehouse rows--------------------------------------------------------------------------
   ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(DIS.value:id) ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                           AS DW_STARTDATE 
     ,TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate)::DATE                   AS FISCALDATE                                                                           
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(DIS.value:id) ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(DIS.value:id) ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                           AS DW_ENDDATE          
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                               AS DW_ISDELETED               
  ,CASE WHEN RANK() OVER(PARTITION BY TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(DIS.value:id) ORDER BY  
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
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(CHK.ID,-1)                                            AS CHEQUE_FACT_FK 
  ,IFNULL(CHK.day_part_id,-1)                                   AS DAYPART_DIM_FK  

  ,IFNULL(DIS.value:adddedByEmployeeId,-1)::NUMBER              AS EMPLOYEE_DIM_FK_AS_ADDED_BY
  ,IFNULL(DIS.value:approvedByEmployeeId,-1)::NUMBER            AS EMPLOYEE_DIM_FK_AS_APPROVED_BY
  ,IFNULL(CHK.EMPLOYEE_ID,-1)                                   AS EMPLOYEE_DIM_FK
  ,IFNULL(CHK.LOCATION_ID,-1)                                   AS LOCATION_DIM_FK 
  ,CHK.ID || '.'  ||replace(ITM.VALUE:id,'"','')                AS ITEM_FACT_FK
  -- ,IFNULL(TO_VARCHAR(CHK.ID) || '.' 
  -- || TO_VARCHAR(DIS.value:itemId),-1)                           AS ITEM_DIM_FK
  ,IFNULL(TRY_TO_NUMBER(TO_VARCHAR(DIS.value:standardDiscountId)),-1)  
                                                                AS STANDARDDISCOUNT_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,CASE REPLACE(DIS.value:doAutoApply,'"','') 
    WHEN 'false' THEN FALSE
    WHEN 'true'  THEN TRUE ELSE NULL END             AS DO_AUTOAPPLY
   ,CHK.IS_TRAINING                                  AS IS_TRAINING  
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp(to_varchar(DIS.value:addedAt))       AS ADDED_AT
  ,to_timestamp_tz(CHK.CREATED_AT)                   AS CREATED_AT 
  -- ,TO_DATE('20' || CHK.FISCAL_DATE_INT,'YYYY-MM-DD')   AS FISCAL_DATE 
   ,TO_DATE(TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate),'YYYY-MM-DD')   AS FISCAL_DATE 
  ,to_timestamp_tz(CHK.OPENED_AT)                    AS OPENED_AT 
  ,to_timestamp_tz(CHK.UPDATED_AT)                   AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,IFNULL(REPLACE(DIS.value:application,'"',''),'None')                                     AS APPLICATION
  ,IFNULL(REPLACE(DIS.value:status,'"',''),'None')                                          AS STATUS
  ,CHK.STATUS                                                                               AS CHEQUESTATUS  
  ,IFNULL(UPPER(REPLACE(DIS.value:reason,'"','')),'None')                                   AS DISCOUNTREASON
  ,'Item'                                                                                   AS DISCOUNTLEVEL  
  ,IFNULL(REPLACE(DIS.value:receiptName,'"','') ,'None')                                    AS RECEIPTNAME
  ,IFNULL(REPLACE(DIS.value:promoCode,'"',''),'None')                                       AS PROMOCODE
  ,IFNULL(REPLACE(DIS.value:promoDescription,'"',''),'None')                                AS PROMODESCRIPTION
  ,IFNULL(replace(TRY_PARSE_JSON(CHK.info):revenueCenterName,'"','') ,'None')               AS REVENUECENTERNAME
  ,IFNULL(replace(TRY_PARSE_JSON(CHK.info):taxSettings:roundingMethod,'"',''),'None')       AS ROUNDINGMETHOD
--name---------------------------------------------------------------------------------------
  ,CHK.NUMBER                                                                AS CHEQUENUMBER  
  ,TRY_TO_NUMBER(TO_VARCHAR(DIS.value:itemID))                               AS ITEM_ID
  ,TRY_TO_NUMBER(TO_VARCHAR(DIS.
  value:promoCodeId))                                                        AS PROMOCODE_ID
  ,REPLACE(DIS.value:type,'"','')                                            AS DISCOUNT_TYPE
--Counts and Amounts--------------------------------------------------------------------------
,CASE WHEN REPLACE(DIS.value:type,'"','') = 'Percent'
   THEN to_char(REPLACE(DIS.value:value,'"',''))
   ELSE 0 END ::NUMBER(38,4)                     
                                                                             AS DISCOUNT_PERCENT
,TO_CHAR(DIS.value:appliedValue) ::NUMBER(38,4)                              AS APPLIED_AMOUNT

,CASE WHEN REPLACE(DIS.value:type,'"','') = 'Amount' 
     THEN to_char(REPLACE(DIS.value:value,'"',''))
   END::NUMBER(38,4)                  
                                                                              AS DISCOUNT_AMOUNT

   
  ,to_char(REPLACE(DIS.value:value,'"',''))::NUMBER(38,4)  
                                                                              AS VALUE

    
  ,TO_CHAR(TRY_PARSE_JSON(CHK.balance):discount)::NUMBER(38,4)                AS DISCOUNT
  ,TO_CHAR(TRY_PARSE_JSON(CHK.balance):discountCheck)::NUMBER(38,4)           AS DISCOUNTCHECK
  ,TO_CHAR(TRY_PARSE_JSON(CHK.balance):discountItem)::NUMBER(38,4)            AS DISCOUNTITEM
  ,TO_CHAR(TRY_PARSE_JSON(CHK.balance):gross)::NUMBER(38,4)                   AS GROSS
  ,TO_CHAR(TRY_PARSE_JSON(CHK.balance):net)::NUMBER(38,4)                     AS NET
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                                        CHK 
   ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON( '{ITEMS:' || CHK.ITEMS || '}' ), PATH => 'ITEMS')
                                                                             ITM                                                                                                              
    ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON('{discounts:' || TRY_PARSE_JSON(ITM.value):discounts || '}'), PATH => 'discounts')
                                                                             DIS
WHERE TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):discount), 38, 4) > 0
 AND NOT COALESCE(TRUNCATED,FALSE)
 AND MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;
