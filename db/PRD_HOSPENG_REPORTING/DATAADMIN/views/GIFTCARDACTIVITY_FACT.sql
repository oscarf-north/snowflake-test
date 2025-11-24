create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.GIFTCARDACTIVITY_FACT(
	GIFTCARDACTIVITY_FACT_PK,
	GIFTCARDACTIVITY_FACT_NK,
	GIFTCARD,
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
	ACCOUNT_DIM_FK,
	CHEQUE_FACT_FK,
	DAYPART_DIM_FK,
	EMPLOYEE_DIM_FK_AS_CREATOR,
	LOCATION_DIM_FK,
	MERCHANT_DIM_FK,
	ORDERTYPE_DIM_FK,
	REVENUECENTER_DIM_FK,
	STATUSREASON_DIM_FK,
	IS_RELOAD,
	IS_TRAINING,
	CREATED_AT,
	UPDATED_AT,
	OPENED_AT,
	FISCALDATE,
	AUTHGUIDID,
	CHEQUENUMBER,
	CURRENCY_ID,
	FLOORPLAN_ID,
	REVENUECENTERNAME,
	STATUS,
	STATUSREASON,
	TABLENAME,
	PARTYSIZE,
	AMOUNT
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------ 
   CHK.ID || '.' || REPLACE(GCD.value:id,'"','')                    AS GIFTCARDACTIVITY_FACT_PK
--natural keys------------------------------------------------------------------------------
  ,CHK.ID || '.' || REPLACE(GCD.value:id,'"','')                    AS GIFTCARDACTIVITY_FACT_NK
--name---------------------------------------------------------------------------------------
  , REPLACE(GCD.value:id,'"','')                                    AS GIFTCARD
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY 
        CHK.ID || '.'  || REPLACE(GCD.value:id,'"','') 
        ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                      AS DW_STARTDATE        
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
    CHK.ID || '.'  || REPLACE(GCD.value:id,'"','') 
    ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY CHK.ID || '.'  || REPLACE(GCD.value:id,'"','') ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                       AS DW_ENDDATE         
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                           AS DW_ISDELETED          
  ,CASE WHEN RANK() OVER(PARTITION BY  CHK.ID || '.'  || REPLACE(GCD.value:id,'"','') 
    ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                        AS DW_ISCURRENTROW    
--CDC Meta data-------------------------------------------------------------------------------
  ,CHK.MTLN_CDC_LAST_CHANGE_TYPE                                        AS MTLN_CDC_LAST_CHANGE_TYPE
  ,CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                   AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,CHK.MTLN_CDC_SEQUENCE_NUMBER                                         AS MTLN_CDC_SEQUENCE_NUMBER
  ,CHK.MTLN_CDC_LOAD_BATCH_ID                                           AS MTLN_CDC_LOAD_BATCH_ID
  ,CHK.MTLN_CDC_LOAD_TIMESTAMP                                          AS MTLN_CDC_LOAD_TIMESTAMP
  ,CHK.MTLN_CDC_PROCESSED_DATE_HOUR                                     AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,CHK.MTLN_CDC_SRC_VERSION                                             AS MTLN_CDC_SRC_VERSION
  ,CHK.MTLN_CDC_FILENAME                                                AS MTLN_CDC_FILENAME
  ,CHK.MTLN_CDC_FILEPATH                                                AS MTLN_CDC_FILEPATH
  ,CHK.MTLN_CDC_SRC_DATABASE                                            AS MTLN_CDC_SRC_DATABASE
  ,CHK.MTLN_CDC_SRC_SCHEMA                                              AS MTLN_CDC_SRC_SCHEMA
  ,CHK.MTLN_CDC_SRC_TABLE                                               AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(REPLACE(GCD.value:account,'"',''),'-1')                       AS ACCOUNT_DIM_FK
  ,IFNULL(CHK.ID,-1)                                                    AS CHEQUE_FACT_FK
  ,IFNULL(CHK.DAY_PART_ID,-1)                                           AS DAYPART_DIM_FK
  ,IFNULL(CHK.EMPLOYEE_ID,-1)                                           AS EMPLOYEE_DIM_FK_AS_CREATOR
  ,IFNULL(CHK.LOCATION_ID,-1)                                           AS LOCATION_DIM_FK
  ,IFNULL(REPLACE(GCD.value:merchantId,'"',''),'-1')                    AS MERCHANT_DIM_FK  
  ,IFNULL(CHK.order_type_id,-1)                                         AS ORDERTYPE_DIM_FK    
  ,IFNULL(TO_NUMBER(replace(TRY_PARSE_JSON(
      CHK.info):revenueCenterId,'"','')),-1)                            AS REVENUECENTER_DIM_FK    
  ,IFNULL(REPLACE(GCD.value:statusReasonId,'"',''),-1)                  AS STATUSREASON_DIM_FK                                                      
--flags---------------------------------------------------------------------------------------
  ,REPLACE(GCD.value:isReload,'"','')                                   AS IS_RELOAD
  ,CHK.IS_TRAINING                                                      AS IS_TRAINING
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.CREATED_AT)                                      AS CREATED_AT   
  ,to_timestamp_tz(CHK.UPDATED_AT)                                      AS UPDATED_AT
 -- ,to_timestamp_tz(replace(value:ccData:expirationDate,'"',''))       AS EXPIRATIONDATE   
  ,to_timestamp_tz(CHK.opened_at)                                       AS OPENED_AT
  ,TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate)::DATE                   AS FISCALDATE
--names, options, etc-------------------------------------------------------------------------
  ,REPLACE(GCD.value:authGuid,'"','')                                   AS AUTHGUIDID
  ,CHK.NUMBER                                                           AS CHEQUENUMBER
  ,REPLACE(TRY_PARSE_JSON(info):currencyId,'"','')                      AS CURRENCY_ID  
  ,TO_NUMBER(REPLACE(TRY_PARSE_JSON(info):floorplanId,'"',''))          AS FLOORPLAN_ID
  ,REPLACE(TRY_PARSE_JSON(info):revenueCenterName,'"','')               AS REVENUECENTERNAME
  ,REPLACE(GCD.value:status,'"','')                                     AS STATUS
  ,REPLACE(GCD.value:statusReason,'"','')                               AS STATUSREASON 
  ,REPLACE(TRY_PARSE_JSON(info):tableName,'"','')                       AS TABLENAME
--Counts and Amounts--------------------------------------------------------------------------
   ,REPLACE(TRY_PARSE_JSON(info):partySize,'"','')::NUMBER(38)          AS PARTYSIZE
   ,GCD.value:amount::NUMBER(38,4)                                      AS AMOUNT
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                                   CHK 
    ,LATERAL FLATTEN(INPUT => 
    PARSE_JSON( '{GIFTCARDS:' || CHK.GIFT_CARDS || '}' ),  
        path => 'GIFTCARDS')                                            GCD
  WHERE CHK.GIFT_CARDS <> '__value_not_modified__'  
    AND NOT COALESCE(TRUNCATED,FALSE)
    AND CHK.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;
