create or replace view CHEQUE_FACT(
	CHEQUE_FACT_PK,
	CHEQUE_FACT_NK,
	CHEQUENUMBER,
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
	DAYPART_DIM_FK,
	EMPLOYEE_DIM_FK,
	LOCATION_DIM_FK,
	ORDERTYPE_DIM_FK,
	ORGANIZATION_DIM_FK,
	SHIFT_DIM_FK,
	TAXSETTINGS_DIM_FK,
	REVENUECENTER_DIM_FK,
	VOIDREASON_DIM_FK,
	HAS_DISCOUNT,
	IS_TRAINING,
	IS_VOID,
	HAS_TRACKTAXESONCOMP,
	FISCAL_DATE_INT,
	FISCAL_DATE,
	OPENED_AT,
	BEGIN_PREP_AT,
	CLOSED_AT,
	SCHEDULED_AT,
	CREATED_AT,
	UPDATED_AT,
	UUID,
	AUDIT,
	STATUS,
	ROUNDINGMETHOD,
	COMBINEDRECEIPTNAME,
	RECEIPTOPTION,
	TAXTRACKING,
	REVENUECENTERNAME,
	REVENUECENTERID,
	STATUS_REASON_ID,
	CHECK_ID,
	TABLE_NAME,
	FEES,
	GIFTCARDS,
	GRATUITIES,
	PARTY_COUNT,
	DISCOUNT_COUNT,
	PAYMENT_COUNT,
	DISCOUNT,
	DISCOUNTCHECK,
	DISCOUNTITEM,
	GROSS,
	INCLUSIVETAX,
	NET,
	PAID,
	SURCHARGE,
	TAX,
	TIP,
	TOTAL,
	UNPAID
) as
--============================================================================================
SELECT 
--primary keys--------------------------------------------------------------------------------
  CHK.ID                                                     AS CHEQUE_FACT_PK
--natural keys--------------------------------------------------------------------------------
  ,CHK.ID                                                    AS CHEQUE_FACT_NK
--name----------------------------------------------------------------------------------------
  ,CHK.NUMBER                                                AS CHEQUENUMBER
--data warehouse rows-------------------------------------------------------------------------            
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(row_number() OVER (
      PARTITION BY CHK.ID ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                            AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY CHK.ID ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(row_number() OVER (PARTITION BY CHK.ID ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN row_number() OVER(PARTITION BY CHK.ID ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data---------------------------------------------------------------------------------
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
  ,IFNULL(CHK.day_part_id,-1)                     AS Daypart_DIM_FK       
  ,IFNULL(CHK.employee_id,-1)                     AS Employee_DIM_FK      
  ,IFNULL(CHK.location_id,-1)                     AS Location_DIM_FK     
  ,IFNULL(CHK.order_type_id,-1)                   AS Ordertype_DIM_FK  
  ,IFNULL(TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.info):orgId),38,0),-1)   
                                                  AS Organization_DIM_FK  
  ,IFNULL(TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.info):shiftId),38,0),-1)
                                                  AS Shift_DIM_FK
  ,IFNULL(TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.info):taxSettings:taxSettingsId),38,4),-1)
                                                  AS Taxsettings_DIM_FK
  ,IFNULL(TO_NUMBER(replace(TRY_PARSE_JSON(CHK.info):revenueCenterId,'"','')),-1)        
                                                  AS REVENUECENTER_DIM_FK 
                                                                   
   ,IFNULL(CASE WHEN CHK.status = 'Voided' 
     THEN CHK.status_reason_id ELSE -1 
     END,-1)                                      AS VoidReason_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,CASE WHEN TRY_PARSE_JSON(CHK.balance):discount > 0 
      THEN TRUE ELSE FALSE END         AS HAS_DISCOUNT 
  ,CHK.IS_TRAINING                     AS IS_TRAINING 
  ,CASE WHEN CHK.STATUS = 'Voided'
      THEN TRUE ELSE FALSE END         AS IS_VOID      
  ,CASE UPPER(TO_CHAR(TRY_PARSE_JSON(CHK.info):taxSettings:trackTaxesOnComps)) 
    WHEN 'TRUE'
    THEN TRUE WHEN 'FALSE' THEN FALSE END

                                       AS HAS_TRACKTAXESONCOMP 
 
--Dates--------------.------------------------------------------------------------------------
  ,CHK.FISCAL_DATE_INT                 AS FISCAL_DATE_INT
  ,TO_CHAR(max(TRY_PARSE_JSON(CHK.info):fiscalDate) over (partition by chk.id))::DATE  
                                       AS FISCAL_DATE                                    
  ,To_timestamp_tz(CHK.opened_at)      AS OPENED_AT
  ,To_timestamp_tz(CHK.begin_prep_at)  AS BEGIN_PREP_AT
  ,To_timestamp_tz(CHK.closed_at)      AS CLOSED_AT
  ,To_timestamp_tz(CHK.scheduled_at)   AS SCHEDULED_AT
  ,To_timestamp_tz(CHK.created_at)     AS CREATED_AT
  ,To_timestamp_tz(CHK.updated_at)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,CHK.UUID                                                                   AS UUID
  ,CHK.AUDIT                                                                  AS AUDIT
  ,CHK.STATUS                                                                 AS STATUS
  ,replace(TRY_PARSE_JSON(CHK.info):taxSettings:roundingMethod,'"','')        AS ROUNDINGMETHOD
  ,replace(TRY_PARSE_JSON(CHK.info):taxSettings:combinedReceiptName,'"','')   AS COMBINEDRECEIPTNAME
  ,replace(TRY_PARSE_JSON(CHK.info):taxSettings:receiptOption,'"','')         AS RECEIPTOPTION
  ,replace(TRY_PARSE_JSON(CHK.info):taxSettings:taxTracking,'"','')           AS TAXTRACKING  
  ,replace(TRY_PARSE_JSON(CHK.info):revenueCenterName,'"','')                 AS REVENUECENTERNAME
  ,TO_NUMBER(replace(TRY_PARSE_JSON(CHK.info):revenueCenterId,'"',''))        AS REVENUECENTERID
  ,CHK.status_reason_id                                                       AS STATUS_REASON_ID
  ,CHK.ID                                                                     AS CHECK_ID
  ,REPLACE(TRY_PARSE_JSON(chk.info):tableName,'"','')                         AS TABLE_NAME
--Counts and Amounts--------------------------------------------------------------------------

  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):fees), 38,4)             AS FEES
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):giftcards), 38,4)        AS GIFTCARDS
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):gratuities), 38,4)       AS GRATUITIES

  ,TRY_PARSE_JSON(chk.info):partySize::number(38,4)                           AS PARTY_COUNT
  ,regexp_count( (TRY_PARSE_JSON(chk.info):discounts) , '"id":' ,1 )          AS DISCOUNT_COUNT
  ,regexp_count( TO_CHAR(CHK.PAYMENTS) , '"id":' ,1 )                         AS PAYMENT_COUNT
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):discount), 38,4)         AS DISCOUNT
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):discountCheck), 38,4)    AS DISCOUNTCHECK
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):discountItem), 38,4)     AS DISCOUNTITEM
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):gross), 38,4)            AS GROSS
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):inclusiveTax), 38,4)     AS INCLUSIVETAX
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):net), 38,4)              AS NET
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):paid), 38,4)             AS PAID
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):surcharge), 38,4)        AS SURCHARGE
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):tax), 38,4)              AS TAX
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):tip), 38,4)              AS TIP
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):total), 38,4)            AS TOTAL
  ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK.balance):unpaid), 38,4)           AS UNPAID
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                   CHK
  WHERE NOT COALESCE(TRUNCATED,FALSE)
    AND CHK.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;