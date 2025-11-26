create or replace view SURCHARGE_FACT2(
	SURCHARGE_FACT_PK,
	SURCHARGE_FACT_NK,
	SURCHARGENAME,
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
	SURCHARGE_DIM_NK,
	IS_AUTOAPPLIED,
	IS_GRATUITY,
	IS_TAXABLE,
	IS_TRAINING,
	IS_PRINTONRECEIPT,
	CREATED_AT,
	FISCAL_DATE,
	OPENED_AT,
	UPDATED_AT,
	STATUS,
	CHEQUENUMBER,
	SURCHARGE_TYPE,
	QUANTITY,
	AMOUNT,
	APPLIEDAMOUNT
) as
--============================================================================================
SELECT 
COALESCE(TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(replace(SSS.VALUE:id,'"',''))
  ,TO_VARCHAR(CHK.ID) || '.' || MAX(TO_CHAR(replace(SSS.VALUE:id,'"',''))) OVER (PARTITION BY CHK.ID)
  ,TO_CHAR(CHK.ID)  || '.'||'0')
                                                                         AS SURCHARGE_FACT_PK
-- --natural keys------------------------------------------------------------------------------
  ,COALESCE(TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(replace(SSS.VALUE:id,'"',''))
  ,TO_VARCHAR(CHK.ID) || '.' || MAX(TO_CHAR(replace(SSS.VALUE:id,'"',''))) OVER (PARTITION BY CHK.ID)
  ,TO_CHAR(CHK.ID)  || '.'||'0')  
                                                                         AS SURCHARGE_FACT_NK
-- --name---------------------------------------------------------------------------------------
   ,COALESCE( TO_CHAR(replace(SSS.VALUE:id,'"',''))
  ,MAX(TO_CHAR(replace(SSS.VALUE:id,'"',''))) OVER (PARTITION BY CHK.ID)
  ,'0')                               
                                                                         AS SURCHARGENAME
--data warehouse rows--------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY CHK.ID ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                          AS DW_STARTDATE      
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY CHK.ID
    ,SSS.VALUE:id   ----------------------------
    ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY CHK.ID ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                          AS DW_ENDDATE          
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                              AS DW_ISDELETED        
  ,CASE WHEN RANK() OVER(PARTITION BY CHK.ID ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                          AS DW_ISCURRENTROW 
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
  ,IFNULL(CHK.ID,-1)                                 AS CHEQUE_FACT_FK 
  ,IFNULL(CHK.day_part_id,-1)                        AS DAYPART_DIM_FK
  ,IFNULL(CHK.EMPLOYEE_ID,-1)                        AS EMPLOYEE_DIM_FK
  ,IFNULL(CHK.LOCATION_ID,-1)                        AS LOCATION_DIM_FK
  ,IFNULL(TO_VARCHAR(SSS.value:surchargeId),-1)      AS SURCHARGE_DIM_NK
--flags--------------------------------------------------------------------------------------
  ,case upper(SSS.value:isAutoApplied) when 'TRUE' THEN TRUE else FALSE END
                                                     AS IS_AUTOAPPLIED
  ,case upper(SSS.value:isGratuity) when 'TRUE' THEN TRUE else FALSE END
                                                     AS IS_GRATUITY 
  ,case upper(SSS.value:isTaxable) when 'TRUE' THEN TRUE else FALSE END                              
                                                     AS IS_TAXABLE
  ,CHK.IS_TRAINING                                   AS IS_TRAINING 
 ,case upper(SSS.value:printOnReceip) when 'TRUE' THEN TRUE else FALSE END                         
                                                     AS IS_PRINTONRECEIPT   
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.CREATED_AT)                   AS CREATED_AT 
  ,TO_CHAR(TRY_PARSE_JSON(CHK.info):fiscalDate)::DATE   
                                                     AS FISCAL_DATE 
  ,to_timestamp_tz(CHK.OPENED_AT)                    AS OPENED_AT 
  ,to_timestamp_tz(CHK.UPDATED_AT)                   AS UPDATED_AT

--names, options, etc-------------------------------------------------------------------------
   ,IFNULL(TO_VARCHAR(SSS.value:status),'None')      AS STATUS
--name---------------------------------------------------------------------------------------
  ,CHK.NUMBER                                        AS CHEQUENUMBER  
  ,IFNULL(REPLACE(SSS.value:type,'"',''),'None')     AS SURCHARGE_TYPE
--Counts and Amounts--------------------------------------------------------------------------
 ,IFNULL(SSS.value:quantity,0)::NUMBER(38,2)         AS QUANTITY
 ,IFNULL(SSS.value:value,0)::NUMBER(38,2)            AS AMOUNT
 ,IFNULL(SSS.value:appliedValue,0)::NUMBER(38,2)     AS APPLIEDAMOUNT  
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                                        CHK
  LEFT JOIN (  --LOJ needed to get the correct current row and start dates.  Necessary for cases where the current check row has no surcharges, but a previous row did have surcharges
    SELECT *
      FROM 
        DATALANDING.POSAPI_PUBLIC_CHEQUE              CHK_1 
            ,LATERAL FLATTEN(INPUT => 
            TRY_PARSE_JSON('{surcharges:' || TRY_PARSE_JSON(chk_1.info):surcharges || '}'), PATH => 'surcharges')
                                                      SUR
            WHERE NOT COALESCE(TRUNCATED,FALSE)                               )SSS
       ON CHK.ID = SSS.ID
         AND CHK.MTLN_CDC_SEQUENCE_NUMBER = SSS.MTLN_CDC_SEQUENCE_NUMBER
         AND CHK.MTLN_CDC_SRC_VERSION = SSS.MTLN_CDC_SRC_VERSION
         AND CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP = SSS.MTLN_CDC_LAST_COMMIT_TIMESTAMP
         -- and chk.id = 3287
;