create or replace view FEETAX_FACT(
	FEETAX_FACT_PK,
	FEETAX_FACT_NK,
	FEENAME,
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
	TAXRATEDIM_DIM_FK,
	IS_AUTOAPPLIED,
	IS_GRATUITY,
	IS_TAXABLE,
	IS_TAXINCLUDED,
	IS_TRAINING,
	IS_PRINTONRECEIPT,
	CREATED_AT,
	FISCAL_DATE,
	OPENED_AT,
	UPDATED_AT,
	STATUS,
	CHEQUENUMBER,
	SURCHARGE_TYPE,
	TAXRATENAME,
	PERCENT,
	APPLIEDAMOUNT,
	TAX,
	TAXBASIS
) as

--============================================================================================
SELECT  TO_VARCHAR(CHK.ID) || '.' || sss.SURCHARGEVALUE:id || '.' || TO_CHAR(sss.TAXKEY)
                                                                         AS FEETAX_FACT_PK                                                                
-- -- --natural keys------------------------------------------------------------------------------
  ,TO_VARCHAR(CHK.ID) || '.' || sss.SURCHARGEVALUE:id || '.'
   || TO_CHAR(sss.TAXKEY)
                                                                         AS FEETAX_FACT_NK
-- -- --name---------------------------------------------------------------------------------------
  ,COALESCE( TO_CHAR(replace(SSS.SURCHARGEVALUE:id,'"',''))
  ,MAX(TO_CHAR(replace(SSS.SURCHARGEVALUE:id,'"',''))) OVER (PARTITION BY CHK.ID)
  ,'0')                               
                                                                         AS FEENAME
-- --data warehouse rows--------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(row_number() OVER (
      PARTITION BY TO_VARCHAR(CHK.ID) || '.' || sss.SURCHARGEVALUE:id || '.' || TO_CHAR(sss.TAXKEY) 
      ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
      ,CHK.MTLN_CDC_FILENAME)),6))
                                                                          AS DW_STARTDATE      
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY TO_VARCHAR(CHK.ID) || '.' || sss.SURCHARGEVALUE:id || '.' || TO_CHAR(sss.TAXKEY)
    ORDER BY CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION,CHK.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(row_number() OVER (PARTITION BY CHK.ID ORDER BY 
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP,CHK.MTLN_CDC_SEQUENCE_NUMBER,CHK.MTLN_CDC_SRC_VERSION
   ,CHK.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                                          AS DW_ENDDATE          
  
  ,CASE WHEN CHK.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                              AS DW_ISDELETED        
  ,CASE WHEN row_number() OVER(PARTITION BY TO_VARCHAR(CHK.ID) || '.' || sss.SURCHARGEVALUE:id || '.' || TO_CHAR(sss.TAXKEY) 
    ORDER BY  
    CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,CHK.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,CHK.MTLN_CDC_SRC_VERSION DESC
   ,CHK.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                                          AS DW_ISCURRENTROW 
-- --CDC Meta data-------------------------------------------------------------------------------
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
  ,IFNULL(CHK.LOCATION_ID,-1)                        AS LOCATION_DIM_FK
  ,IFNULL(TO_CHAR(SSS.TAXVALUE:taxRateId),-1)        AS TAXRATEDIM_DIM_FK  
-- --flags--------------------------------------------------------------------------------------
  ,case upper(SSS.SURCHARGEVALUE:isAutoApplied) when 'TRUE' THEN TRUE else FALSE END
                                                     AS IS_AUTOAPPLIED
   ,case upper(SSS.SURCHARGEVALUE:isGratuity) when 'TRUE' THEN TRUE else FALSE END
                                                      AS IS_GRATUITY 
   ,case upper(SSS.SURCHARGEVALUE:isTaxable) when 'TRUE' THEN TRUE else FALSE END                              
                                                      AS IS_TAXABLE
   ,case upper(SSS.TAXVALUE:isTaxIncluded) when 'TRUE' THEN TRUE else FALSE END                              
                                                      AS IS_TAXINCLUDED                                                     
   ,CHK.IS_TRAINING                                   AS IS_TRAINING 
  ,case upper(SSS.SURCHARGEVALUE:printOnReceip) when 'TRUE' THEN TRUE else FALSE END                         
                                                     AS IS_PRINTONRECEIPT   
-- --Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(CHK.CREATED_AT)                   AS CREATED_AT 
  ,TO_CHAR(max(TRY_PARSE_JSON(CHK.info):fiscalDate) over (partition by chk.id))::DATE      
                                                     AS FISCAL_DATE 
  ,to_timestamp_tz(CHK.OPENED_AT)                    AS OPENED_AT 
  ,to_timestamp_tz(CHK.UPDATED_AT)                   AS UPDATED_AT

-- --names, options, etc-------------------------------------------------------------------------
   ,IFNULL(TO_VARCHAR(SSS.SURCHARGEVALUE:status),'None')      AS STATUS
-- --name---------------------------------------------------------------------------------------
  ,CHK.NUMBER                                                 AS CHEQUENUMBER      
  ,IFNULL(REPLACE(SSS.SURCHARGEVALUE:type,'"',''),'None')     AS SURCHARGE_TYPE
-- --Counts and Amounts--------------------------------------------------------------------------
  ,IFNULL(SSS.TAXVALUE:taxRateName,'None')                    AS TAXRATENAME  
  ,IFNULL(SSS.TAXVALUE:percent,0)::NUMBER(38,2)               AS PERCENT
  ,IFNULL(SSS.TAXVALUE:appliedValue,0)::NUMBER(38,2)          AS APPLIEDAMOUNT 
  ,IFNULL(SSS.TAXVALUE:tax,0)::NUMBER(38,2)                   AS TAX
  ,IFNULL(SSS.TAXVALUE:basis,0)::NUMBER(38,2)                 AS TAXBASIS 
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_CHEQUE                                        CHK
  LEFT JOIN (  --LOJ needed to get the correct current row and start dates.  Necessary for cases where the current check row has no surcharges, but a previous row did have surcharges
    SELECT CHK_1.ID AS ID
      ,CHK_1.MTLN_CDC_SEQUENCE_NUMBER
      ,CHK_1.MTLN_CDC_SRC_VERSION
      ,CHK_1.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      -- ,TRY_PARSE_JSON(TAX.VALUE)
      ,TAX.VALUE AS TAXVALUE
      ,TAX.INDEX AS TAXKEY
      ,SUR.VALUE AS SURCHARGEVALUE
      FROM 
        DATALANDING.POSAPI_PUBLIC_CHEQUE              CHK_1 
            ,LATERAL FLATTEN(INPUT => 
            TRY_PARSE_JSON('{surcharges:' || TRY_PARSE_JSON(chk_1.info):surcharges || '}'), PATH => 'surcharges')
                                                      SUR
           ,LATERAL FLATTEN(INPUT => 
            TRY_PARSE_JSON('{taxes:' || TRY_PARSE_JSON(sur.value):taxes || '}'), PATH => 'taxes')
                                                      TAX                                                                      
            WHERE NOT COALESCE(CHK_1.TRUNCATED,FALSE)   
              AND TAX.VALUE IS NOT NULL

                                                      )SSS
       ON CHK.ID = SSS.ID
         AND SSS.TAXVALUE IS NOT NULL
         AND CHK.MTLN_CDC_SEQUENCE_NUMBER = SSS.MTLN_CDC_SEQUENCE_NUMBER
         AND CHK.MTLN_CDC_SRC_VERSION = SSS.MTLN_CDC_SRC_VERSION
         AND CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP = SSS.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    WHERE SSS.TAXVALUE IS NOT NULL
      AND CHK.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
      AND NOT COALESCE(TRUNCATED,FALSE)
      and FEETAX_FACT_NK in ('227408.c93993a0-88f8-4760-9d0c-409c1bda3339.1'
,'227408.c93993a0-88f8-4760-9d0c-409c1bda3339.0')
order by  chk.mtln_cdc_sequence_number
         -- and chk.mtln_cdc_sequence_number = 31912528448
-- ORDER BY    COALESCE(TO_VARCHAR(CHK.ID) || '.' || TO_CHAR(replace(SSS.VALUE:id,'"',''))
--   ,TO_VARCHAR(CHK.ID) || '.' || MAX(TO_CHAR(replace(SSS.VALUE:id,'"',''))) OVER (PARTITION BY CHK.ID)
--   ,TO_CHAR(CHK.ID)  || '.'||'0')        
;