create or replace view BATCH_DIM(
	BATCH_DIM_PK,
	BATCH_DIM_NK,
	BATCH_NUMBER,
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
	LOCATION_DIM_FK,
	PAYMENTMETHOD_DIM_FK,
	IS_TRUNCATED,
	FISCAL_DATE_INT,
	FISCAL_DATE,
	CREATED_AT,
	UPDATED_AT,
	BATCH_NUMBER_SUFFIX,
	STATE,
	TRANSACTION_NUMBER
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  ((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix)) || '.' || TO_CHAR(dad.LOCATION_ID)                                             
                                                              AS BATCH_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix)) || '.' || TO_CHAR(dad.LOCATION_ID)                                            
                                                              AS BATCH_DIM_NK
--name---------------------------------------------------------------------------------------
  ,((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix))                                                    
                                                              AS BATCH_NUMBER
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY 
      ((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix)) || '.' || TO_CHAR(dad.LOCATION_ID) 
      ORDER BY dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,dad.MTLN_CDC_SEQUENCE_NUMBER,dad.MTLN_CDC_SRC_VERSION
      ,dad.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY 
    ((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix)) || '.' || TO_CHAR(dad.LOCATION_ID) 
    ORDER BY dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,dad.MTLN_CDC_SEQUENCE_NUMBER,dad.MTLN_CDC_SRC_VERSION,dad.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY 
    ((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix)) || '.' || TO_CHAR(dad.LOCATION_ID) 
    ORDER BY 
    dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP,dad.MTLN_CDC_SEQUENCE_NUMBER,dad.MTLN_CDC_SRC_VERSION
   ,dad.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN dad.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY 
  ((((dad.fiscal_date_int)::int * 10000) + dad.batch_number_suffix)) || '.' || TO_CHAR(dad.LOCATION_ID) 
  ORDER BY  
    dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,dad.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,dad.MTLN_CDC_SRC_VERSION DESC
   ,dad.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,dad.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,dad.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,dad.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,dad.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,dad.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,dad.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,dad.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,dad.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,dad.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,dad.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,dad.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,dad.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(dad.LOCATION_ID,-1)          AS LOCATION_DIM_FK  
  ,IFNULL(dad.PAYMENT_METHOD_ID,-1)    AS PAYMENTMETHOD_DIM_FK
--flags---------------------------------------------------------------------------------------
  ,dad.TRUNCATED                       AS IS_TRUNCATED
--Dates--------------.------------------------------------------------------------------------
  ,dad.fiscal_date_int                 AS FISCAL_DATE_INT
  ,CASE WHEN FISCAL_DATE_INT > 0 
    THEN '20' || SUBSTRING(TO_CHAR(dad.fiscal_date_int),1,2) || '-' 
      || SUBSTRING(TO_CHAR(dad.fiscal_date_int),3,2)     || '-' 
      || SUBSTRING(TO_CHAR(dad.fiscal_date_int),5,2) 
    ELSE NULL END
                                       AS FISCAL_DATE
  ,to_timestamp_tz(dad.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(dad.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,to_char(dad.batch_number_suffix)    AS BATCH_NUMBER_SUFFIX
  ,dad.STATE                           AS STATE
  ,to_char(dad.TRANSACTION_NUMBER)     AS TRANSACTION_NUMBER  
--Counts and Amounts--------------------------------------------------------------------------
 --none
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_EPX_BATCH     dad
   WHERE dad.MTLN_CDC_LAST_CHANGE_TYPE <> 'd' 
;