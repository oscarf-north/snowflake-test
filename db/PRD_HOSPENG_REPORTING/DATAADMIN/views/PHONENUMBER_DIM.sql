create or replace view PHONENUMBER_DIM(
	PHONENUMBER_DIM_PK,
	PHONENUMBER_DIM_NK,
	PHONE,
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
	CREATED_AT,
	UPDATED_AT,
	KIND,
	REGION,
	EXTENSION,
	COUNTRY_CODE,
	FUZZY_VECTOR
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  PHN.ID                                                     AS PHONENUMBER_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,PHN.ID                                                    AS PHONENUMBER_DIM_NK
--name---------------------------------------------------------------------------------------
  ,PHN.PHONE                                                 AS PHONE
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY PHN.ID ORDER BY PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,PHN.MTLN_CDC_SEQUENCE_NUMBER,PHN.MTLN_CDC_SRC_VERSION
      ,PHN.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY PHN.ID ORDER BY PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,PHN.MTLN_CDC_SEQUENCE_NUMBER,PHN.MTLN_CDC_SRC_VERSION,PHN.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY PHN.ID ORDER BY 
    PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP,PHN.MTLN_CDC_SEQUENCE_NUMBER,PHN.MTLN_CDC_SRC_VERSION
   ,PHN.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN PHN.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY PHN.ID ORDER BY  
    PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,PHN.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,PHN.MTLN_CDC_SRC_VERSION DESC
   ,PHN.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,PHN.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,PHN.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,PHN.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,PHN.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,PHN.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,PHN.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,PHN.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,PHN.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,PHN.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,PHN.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,PHN.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,PHN.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
--NONE
--flags---------------------------------------------------------------------------------------
--NONE
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(PHN.CREATED_AT)     AS CREATED_AT   
  ,to_timestamp_tz(PHN.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,PHN.KIND                            AS KIND
  ,PHN.REGION                          AS REGION
  ,PHN.EXTENSION                       AS EXTENSION
  ,PHN.COUNTRY_CODE                    AS COUNTRY_CODE
  ,PHN.FUZZY_VECTOR                    AS FUZZY_VECTOR
--Counts and Amounts--------------------------------------------------------------------------
--NONE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_PHONE_NUMBER     PHN
;