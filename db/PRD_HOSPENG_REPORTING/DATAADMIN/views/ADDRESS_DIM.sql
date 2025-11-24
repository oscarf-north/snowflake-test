create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.ADDRESS_DIM(
	ADDRESS_DIM_PK,
	ADDRESS_DIM_NK,
	ADDRESS,
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
	GEO,
	ZIP,
	ZIP4,
	COUNTRY,
	INTERNAL,
	LOCATION,
	STREETNAME,
	STREETTYPEABBREV,
	PREDIRABBREV,
	POSTDIRABBREV,
	ADDRESS_ALPHANUMERIC
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  ADD.ID                                                     AS ADDRESS_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,ADD.ID                                                    AS ADDRESS_DIM_NK
--name---------------------------------------------------------------------------------------
  ,ADD.STREETNAME                                            AS ADDRESS
--data warehouse rows------------------------------------------------------------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY ADD.ID ORDER BY ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,ADD.MTLN_CDC_SEQUENCE_NUMBER,ADD.MTLN_CDC_SRC_VERSION
      ,ADD.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY ADD.ID ORDER BY ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,ADD.MTLN_CDC_SEQUENCE_NUMBER,ADD.MTLN_CDC_SRC_VERSION,ADD.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY ADD.ID ORDER BY 
    ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP,ADD.MTLN_CDC_SEQUENCE_NUMBER,ADD.MTLN_CDC_SRC_VERSION
   ,ADD.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN ADD.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY ADD.ID ORDER BY  
    ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,ADD.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,ADD.MTLN_CDC_SRC_VERSION DESC
   ,ADD.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,ADD.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,ADD.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,ADD.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,ADD.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,ADD.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,ADD.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,ADD.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,ADD.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,ADD.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,ADD.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,ADD.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,ADD.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
 --none
--flags---------------------------------------------------------------------------------------
--none
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(ADD.CREATED_AT)     AS CREATED_AT   
  ,to_timestamp_tz(ADD.UPDATED_AT)     AS UPDATED_AT  
--names, options, etc-------------------------------------------------------------------------
  ,to_varchar(ADD.GEO)                 AS GEO
  ,ADD.ZIP                             AS ZIP
  ,ADD.ZIP4                            AS ZIP4
  ,ADD.COUNTRY                         AS COUNTRY
  ,ADD.INTERNAL                        AS INTERNAL
  ,ADD.LOCATION                        AS LOCATION
  ,ADD.STREETNAME                      AS STREETNAME
  ,ADD.STREETTYPEABBREV                AS STREETTYPEABBREV
  ,ADD.PREDIRABBREV                    AS PREDIRABBREV
  ,ADD.POSTDIRABBREV                   AS POSTDIRABBREV
  ,ADD.ADDRESS_ALPHANUMERIC            AS ADDRESS_ALPHANUMERIC
--Counts and Amounts--------------------------------------------------------------------------
--none 
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_ADDRESS     ADD
;
