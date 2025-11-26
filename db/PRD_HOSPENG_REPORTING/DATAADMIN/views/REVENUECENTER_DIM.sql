create or replace view REVENUECENTER_DIM(
	REVENUECENTER_DIM_PK,
	REVENUECENTER_DIM_NK,
	REVENUECENTER,
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
	RECEIPT_FOOTER
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  tab.ID                                                      AS REVENUECENTER_DIM_PK  --REQUIRED
--natural keys------------------------------------------------------------------------------
  ,tab.ID                                                     AS REVENUECENTER_DIM_NK  --REQUIRED
--name-------------------------------------------------------------------------------------------
  ,tab.NAME                                                   AS REVENUECENTER
--data warehouse REQUIRED rows-------------------------------------------------------------------
,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY tab.ID ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
      ,tab.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY tab.ID ORDER BY tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION,tab.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY tab.ID ORDER BY 
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP,tab.MTLN_CDC_SEQUENCE_NUMBER,tab.MTLN_CDC_SRC_VERSION
   ,tab.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN tab.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY tab.ID ORDER BY  
    tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,tab.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,tab.MTLN_CDC_SRC_VERSION DESC
   ,tab.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data REQUIRED rows-------------------------------------------------------------------------------
  ,tab.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE                   --REQUIRED
  ,tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP              --REQUIRED
  ,tab.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER                    --REQUIRED
  ,tab.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID                      --REQUIRED
  ,tab.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP                     --REQUIRED
  ,tab.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR                --R EQUIRED
  ,tab.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION                        --REQUIRED
  ,tab.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME                           --REQUIRED
  ,tab.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH                           --REQUIRED
  ,tab.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE                       --REQUIRED
  ,tab.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA                         --REQUIRED
  ,tab.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE                          --REQUIRED
--foreign keys-------------------------------------------------------------------------------
  -- ,IFNULL(tab.LOCATION_ID,-1)          AS LOCATION_DIM_FK 
--flags---------------------------------------------------------------------------------------
--ALLCOLUMN NAMES IN THIS SECTION SHOULD begin with is_ or has_ if created by the dw.
--   columns created by the source app should be specifically cast as boolean 
--Dates--------------.------------------------------------------------------------------------
--dates should be cast as dates
-- created_at timestamp with time zone  <--this is the create table from postgres
--,to_timestamp_tz(created_at) 
  ,to_timestamp_tz(tab.CREATED_AT)     AS CREATED_AT  
  ,to_timestamp_tz(tab.UPDATED_AT)     AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,tab.RECEIPT_FOOTER                  AS RECEIPT_FOOTER
--Counts and Amounts--------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_REVENUE_CENTER     tab
;