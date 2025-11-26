create or replace view TAXSETTINGS_DIM(
	TAXSETTINGS_DIM_PK,
	TAXSETTINGS_DIM_NK,
	TAXSETTINGS_ID,
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
	LOCATIONGROUP_DIM_FK,
	TRACK_TAXES_ON_COMPS,
	CREATED_AT,
	UPDATED_AT,
	TAX_TRACKING,
	RECEIPT_OPTIONS,
	ROUNDING_METHOD,
	COMBINED_RECEIPT_NAME
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  tax.ID                                                     AS TAXSETTINGS_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,tax.ID                                                    AS TAXSETTINGS_DIM_NK
--name---------------------------------------------------------------------------------------
   ,tax.ID                                                   AS TAXSETTINGS_ID
--data warehouse rows--------------------------------------------------s----------------------
  ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY tax.ID ORDER BY tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,tax.MTLN_CDC_SEQUENCE_NUMBER,tax.MTLN_CDC_SRC_VERSION
      ,tax.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY tax.ID ORDER BY tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,tax.MTLN_CDC_SEQUENCE_NUMBER,tax.MTLN_CDC_SRC_VERSION,tax.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY tax.ID ORDER BY 
    tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP,tax.MTLN_CDC_SEQUENCE_NUMBER,tax.MTLN_CDC_SRC_VERSION
   ,tax.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN tax.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY tax.ID ORDER BY  
    tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,tax.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,tax.MTLN_CDC_SRC_VERSION DESC
   ,tax.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED  
--CDC Meta data-------------------------------------------------------------------------------
  ,tax.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,tax.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,tax.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,tax.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,tax.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,tax.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,tax.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,tax.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,tax.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,tax.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,tax.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,tax.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
 ,IFNULL(tax.location_id,-1)           AS LOCATION_DIM_FK 
 ,IFNULL(tax.location_group_id,-1)     AS LOCATIONGROUP_DIM_FK 
--flags---------------------------------------------------------------------------------------
  ,tax.track_taxes_on_comps            AS TRACK_TAXES_ON_COMPS
--Dates--------------.------------------------------------------------------------------------
  ,TO_TIMESTAMP_TZ(tax.created_at)     AS CREATED_AT
  ,TO_TIMESTAMP_TZ(tax.updated_at)     AS UPDATED_AT 
--names, options, etc-------------------------------------------------------------------------
  ,tax.tax_tracking                    AS TAX_TRACKING
  ,tax.receipt_options                 AS RECEIPT_OPTIONS
  ,tax.rounding_method                 AS ROUNDING_METHOD
  ,tax.combined_receipt_name           AS COMBINED_RECEIPT_NAME
--Counts and Amounts--------------------------------------------------------------------------
--none
----------------------------------------------------------------------------------------------

FROM DATALANDING.POSAPI_PUBLIC_TAX_SETTINGS     tax
;