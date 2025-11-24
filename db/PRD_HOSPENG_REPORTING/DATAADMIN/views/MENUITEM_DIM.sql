create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.MENUITEM_DIM(
	MENUITEM_DIM_PK,
	MENUITEM_DIM_NK,
	SKU,
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
	ORGANIZATION_DIM_FK,
	MENUITEMNAME_DIM_FK,
	VARIANT_DIM_FK,
	CREATED_AT,
	UPDATED_AT,
	UPC,
	VARIANT_OPTION_ID,
	CALORIES,
	COST,
	PRICE
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  mei.ID                                                      AS MENUITEM_DIM_PK
--natural keys------------------------------------------------------------------------------
  ,mei.ID                                                     AS MENUITEM_DIM_NK
--name---------------------------------------------------------------------------------------
  ,mei.SKU                                                    AS SKU
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY mei.ID ORDER BY mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,mei.MTLN_CDC_SEQUENCE_NUMBER,mei.MTLN_CDC_SRC_VERSION
      ,mei.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY mei.ID ORDER BY mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,mei.MTLN_CDC_SEQUENCE_NUMBER,mei.MTLN_CDC_SRC_VERSION,mei.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY mei.ID ORDER BY 
    mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP,mei.MTLN_CDC_SEQUENCE_NUMBER,mei.MTLN_CDC_SRC_VERSION
   ,mei.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN mei.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY mei.ID ORDER BY  
    mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,mei.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,mei.MTLN_CDC_SRC_VERSION DESC
   ,mei.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED 
--CDC Meta data-------------------------------------------------------------------------------
  ,mei.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,mei.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,mei.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,mei.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,mei.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,mei.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,mei.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,mei.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,mei.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,mei.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,mei.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,mei.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(mei.organization_id,-1)                 AS ORGANIZATION_DIM_FK
  ,IFNULL(mei.menu_item_name_id,-1)               AS MENUITEMNAME_DIM_FK
  -- ,IFNULL(mei.reporting_category_id,-1)        AS REPORTCATEGORY_DIM_FK
  ,IFNULL(mei.variant_option_id,-1)               AS VARIANT_DIM_FK
--flags---------------------------------------------------------------------------------------
--NO FLAGS
--Dates--------------.------------------------------------------------------------------------
  ,to_timestamp_tz(mei.CREATED_AT)      AS CREATED_AT
  ,to_timestamp_tz(mei.UPDATED_AT)      AS UPDATED_AT
--names, options, etc-------------------------------------------------------------------------
  ,mei.UPC                              AS UPC
  -- ,mei.reporting_category_id         AS REPORT_CATEGORY_ID
  ,mei.variant_option_id                as VARIANT_OPTION_ID
--Counts and Amounts--------------------------------------------------------------------------
  ,mei.CALORIES                         AS CALORIES
  ,mei.COST                             AS COST
  ,mei.PRICE                            AS PRICE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_MENU_ITEM     mei
;
