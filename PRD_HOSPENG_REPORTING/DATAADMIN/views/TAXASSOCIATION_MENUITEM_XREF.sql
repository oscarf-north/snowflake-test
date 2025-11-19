create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.TAXASSOCIATION_MENUITEM_XREF(
	TAXASSOCIATION_MENUITEM_XREF_PK,
	TAXASSOCIATION_MENUITEM_XREF_NK,
	TAXASSOCIATION_MENUITEM,
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
	TAXASSOCIATION_DIM_FK,
	MENUITEM_DIM_FK
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  tab.ID                                                      AS TAXASSOCIATION_MENUITEM_XREF_PK
--natural keys------------------------------------------------------------------------------
  ,tab.ID                                                     AS TAXASSOCIATION_MENUITEM_XREF_NK
--name---------------------------------------------------------------------------------------
  ,tab.TAX_ASSOCIATION_ID || '|' || tab.MENU_ITEM_ID          AS TAXASSOCIATION_MENUITEM
--data warehouse rows------------------------------------------------------------------------
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
--CDC Meta data-------------------------------------------------------------------------------
  ,tab.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,tab.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,tab.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,tab.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,tab.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,tab.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,tab.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,tab.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,tab.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,tab.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,tab.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,tab.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(tab.TAX_ASSOCIATION_ID,-1)   AS TAXASSOCIATION_DIM_FK
  ,IFNULL(tab.MENU_ITEM_ID,-1)         AS MENUITEM_DIM_FK
--flags---------------------------------------------------------------------------------------
--NONE
--Dates--------------.------------------------------------------------------------------------
--NONE  CREATE DATE AND UPDATE DATE EXCEPTED SINCE THIS IS AN XREF
--names, options, etc-------------------------------------------------------------------------
--NONE                           
--Counts and Amounts--------------------------------------------------------------------------
--NONE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_TAX_ASSOCIATION_MENU_ITEMS     tab
;