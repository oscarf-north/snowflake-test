create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.MENUITEM_MENUGROUP_XREF(
	MENUITEM_MENUGROUP_XREF_PK,
	MENUITEM_MENUGROUP_XREF_NK,
	MENUITEM_MENUGROUP,
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
	MENUGROUP_DIM_FK,
	MENUITEM_DIM_FK
) as
--============================================================================================
SELECT 
--primary keys------------------------------------------------------------------------------
  MGI.ID                                                      AS MENUITEM_MENUGROUP_XREF_PK
--natural keys------------------------------------------------------------------------------
  ,MGI.ID                                                     AS MENUITEM_MENUGROUP_XREF_NK
--name---------------------------------------------------------------------------------------
  ,MGI.menu_Item_ID || '|' || MGI.menu_group_id               AS MENUITEM_MENUGROUP
--data warehouse rows------------------------------------------------------------------------
 ,TO_TIMESTAMP(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP))),23)
    ||  RIGHT('00000' || TO_CHAR(RANK() OVER (
      PARTITION BY MGI.ID ORDER BY MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP
      ,MGI.MTLN_CDC_SEQUENCE_NUMBER,MGI.MTLN_CDC_SRC_VERSION
      ,MGI.MTLN_CDC_FILENAME)),6))
                                                               AS DW_STARTDATE       --REQUIRED
   ,TIMESTAMPADD(NANOSECOND,-1,TO_TIMESTAMP(
    IFNULL(LEFT(TO_CHAR(TO_TIMESTAMP(TO_CHAR(LEAD(MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP) 
    OVER (PARTITION BY MGI.ID ORDER BY MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,MGI.MTLN_CDC_SEQUENCE_NUMBER,MGI.MTLN_CDC_SRC_VERSION,MGI.MTLN_CDC_FILENAME) ))),23)
    || RIGHT('00000' || TO_CHAR(RANK() OVER (PARTITION BY MGI.ID ORDER BY 
    MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP,MGI.MTLN_CDC_SEQUENCE_NUMBER,MGI.MTLN_CDC_SRC_VERSION
   ,MGI.MTLN_CDC_FILENAME) +1),6),'9999-09-09 09:09:09.000') ))
                                                              AS DW_ENDDATE          --REQUIRED
  
  ,CASE WHEN MGI.MTLN_CDC_LAST_CHANGE_TYPE ='d' 
    THEN TRUE ELSE FALSE END                                  AS DW_ISDELETED        --REQUIRED       
  ,CASE WHEN RANK() OVER(PARTITION BY MGI.ID ORDER BY  
    MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP DESC
   ,MGI.MTLN_CDC_SEQUENCE_NUMBER DESC
   ,MGI.MTLN_CDC_SRC_VERSION DESC
   ,MGI.MTLN_CDC_FILENAME DESC) = 1
        THEN TRUE ELSE FALSE END                              AS DW_ISCURRENTROW     --REQUIRED
--CDC Meta data-------------------------------------------------------------------------------
  ,MGI.MTLN_CDC_LAST_CHANGE_TYPE       AS MTLN_CDC_LAST_CHANGE_TYPE
  ,MGI.MTLN_CDC_LAST_COMMIT_TIMESTAMP  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
  ,MGI.MTLN_CDC_SEQUENCE_NUMBER        AS MTLN_CDC_SEQUENCE_NUMBER
  ,MGI.MTLN_CDC_LOAD_BATCH_ID          AS MTLN_CDC_LOAD_BATCH_ID
  ,MGI.MTLN_CDC_LOAD_TIMESTAMP         AS MTLN_CDC_LOAD_TIMESTAMP
  ,MGI.MTLN_CDC_PROCESSED_DATE_HOUR    AS MTLN_CDC_PROCESSED_DATE_HOUR
  ,MGI.MTLN_CDC_SRC_VERSION            AS MTLN_CDC_SRC_VERSION
  ,MGI.MTLN_CDC_FILENAME               AS MTLN_CDC_FILENAME
  ,MGI.MTLN_CDC_FILEPATH               AS MTLN_CDC_FILEPATH
  ,MGI.MTLN_CDC_SRC_DATABASE           AS MTLN_CDC_SRC_DATABASE
  ,MGI.MTLN_CDC_SRC_SCHEMA             AS MTLN_CDC_SRC_SCHEMA
  ,MGI.MTLN_CDC_SRC_TABLE              AS MTLN_CDC_SRC_TABLE
--foreign keys-------------------------------------------------------------------------------
  ,IFNULL(MGI.MENU_GROUP_ID,-1)        AS MENUGROUP_DIM_FK
  ,IFNULL(MGI.MENU_ITEM_ID,-1)         AS MENUITEM_DIM_FK
--flags---------------------------------------------------------------------------------------
--NONE
--Dates--------------.------------------------------------------------------------------------
--NONE  CREATE DATE AND UPDATE DATE EXCEPTED SINCE THIS IS AN XREF
--names, options, etc-------------------------------------------------------------------------
--NONE                           
--Counts and Amounts--------------------------------------------------------------------------
--NONE
----------------------------------------------------------------------------------------------
FROM DATALANDING.POSAPI_PUBLIC_MENU_GROUP_ITEMS     MGI
;
