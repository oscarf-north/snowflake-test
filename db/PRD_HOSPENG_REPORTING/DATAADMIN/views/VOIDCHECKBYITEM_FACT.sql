create or replace view VOIDCHECKBYITEM_FACT(
	VOIDCHECKBYITEM_FACT_PK,
	VOIDCHECKBYITEM_FACT_NK,
	CHEQUENUMBER,
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
	ITEM_FACT_FK,
	LOCATION_DIM_FK,
	MENUITEM_DIM_FK,
	MENUITEMNAME_DIM_FK,
	ORDERTYPE_DIM_FK,
	ORGANIZATION_DIM_FK,
	REVENUECENTER_DIM_FK,
	SHIFT_DIM_FK,
	TAXSETTINGS_DIM_FK,
	VARIANT_DIM_FK,
	VOIDREASON_DIM_FK,
	HAS_DISCOUNT,
	IS_TRAINING,
	IS_VOID,
	HAS_TRACKTAXESONCOMP,
	FISCAL_DATE_INT,
	FISCAL_DATE,
	OPENED_AT,
	BEGIN_PREP_AT,
	CLOSED_AT,
	SCHEDULED_AT,
	CREATED_AT,
	UPDATED_AT,
	UUID,
	AUDIT,
	ROUNDINGMETHOD,
	COMBINEDRECEIPTNAME,
	RECEIPTOPTION,
	TAXTRACKING,
	REVENUECENTERNAME,
	REVENUECENTERID,
	STATUS_REASON_ID,
	CHECK_ID,
	TABLE_NAME,
	STATUS,
	ITEM_ID,
	ITEMSTATUS,
	CHECKGROSS,
	CHECKTOTAL,
	PRICE,
	BASEPRICE,
	QUANTITY,
	GROSS,
	VOID_LEVEL,
	TOTAL_NON_VOIDED_QUANTITY_ON_CHECK,
	NON_VOIDED_ITEM_ROW_NUMBER,
	ALLOCATED_ITEM_VOID
) as

--=================================================================================================================
SELECT
     ITM.ITEM_FACT_PK                                                                 as VOIDCHECKBYITEM_FACT_PK
    ,ITM.ITEM_FACT_NK                                                                 as VOIDCHECKBYITEM_FACT_NK
    --name--------------------------------------------------------------------------------------------------------
    ,CHK.CHEQUENUMBER                                                                 as CHEQUENUMBER
    --data warehouse rows-----------------------------------------------------------------------------------------            
	,CHK.DW_STARTDATE                                                                 as DW_STARTDATE
	,CHK.DW_ENDDATE                                                                   as DW_ENDDATE
	,CHK.DW_ISDELETED                                                                 as DW_ISDELETED
	,CHK.DW_ISCURRENTROW                                                              as DW_ISCURRENTROW
    --CDC Meta data-----------------------------------------------------------------------------------------------
	,CHK.MTLN_CDC_LAST_CHANGE_TYPE                                                    as MTLN_CDC_LAST_CHANGE_TYPE
	,CHK.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                               as MTLN_CDC_LAST_COMMIT_TIMESTAMP
	,CHK.MTLN_CDC_SEQUENCE_NUMBER                                                     as MTLN_CDC_SEQUENCE_NUMBER
	,CHK.MTLN_CDC_LOAD_BATCH_ID                                                       as MTLN_CDC_LOAD_BATCH_ID
	,CHK.MTLN_CDC_LOAD_TIMESTAMP                                                      as MTLN_CDC_LOAD_TIMESTAMP
	,CHK.MTLN_CDC_PROCESSED_DATE_HOUR                                                 as MTLN_CDC_PROCESSED_DATE_HOUR
	,CHK.MTLN_CDC_SRC_VERSION                                                         as MTLN_CDC_SRC_VERSION
	,CHK.MTLN_CDC_FILENAME                                                            as MTLN_CDC_FILENAME
	,CHK.MTLN_CDC_FILEPATH                                                            as MTLN_CDC_FILEPATH
	,CHK.MTLN_CDC_SRC_DATABASE                                                        as MTLN_CDC_SRC_DATABASE
	,CHK.MTLN_CDC_SRC_SCHEMA                                                          as MTLN_CDC_SRC_SCHEMA
	,CHK.MTLN_CDC_SRC_TABLE                                                           as MTLN_CDC_SRC_TABLE
    --foreign keys------------------------------------------------------------------------------------------------
    ,ITM.CHEQUE_FACT_FK                                                               as CHEQUE_FACT_FK
    ,CHK.DAYPART_DIM_FK                                                               as DAYPART_DIM_FK
	,CHK.EMPLOYEE_DIM_FK                                                              as EMPLOYEE_DIM_FK
    ,ITM.ITEM_FACT_NK                                                                 as ITEM_FACT_FK    
	,CHK.LOCATION_DIM_FK                                                              as LOCATION_DIM_FK
	,ITM.MENUITEM_DIM_FK                                                              as MENUITEM_DIM_FK
	,ITM.MENUITEMNAME_DIM_FK                                                          as MENUITEMNAME_DIM_FK    
	,CHK.ORDERTYPE_DIM_FK                                                             as ORDERTYPE_DIM_FK
	,CHK.ORGANIZATION_DIM_FK                                                          as ORGANIZATION_DIM_FK
	,CHK.REVENUECENTER_DIM_FK                                                         as REVENUECENTER_DIM_FK   
	,CHK.SHIFT_DIM_FK                                                                 as SHIFT_DIM_FK
	,CHK.TAXSETTINGS_DIM_FK                                                           as TAXSETTINGS_DIM_FK
	,ITM.VARIANT_DIM_FK                                                               as VARIANT_DIM_FK
	,CHK.VOIDREASON_DIM_FK                                                            as VOIDREASON_DIM_FK    
    --flags------------------------------------------------------------------------------------------------------
    ,CHK.HAS_DISCOUNT                                                                 as HAS_DISCOUNT
	,CHK.IS_TRAINING                                                                  as IS_TRAINING
	,CHK.IS_VOID                                                                      as IS_VOID
	,CHK.HAS_TRACKTAXESONCOMP                                                         as HAS_TRACKTAXESONCOMP
    --Dates--------------.---------------------------------------------------------------------------------------
	,CHK.FISCAL_DATE_INT                                                              as FISCAL_DATE_INT
	,CHK.FISCAL_DATE                                                                  as FISCAL_DATE
	,CHK.OPENED_AT                                                                    as OPENED_AT
	,CHK.BEGIN_PREP_AT                                                                as BEGIN_PREP_AT
	,CHK.CLOSED_AT                                                                    as CLOSED_AT
	,CHK.SCHEDULED_AT                                                                 as SCHEDULED_AT
	,CHK.CREATED_AT                                                                   as CREATED_AT
	,CHK.UPDATED_AT                                                                   as UPDATED_AT
    --names, options, etc---------------------------------------------------------------------------------------
	,CHK.UUID                                                                         as UUID
	,CHK.AUDIT                                                                        as AUDIT
	,CHK.ROUNDINGMETHOD                                                               as ROUNDINGMETHOD
	,CHK.COMBINEDRECEIPTNAME                                                          as COMBINEDRECEIPTNAME
	,CHK.RECEIPTOPTION                                                                as RECEIPTOPTION
	,CHK.TAXTRACKING                                                                  as TAXTRACKING
	,CHK.REVENUECENTERNAME                                                            as REVENUECENTERNAME
	,CHK.REVENUECENTERID                                                              as REVENUECENTERID
	,CHK.STATUS_REASON_ID                                                             as STATUS_REASON_ID
	,CHK.CHECK_ID                                                                     as CHECK_ID
	,CHK.TABLE_NAME                                                                   as TABLE_NAME
    ,CHK.STATUS                                                                       as STATUS
    ,ITM.ITEM_ID                                                                      as ITEM_ID
    ,ITM.ITEMSTATUS                                                                   as ITEMSTATUS
    --Counts and Amounts-------------------------------------------------------------------------------------
    --columns I used when developing the view
    ,CHK.GROSS                                                                        as CHECKGROSS
    ,CHK.TOTAL                                                                        as CHECKTOTAL
    ,ITM.PRICE                                                                        as PRICE
    ,ITM.BASEPRICE                                                                    as BASEPRICE
    ,ITM.QUANTITY                                                                     as QUANTITY
    ,ITM.GROSS                                                                        as GROSS
    ,CASE
        WHEN ITM.ITEMSTATUS = 'Voided' THEN 'item'
        WHEN CHK.STATUS = 'Voided' THEN 'cheque'
        ELSE 'ERROR'
    END                                                                              as VOID_LEVEL
    -- Calculate the total quantity of non-voided items on the cheque for distribution
    ,SUM(CASE WHEN ITM.ITEMSTATUS <> 'Voided' THEN ITM.QUANTITY ELSE 0 END) OVER (
        PARTITION BY CHK.CHEQUE_FACT_NK, CHK.MTLN_CDC_SEQUENCE_NUMBER
    )                                                                                as TOTAL_NON_VOIDED_QUANTITY_ON_CHECK
    -- Assign a row number to non-voided items to handle rounding remainders
    ,ROW_NUMBER() OVER (
        PARTITION BY CHK.CHEQUE_FACT_NK, CHK.MTLN_CDC_SEQUENCE_NUMBER
        ORDER BY (CASE WHEN ITM.ITEMSTATUS <> 'Voided' THEN ITM.ITEM_FACT_NK ELSE NULL END) ASC NULLS LAST
    )                                                                                as NON_VOIDED_ITEM_ROW_NUMBER
    -- Calculate the final allocated void amount for each item
    ,CASE
        -- If an item is voided, use its PRICE value to match sp_report_void
        WHEN ITM.ITEMSTATUS = 'Voided' THEN 
            ((CASE WHEN ITM.PRICE > 0.0000 THEN ITM.PRICE ELSE ITM.BASEPRICE END) + IFNULL(MOD.TotalModifierPrice, 0))
        -- If the cheque is voided, distribute the cheque's TOTAL across the non-voided items.
        WHEN CHK.STATUS = 'Voided' THEN
            -- Calculate the base allocation for this item based on its quantity, using CHK.GROSS.
            (ROUND(CHK.GROSS / NULLIF(TOTAL_NON_VOIDED_QUANTITY_ON_CHECK, 0), 2) * ITM.QUANTITY)
            +
            -- Add the rounding remainder to the first non-voided item.
            CASE
                WHEN NON_VOIDED_ITEM_ROW_NUMBER = 1 THEN
                    CHK.GROSS - (ROUND(CHK.GROSS / NULLIF(TOTAL_NON_VOIDED_QUANTITY_ON_CHECK, 0), 2) * TOTAL_NON_VOIDED_QUANTITY_ON_CHECK)
                ELSE 0
            END
        -- This case should not be reached due to the WHERE clause, but as a fallback, the void is 0.
        ELSE 0
    END                                                                              as ALLOCATED_ITEM_VOID
    
FROM DATAADMIN.CHEQUE_FACT                                       CHK
  LEFT JOIN DATAADMIN.ITEM_FACT                                  ITM
    ON CHK.CHEQUE_FACT_NK = ITM.CHEQUE_FACT_FK
    AND CHK.MTLN_CDC_SEQUENCE_NUMBER = ITM.MTLN_CDC_SEQUENCE_NUMBER
  LEFT JOIN (
            SELECT
                ITEM_FACT_FK,
                SUM(IFNULL(PRICE, 0)) AS TotalModifierPrice
            FROM DATAADMIN.ITEMMODIFIER_DIM
            WHERE DW_ISCURRENTROW
            GROUP BY ITEM_FACT_FK
    )
                                                                 MOD -- Join the new CTE
    ON ITM.ITEM_FACT_NK = MOD.ITEM_FACT_FK
    
WHERE 
    (
        (CHK.STATUS = 'Voided' AND ITM.ITEMSTATUS IN ('Added','Sent') )
        OR
        (ITM.ITEMSTATUS = 'Voided')
    )
    -- uncomment when developing
    -- AND CHK.DW_ISCURRENTROW
    -- AND ITM.DW_ISCURRENTROW
    -- AND NOT CHK.DW_ISDELETED
    -- AND NOT ITM.DW_ISDELETED
    -- AND CHK.location_dim_fk = 26
    -- AND ITM.location_dim_fk = 26
    -- AND CHK.FISCAL_DATE = '2025-05-09'
    -- AND CHK.CHEQUE_FACT_NK in (175772, 197923)


--===========================================================================================
--End of View
--============================================================================================

/*
;
--DATA VALIDATION QUERIES

CALL DATAADMIN.SP_REPORT_VOID_0001('2020-07-01T14:48:37.661Z','2029-07-26T14:48:37.661Z','[26,27]');
CREATE OR REPLACE TEMP TABLE REPORT_DATA_VOID AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- compare vs void report at the cheque level
WITH
  fact_data AS (
    -- First query with LEVEL normalization
    SELECT
      LOCATION_DIM_FK,
      CHEQUE_FACT_FK,
      SUM(ALLOCATED_ITEM_VOID * (case when VOID_LEVEL = 'item' then QUANTITY else 1 END) )  AS total_allocated_void
    -- SUM(ALLOCATED_ITEM_VOID) AS total_allocated_void
    FROM
       dataadmin.VOIDCHECKBYITEM_FACT
    WHERE
      dw_iscurrentrow
      AND LOCATION_DIM_FK IN (26, 27)
    --   and CHEQUE_FACT_FK in (220356)
    GROUP BY
      LOCATION_DIM_FK,
      CHEQUE_FACT_FK
  ),
  report_data AS (
    -- Second query with LEVEL normalization
    SELECT
      "Location ID",
      "Check ID",
      SUM("Amount") AS total_report_amount
    FROM
      REPORT_DATA_VOID
    -- WHERE "Check ID" in (220356)
    GROUP BY
      "Location ID",
      "Check ID"
  )
SELECT
  COALESCE(fd.LOCATION_DIM_FK, rd."Location ID") AS location,
  fd.CHEQUE_FACT_FK,
  rd."Check ID",
  COALESCE(fd.total_allocated_void, 0) AS allocated_void,
  COALESCE(rd.total_report_amount, 0) AS report_amount,
  COALESCE(fd.total_allocated_void, 0) - COALESCE(rd.total_report_amount, 0) AS difference
FROM
  fact_data fd
--   FULL OUTER JOIN report_data rd 
  INNER JOIN report_data rd --it returns null with inner join, meaning that all matching cheques have the same void value which makes me think there are missing cheques.
  ON fd.LOCATION_DIM_FK = rd."Location ID"
  AND fd.CHEQUE_FACT_FK = rd."Check ID"
where
  abs(difference) > 0.01
order by
 abs(difference) desc
 
-- LOCATION	CHEQUE_FACT_FK	Check ID	ALLOCATED_VOID	REPORT_AMOUNT	DIFFERENCE
-- 26	243922	243922	59.9	89.85	-29.95
-- 26	207917	207917	14.95	29.9	-14.95
-- 26	336065	336065	14.95	29.9	-14.95
-- 26	369101	369101	4.95	9.9	-4.95

;


--AD HOC granualr queries
select * from datawarehouse.VOIDCHECKBYITEM where dw_iscurrentrow and cheque_fact_fk in (207917)
;

SELECT CHEQUE_FACT_NK, fiscal_date,STATUS, DW_ISCURRENTROW, gross, net, total FROM DATAWAREHOUSE.CHEQUE_FACT WHERE CHEQUE_FACT_NK IN (207917 ) AND DW_ISCURRENTROW;


SELECT CHEQUE_FACT_FK, item_fact_nk,  fiscal_date,ITEMSTATUS, is_void , price, quantity, gross, net, total , DW_ISCURRENTROW 
FROM DATAWAREHOUSE.ITEM_FACT 
WHERE 
CHEQUE_FACT_FK IN (207917 ) 
--  item_fact_nk in ('243922.b8526d5c-d26c-422b-83e2-d50ea4fab464')

AND DW_ISCURRENTROW
;

SELECT ITEM_FACT_FK, PRICE FROM DATAWAREHOUSE.ITEMMODIFIER_DIM 
WHERE ITEM_FACT_FK IN ('243922.cb54829f-87f6-4a5d-855b-dba26dcca2f0'
                       , '243922.b8526d5c-d26c-422b-83e2-d50ea4fab464') 
AND DW_ISCURRENTROW;




select PRICE, item_fact_fk from DATAWAREHOUSE.ITEMMODIFIER_DIM 
where item_fact_fk in ('220356.ef517b20-5cbc-4b46-9ee5-d498dfa80ca2','220356.42bcee5e-2bc7-459f-97db-17b79a3e6b71','220356.3a493c1e-f328-4ad7-aa95-653cf2ae81d0')
AND dw_iscurrentrow

here are the missing 30.85!!
-- PRICE	ITEM_FACT_FK
-- 0	220356.42bcee5e-2bc7-459f-97db-17b79a3e6b71
-- 0	220356.ef517b20-5cbc-4b46-9ee5-d498dfa80ca2
-- 0	220356.3a493c1e-f328-4ad7-aa95-653cf2ae81d0
-- 4.95	220356.3a493c1e-f328-4ad7-aa95-653cf2ae81d0
-- 20.95	220356.42bcee5e-2bc7-459f-97db-17b79a3e6b71
-- 4.95	220356.ef517b20-5cbc-4b46-9ee5-d498dfa80ca2
;

*/
;