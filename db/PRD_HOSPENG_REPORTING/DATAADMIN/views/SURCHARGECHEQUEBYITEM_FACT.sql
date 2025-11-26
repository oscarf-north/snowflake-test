create or replace view SURCHARGECHEQUEBYITEM_FACT(
	SURCHARGECHEQUEBYITEM_FACT_PK,
	SURCHARGECHEQUEBYITEM_FACT_NK,
	SURCHARGENAME,
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
	SURCHARGE_DIM_NK,
	ITEM_FACT_FK,
	IS_AUTOAPPLIED,
	IS_GRATUITY,
	IS_TAXABLE,
	IS_TRAINING,
	IS_PRINTONRECEIPT,
	CREATED_AT,
	FISCAL_DATE,
	OPENED_AT,
	UPDATED_AT,
	STATUS,
	CHEQUENUMBER,
	SURCHARGE_TYPE,
	ITEM_ID,
	MENUITEM_DIM_FK,
	MENUITEMNAME_DIM_FK,
	SURCHARGE_QUANTITY,
	SURCHARGE_AMOUNT,
	SURCHARGE_APPLIEDAMOUNT,
	ITEM_QUANTITY,
	TOTAL_ITEMS_ON_CHECK,
	ITEM_ROW_NUMBER,
	AMOUNT_SINGLE_ITEM_BASE_ALLOCATION,
	AMOUNT_CHECK_REMAINDER,
	AMOUNT_ALLOCATED_ITEM_SURCHARGE,
	APPLIED_SINGLE_ITEM_BASE_ALLOCATION,
	APPLIED_CHECK_REMAINDER,
	APPLIED_ALLOCATED_ITEM_SURCHARGE
) as 
SELECT
--keys---------------------------------------------------------------------------------------
    S.SURCHARGE_FACT_PK || '.' || ITM.ITEM_FACT_PK                    AS SURCHARGECHEQUEBYITEM_FACT_PK
   ,S.SURCHARGE_FACT_NK || '.' || ITM.ITEM_FACT_NK                    AS SURCHARGECHEQUEBYITEM_FACT_NK

--name---------------------------------------------------------------------------------------
   ,S.SURCHARGENAME                                                   AS SURCHARGENAME

--data warehouse rows------------------------------------------------------------------------
   ,S.DW_STARTDATE                                                    AS DW_STARTDATE
   ,S.DW_ENDDATE                                                      AS DW_ENDDATE
   ,S.DW_ISDELETED                                                    AS DW_ISDELETED
   ,S.DW_ISCURRENTROW                                                 AS DW_ISCURRENTROW

--CDC Meta data-------------------------------------------------------------------------------
   ,S.MTLN_CDC_LAST_CHANGE_TYPE                                       AS MTLN_CDC_LAST_CHANGE_TYPE
   ,S.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                  AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
   ,S.MTLN_CDC_SEQUENCE_NUMBER                                        AS MTLN_CDC_SEQUENCE_NUMBER
   ,S.MTLN_CDC_LOAD_BATCH_ID                                          AS MTLN_CDC_LOAD_BATCH_ID
   ,S.MTLN_CDC_LOAD_TIMESTAMP                                         AS MTLN_CDC_LOAD_TIMESTAMP
   ,S.MTLN_CDC_PROCESSED_DATE_HOUR                                    AS MTLN_CDC_PROCESSED_DATE_HOUR
   ,S.MTLN_CDC_SRC_VERSION                                            AS MTLN_CDC_SRC_VERSION
   ,S.MTLN_CDC_FILENAME                                               AS MTLN_CDC_FILENAME
   ,S.MTLN_CDC_FILEPATH                                               AS MTLN_CDC_FILEPATH
   ,S.MTLN_CDC_SRC_DATABASE                                           AS MTLN_CDC_SRC_DATABASE
   ,S.MTLN_CDC_SRC_SCHEMA                                             AS MTLN_CDC_SRC_SCHEMA
   ,S.MTLN_CDC_SRC_TABLE                                              AS MTLN_CDC_SRC_TABLE

--foreign keys-------------------------------------------------------------------------------
   ,S.CHEQUE_FACT_FK                                                  AS CHEQUE_FACT_FK
   ,S.DAYPART_DIM_FK                                                  AS DAYPART_DIM_FK
   ,S.EMPLOYEE_DIM_FK                                                 AS EMPLOYEE_DIM_FK
   ,S.LOCATION_DIM_FK                                                 AS LOCATION_DIM_FK
   ,S.SURCHARGE_DIM_NK                                                AS SURCHARGE_DIM_NK
   ,ITM.ITEM_FACT_NK                                                  AS ITEM_FACT_FK

--flags--------------------------------------------------------------------------------------
   ,S.IS_AUTOAPPLIED                                                  AS IS_AUTOAPPLIED
   ,S.IS_GRATUITY                                                     AS IS_GRATUITY
   ,S.IS_TAXABLE                                                      AS IS_TAXABLE
   ,S.IS_TRAINING                                                     AS IS_TRAINING
   ,S.IS_PRINTONRECEIPT                                               AS IS_PRINTONRECEIPT

--Dates--------------.------------------------------------------------------------------------
   ,S.CREATED_AT                                                      AS CREATED_AT
   ,S.FISCAL_DATE                                                     AS FISCAL_DATE
   ,S.OPENED_AT                                                       AS OPENED_AT
   ,S.UPDATED_AT                                                      AS UPDATED_AT

--names, options, etc-------------------------------------------------------------------------
   ,S.STATUS                                                          AS STATUS

--name---------------------------------------------------------------------------------------
   ,S.CHEQUENUMBER                                                    AS CHEQUENUMBER
   ,S.SURCHARGE_TYPE                                                  AS SURCHARGE_TYPE
   ,ITM.ITEM_ID                                                       AS ITEM_ID
   ,ITM.MENUITEM_DIM_FK                                               AS MENUITEM_DIM_FK
   ,ITM.MENUITEMNAME_DIM_FK                                           AS MENUITEMNAME_DIM_FK

--Counts and Amounts--------------------------------------------------------------------------
   ,S.QUANTITY                                                                             AS SURCHARGE_QUANTITY
   ,S.AMOUNT                                                                               AS SURCHARGE_AMOUNT
   ,S.APPLIEDAMOUNT                                                                        AS SURCHARGE_APPLIEDAMOUNT
    --common aux columns
    ,ITM.QUANTITY                                                                          AS ITEM_QUANTITY 
    ,SUM(ITM.QUANTITY) OVER (
        PARTITION BY S.CHEQUE_FACT_FK 
            , S.SURCHARGENAME
            , S.MTLN_CDC_SEQUENCE_NUMBER
            , S.MTLN_CDC_SRC_VERSION, S.MTLN_CDC_FILENAME
    )                                                                                      AS TOTAL_ITEMS_ON_CHECK
    ,ROW_NUMBER() OVER (
        PARTITION BY S.CHEQUE_FACT_FK
            , S.SURCHARGENAME
            , S.MTLN_CDC_SEQUENCE_NUMBER
            , S.MTLN_CDC_SRC_VERSION
            , S.MTLN_CDC_FILENAME
        ORDER BY ITM.ITEM_ID
     )                                                                                      AS ITEM_ROW_NUMBER
    --amount
    ,ROUND(S.AMOUNT / TOTAL_ITEMS_ON_CHECK, 2)                                              AS AMOUNT_SINGLE_ITEM_BASE_ALLOCATION
    ,S.AMOUNT - (AMOUNT_SINGLE_ITEM_BASE_ALLOCATION * TOTAL_ITEMS_ON_CHECK)                 AS AMOUNT_CHECK_REMAINDER 
    ,(AMOUNT_SINGLE_ITEM_BASE_ALLOCATION * ITM.QUANTITY) +
        CASE
            WHEN ITEM_ROW_NUMBER = 1 THEN AMOUNT_CHECK_REMAINDER
            ELSE 0
        END                                                                                 AS AMOUNT_ALLOCATED_ITEM_SURCHARGE

    --applied amount
    ,ROUND(S.APPLIEDAMOUNT / TOTAL_ITEMS_ON_CHECK, 2)                                       AS APPLIED_SINGLE_ITEM_BASE_ALLOCATION
    ,S.APPLIEDAMOUNT - (APPLIED_SINGLE_ITEM_BASE_ALLOCATION * TOTAL_ITEMS_ON_CHECK)         AS APPLIED_CHECK_REMAINDER
    ,(APPLIED_SINGLE_ITEM_BASE_ALLOCATION * ITM.QUANTITY) +
        CASE
            WHEN ITEM_ROW_NUMBER = 1 THEN APPLIED_CHECK_REMAINDER
            ELSE 0
        END                                                                                 AS APPLIED_ALLOCATED_ITEM_SURCHARGE

FROM DATAADMIN.SURCHARGE_FACT                                                               S
INNER JOIN DATAADMIN.ITEM_FACT                                                              ITM
ON S.CHEQUE_FACT_FK = ITM.CHEQUE_FACT_FK
    AND S.MTLN_CDC_SEQUENCE_NUMBER = ITM.MTLN_CDC_SEQUENCE_NUMBER
    AND S.MTLN_CDC_SRC_VERSION = ITM.MTLN_CDC_SRC_VERSION
    AND S.MTLN_CDC_FILENAME = ITM.MTLN_CDC_FILENAME
    AND ITM.ITEMSTATUS IN ('Added', 'Sent')
    --filters for debugging
    -- AND S.STATUS IN ('Enabled')
    -- AND itm.DW_ISCURRENTROW
    -- AND itm.cheque_fact_fk = 53366
    -- AND itm.item_fact_nk = '412426.daaa4f96-19e8-4a1c-8410-0dd12181d062'
    -- AND S.DW_ISCURRENTROW

--===========================================================================================
--End of View
--============================================================================================

/*
-- USAGE AND VALIDATIONS
;
--sum amounts of surcharge_fact that have any item
--
WITH aux as (
select MTLN_CDC_SEQUENCE_NUMBER, CHEQUE_FACT_FK, MTLN_CDC_SRC_VERSION, MTLN_CDC_FILENAME from DATAADMIN.ITEM_FACT 
group by MTLN_CDC_SEQUENCE_NUMBER, CHEQUE_FACT_FK, MTLN_CDC_SRC_VERSION, MTLN_CDC_FILENAME
)
SELECT
    SUM(S.amount) AS fact_total_amount,
    SUM(S.appliedamount) AS fact_total_appliedamount
  FROM
    DATAADMIN.SURCHARGE_FACT S
  left join aux itf 
    ON S.CHEQUE_FACT_FK = itf.CHEQUE_FACT_FK 
    AND S.MTLN_CDC_SEQUENCE_NUMBER = itf.MTLN_CDC_SEQUENCE_NUMBER
    AND S.MTLN_CDC_SRC_VERSION = itf.MTLN_CDC_SRC_VERSION
    AND S.MTLN_CDC_FILENAME = itf.MTLN_CDC_FILENAME
  WHERE
     not itf.CHEQUE_FACT_FK is null
;

--then sum the amounts on the view: they should match the amounts from last query
SELECT
    SUM(AMOUNT_ALLOCATED_ITEM_SURCHARGE) AS item_total_amount,
    SUM(APPLIED_ALLOCATED_ITEM_SURCHARGE) AS item_total_appliedamount
  FROM
    DATAADMIN.SURCHARGECHEQUEBYITEM_FACT_DEBUG
;

-- identify surcharges with different item count
SELECT
  s.CHEQUE_FACT_FK,
  s.MTLN_CDC_SEQUENCE_NUMBER,
  s.MTLN_CDC_SRC_VERSION,
  s.MTLN_CDC_FILENAME,
  s.TOTAL_ITEMS_ON_CHECK AS VIEW_TOTAL_ITEMS,
  i.ACTUAL_TOTAL_ITEMS_ON_CHECK AS FACT_TOTAL_ITEMS,
  (s.TOTAL_ITEMS_ON_CHECK - i.ACTUAL_TOTAL_ITEMS_ON_CHECK) as DIFFERENCE
FROM
  (SELECT DISTINCT
      CHEQUE_FACT_FK,
      MTLN_CDC_SEQUENCE_NUMBER,
      MTLN_CDC_SRC_VERSION,
      MTLN_CDC_FILENAME,
      TOTAL_ITEMS_ON_CHECK
    FROM
      DATAADMIN.SURCHARGECHEQUEBYITEM_FACT_DEBUG
    WHERE 1=1
    --filters for debugging
    -- AND DW_ISCURRENTROW
    -- AND CHEQUE_FACT_FK = 53366 
    -- AND MTLN_CDC_SEQUENCE_NUMBER = 53525472632
  ) s
  -- LEFT JOIN (
  INNER JOIN (
    SELECT
      CHEQUE_FACT_FK,
      MTLN_CDC_SEQUENCE_NUMBER,
      MTLN_CDC_SRC_VERSION,
      MTLN_CDC_FILENAME,
      SUM(QUANTITY) AS ACTUAL_TOTAL_ITEMS_ON_CHECK
    FROM
      PRD_HOSPENG_REPORTING.DATAADMIN.ITEM_FACT
    WHERE ITEMSTATUS in ('Added', 'Sent') 
    --filters for debugging
    -- AND CHEQUE_FACT_FK = 53366 AND MTLN_CDC_SEQUENCE_NUMBER = 53525472632 AND DW_ISCURRENTROW
    GROUP BY
      CHEQUE_FACT_FK,
      MTLN_CDC_SEQUENCE_NUMBER,
      MTLN_CDC_SRC_VERSION,
      MTLN_CDC_FILENAME
  ) i 
  ON s.CHEQUE_FACT_FK = i.CHEQUE_FACT_FK
  AND s.MTLN_CDC_SEQUENCE_NUMBER = i.MTLN_CDC_SEQUENCE_NUMBER
  AND s.MTLN_CDC_SRC_VERSION = i.MTLN_CDC_SRC_VERSION
  AND s.MTLN_CDC_FILENAME = i.MTLN_CDC_FILENAME
WHERE
  VIEW_TOTAL_ITEMS != FACT_TOTAL_ITEMS
  -- AND s.CHEQUE_FACT_FK IN (<check>)
ORDER BY
  DIFFERENCE DESC;
;
*/
;