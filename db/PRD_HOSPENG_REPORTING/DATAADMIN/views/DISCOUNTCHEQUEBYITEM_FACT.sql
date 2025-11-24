create or replace view PRD_HOSPENG_REPORTING.DATAADMIN.DISCOUNTCHEQUEBYITEM_FACT(
	DISCOUNTCHEQUEBYITEM_FACT_PK,
	DISCOUNTCHEQUEBYITEM_FACT_NK,
	DISCOUNTNAME,
	DW_STARTDATE,
	FISCAL_DATE,
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
	DISCOUNTCHECK_FACT_FK,
	EMPLOYEE_DIM_FK_AS_ADDED_BY,
	EMPLOYEE_DIM_FK_AS_APPROVED_BY,
	EMPLOYEE_DIM_FK,
	ITEM_FACT_FK,
	LOCATION_DIM_FK,
	STANDARDDISCOUNT_DIM_FK,
	DO_AUTOAPPLY,
	IS_TRAINING,
	ADDED_AT,
	CREATED_AT,
	OPENED_AT,
	UPDATED_AT,
	APPLICATION,
	STATUS,
	CHEQUESTATUS,
	DISCOUNTREASON,
	DISCOUNTLEVEL,
	RECEIPTNAME,
	PROMOCODE,
	PROMODESCRIPTION,
	REVENUECENTERNAME,
	ROUNDINGMETHOD,
	CHEQUENUMBER,
	DISCOUNT_ID,
	ITEM_ID,
	PROMOCODE_ID,
	DISCOUNT_TYPE,
	DISCOUNT_PERCENT,
	APPLIED_AMOUNT,
	DISCOUNT_AMOUNT,
	VALUE,
	DISCOUNT,
	DISCOUNTCHECK,
	DISCOUNTITEM,
	GROSS,
	NET,
	ITEM_QUANTITY,
	TOTAL_ITEMS_ON_CHECK,
	ITEM_ROW_NUMBER,
	SINGLE_ITEM_BASE_ALLOCATION,
	CHECK_REMAINDER,
	ALLOCATED_ITEM_DISCOUNT
) as
--============================================================================================
  SELECT
    -- Primary and Natural Keys
    -- Construct a unique key for each item's portion of a cheque discount.
     CD.CHEQUE_FACT_FK || '.' || CD.DISCOUNTNAME || '.' || ITM.ITEM_ID            AS DISCOUNTCHEQUEBYITEM_FACT_PK
     --natural keys------------------------------------------------------------------------------
    ,CD.CHEQUE_FACT_FK || '.' || CD.DISCOUNTNAME || '.' || ITM.ITEM_ID            AS DISCOUNTCHEQUEBYITEM_FACT_NK
    --name---------------------------------------------------------------------------------------
    ,CD.DISCOUNTNAME                                                              AS DISCOUNTNAME --this is discount id
    --data warehouse rows--------------------------------------------------------------------------
    ,CD.DW_STARTDATE                                                              AS DW_STARTDATE
    ,CD.FISCAL_DATE                                                               AS FISCAL_DATE
    ,CD.DW_ENDDATE                                                                AS DW_ENDDATE
    ,CD.DW_ISDELETED                                                              AS DW_ISDELETED
    ,CD.DW_ISCURRENTROW                                                           AS DW_ISCURRENTROW
    --CDC Meta data-------------------------------------------------------------------------------
    ,CD.MTLN_CDC_LAST_CHANGE_TYPE                                                 AS MTLN_CDC_LAST_CHANGE_TYPE
    ,CD.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                            AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,CD.MTLN_CDC_SEQUENCE_NUMBER                                                  AS MTLN_CDC_SEQUENCE_NUMBER
    ,CD.MTLN_CDC_LOAD_BATCH_ID                                                    AS MTLN_CDC_LOAD_BATCH_ID
    ,CD.MTLN_CDC_LOAD_TIMESTAMP                                                   AS MTLN_CDC_LOAD_TIMESTAMP
    ,CD.MTLN_CDC_PROCESSED_DATE_HOUR                                              AS MTLN_CDC_PROCESSED_DATE_HOUR
    ,CD.MTLN_CDC_SRC_VERSION                                                      AS MTLN_CDC_SRC_VERSION
    ,CD.MTLN_CDC_FILENAME                                                         AS MTLN_CDC_FILENAME
    ,CD.MTLN_CDC_FILEPATH                                                         AS MTLN_CDC_FILEPATH
    ,CD.MTLN_CDC_SRC_DATABASE                                                     AS MTLN_CDC_SRC_DATABASE
    ,CD.MTLN_CDC_SRC_SCHEMA                                                       AS MTLN_CDC_SRC_SCHEMA
    ,CD.MTLN_CDC_SRC_TABLE                                                        AS MTLN_CDC_SRC_TABLE
    --foreign keys-------------------------------------------------------------------------------
    ,CD.CHEQUE_FACT_FK                                                            AS CHEQUE_FACT_FK
    ,CD.DAYPART_DIM_FK                                                            AS DAYPART_DIM_FK
    ,CD.DISCOUNTCHECK_FACT_NK                                                     AS DISCOUNTCHECK_FACT_FK
    ,CD.EMPLOYEE_DIM_FK_AS_ADDED_BY                                               AS EMPLOYEE_DIM_FK_AS_ADDED_BY
    ,CD.EMPLOYEE_DIM_FK_AS_APPROVED_BY                                            AS EMPLOYEE_DIM_FK_AS_APPROVED_BY
    ,CD.EMPLOYEE_DIM_FK                                                           AS EMPLOYEE_DIM_FK
    ,ITM.ITEM_FACT_NK                                                             AS ITEM_FACT_FK    
    ,CD.LOCATION_DIM_FK                                                           AS LOCATION_DIM_FK
    ,CD.STANDARDDISCOUNT_DIM_FK                                                   AS STANDARDDISCOUNT_DIM_FK
    --flags---------------------------------------------------------------------------------------
    ,CD.DO_AUTOAPPLY                                                              AS DO_AUTOAPPLY
    ,CD.IS_TRAINING                                                               AS IS_TRAINING
    --Dates--------------.------------------------------------------------------------------------
    ,CD.ADDED_AT                                                                  AS ADDED_AT
    ,CD.CREATED_AT                                                                AS CREATED_AT
    ,CD.OPENED_AT                                                                 AS OPENED_AT
    ,CD.UPDATED_AT                                                                AS UPDATED_AT
    --names, options, etc-------------------------------------------------------------------------
    ,CD.APPLICATION                                                               AS APPLICATION
    ,CD.STATUS                                                                    AS STATUS
    ,CD.CHEQUESTATUS                                                              AS CHEQUESTATUS
    ,CD.DISCOUNTREASON                                                            AS DISCOUNTREASON
    ,CD.DISCOUNTLEVEL                                                             AS DISCOUNTLEVEL
    ,CD.RECEIPTNAME                                                               AS RECEIPTNAME
    ,CD.PROMOCODE                                                                 AS PROMOCODE
    ,CD.PROMODESCRIPTION                                                          AS PROMODESCRIPTION
    ,CD.REVENUECENTERNAME                                                         AS REVENUECENTERNAME
    ,CD.ROUNDINGMETHOD                                                            AS ROUNDINGMETHOD
    --name---------------------------------------------------------------------------------------
    ,CD.CHEQUENUMBER                                                              AS CHEQUENUMBER
    ,CD.DISCOUNTNAME                                                              AS DISCOUNT_ID
    ,ITM.ITEM_ID                                                                  AS ITEM_ID
    ,CD.PROMOCODE_ID                                                              AS PROMOCODE_ID
    ,CD.DISCOUNT_TYPE                                                             AS DISCOUNT_TYPE
    --Counts and Amounts--------------------------------------------------------------------------
    ,CD.DISCOUNT_PERCENT                                                          AS DISCOUNT_PERCENT
    ,CD.APPLIED_AMOUNT                                                            AS APPLIED_AMOUNT
    ,CD.DISCOUNT_AMOUNT                                                           AS DISCOUNT_AMOUNT
    ,CD.VALUE                                                                     AS VALUE
    ,CD.DISCOUNT                                                                  AS DISCOUNT
    ,CD.DISCOUNTCHECK                                                             AS DISCOUNTCHECK
    ,CD.DISCOUNTITEM                                                              AS DISCOUNTITEM
    ,CD.GROSS                                                                     AS GROSS
    ,CD.NET                                                                       AS NET

    -- here starts the operations to calculate the final ALLOCATED_ITEM_DISCOUNT which will be used with STANDARDDISCOUNTNAME
    ,ITM.QUANTITY                                                                           AS ITEM_QUANTITY -- This section performs the distribution of the cheque discount across the items.
    ,SUM(ITM.QUANTITY) OVER (
        PARTITION BY CD.CHEQUE_FACT_FK, CD.DISCOUNTNAME, CD.MTLN_CDC_SEQUENCE_NUMBER
     )                                                                                      AS TOTAL_ITEMS_ON_CHECK -- Calculate the total quantity of all items on the check for this specific discount record.
    ,ROW_NUMBER() OVER (
        PARTITION BY CD.CHEQUE_FACT_FK, CD.DISCOUNTNAME, CD.MTLN_CDC_SEQUENCE_NUMBER
        ORDER BY ITM.ITEM_ID
     )                                                                                      AS ITEM_ROW_NUMBER -- Assign a row number to each item line within the check. This is used to assign the rounding remainder.
    ,ROUND(CD.APPLIED_AMOUNT / TOTAL_ITEMS_ON_CHECK, 2)                                     AS SINGLE_ITEM_BASE_ALLOCATION  -- Calculate the base allocation for a single item (quantity of 1).
    ,CD.APPLIED_AMOUNT - (SINGLE_ITEM_BASE_ALLOCATION * TOTAL_ITEMS_ON_CHECK)               AS CHECK_REMAINDER -- Calculate the total remainder for the entire check to handle rounding issues.
    ,(SINGLE_ITEM_BASE_ALLOCATION * ITM.QUANTITY) +
        CASE
            WHEN ITEM_ROW_NUMBER = 1 THEN CHECK_REMAINDER
            ELSE 0
        END                                                                                 AS ALLOCATED_ITEM_DISCOUNT -- Calculate the final allocated discount for this specific item, adding the remainder to the first item.
  ------------------------------------------------------------------------------------------------------------------------
  FROM DATAADMIN.DISCOUNTCHECK_FACT AS CD
  -- Join the cheque-level discounts with all items on that same cheque using ITEM_FACT.
  -- The join includes MTLN_CDC_SEQUENCE_NUMBER to ensure correctness for records processed in the same batch.
  JOIN DATAADMIN.ITEM_FACT AS ITM
    ON CD.CHEQUE_FACT_FK = ITM.CHEQUE_FACT_FK
   AND CD.MTLN_CDC_SEQUENCE_NUMBER = ITM.MTLN_CDC_SEQUENCE_NUMBER
  WHERE
    ITM.ITEMSTATUS IN ('Added','Sent','IndefiniteHold')
    -- AND CD.CHEQUESTATUS IN ('Closed') 
    -- AND NOT CD.STATUS = 'Disabled'

--===========================================================================================
--End of View
--============================================================================================

/*
-- USAGE AND VALIDATIONS
;
--===========================================================================================
--Compare vs discount report
--============================================================================================
DROP TABLE if exists REPORT_DATA_DIS;
CALL DATAADMIN.SP_REPORT_DISCOUNT('2000-06-01T14:48:37.661Z','2029-06-26T14:48:37.661Z','[26,27]');
CREATE TEMP TABLE REPORT_DATA_DIS AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

WITH DiscountsByItemFactAgg AS (
    SELECT
    CHEQUE_FACT_FK,
        LOCATION_DIM_FK,
        FISCAL_DATE,
        SUM(ALLOCATED_ITEM_DISCOUNT) AS TOTAL_ALLOCATED_ITEM_DISCOUNT
    FROM
        DISCOUNTCHEQUEBYITEM_FACT
        -- DISCOUNTCHEQUEBYITEM_FACT_DEV
    WHERE
      DW_ISCURRENTROW
      AND NOT DW_ISDELETED
      AND LOCATION_DIM_FK IN (26,27)
    GROUP BY
    CHEQUE_FACT_FK,
        LOCATION_DIM_FK,
        FISCAL_DATE
),
ReportDataAgg AS (
    -- This new CTE aggregates the report data to the same level as VoidsByItemFactAgg
    SELECT
    SUBSTR("Support ID", 1, POSITION('.', "Support ID") - 1) AS CHEQUE_REPORT,
        "Location ID",
        "Fiscal Date",
        SUM("Discount Amount") AS total_report_discount_amount
    FROM
         DATAADMIN.REPORT_DATA_DIS
    where "Discount Level" = 'Check'
    
    GROUP BY
    CHEQUE_REPORT,
        "Location ID",
        "Fiscal Date"
)
SELECT
    v.location_dim_fk,
    v.fiscal_date,
    v.CHEQUE_FACT_FK ,
    COALESCE(v.TOTAL_ALLOCATED_ITEM_DISCOUNT, 0) AS TOTAL_ALLOCATED_ITEM_DISCOUNT,
    COALESCE(rdv.total_report_discount_amount, 0) AS total_report_discount_amount,
    -- Calculate the difference using the coalesced amounts
    (COALESCE(v.TOTAL_ALLOCATED_ITEM_DISCOUNT, 0) - COALESCE(rdv.total_report_discount_amount, 0)) AS difference
FROM
    DiscountsByItemFactAgg AS v
-- INNER JOIN
FULL OUTER JOIN
    -- Join to the newly created aggregated CTE
    ReportDataAgg AS rdv
ON
    v.location_dim_fk = rdv."Location ID"
    AND v.fiscal_date = rdv."Fiscal Date"
    AND v.CHEQUE_FACT_FK = rdv.CHEQUE_REPORT
-- WHERE v.CHEQUE_FACT_FK = 356344
ORDER BY
    abs(difference) DESC
;



--===========================================================================================
--Other validation queries
--============================================================================================
-- AT THE PARTITION LEVEL validate if the sum of the allocated disounts yields the same value as the sum if the discounts (APPLIED_AMOUT)
SELECT 
-- Validation columns
  SUM(ALLOCATED_ITEM_DISCOUNT) OVER (PARTITION BY CHEQUE_FACT_FK, DISCOUNT_ID, MTLN_CDC_SEQUENCE_NUMBER)     AS TOTAL_ALLOCATED_DISCOUNT
  ,APPLIED_AMOUNT - SUM(ALLOCATED_ITEM_DISCOUNT) OVER (PARTITION BY CHEQUE_FACT_FK, DISCOUNT_ID, MTLN_CDC_SEQUENCE_NUMBER) AS ALLOCATION_DIFFERENCE
from DISCOUNTCHEQUEBYITEM_FACT_DEV_V2
WHERE DW_ISCURRENTROW
AND LOCATION_DIM_FK = 26
qualify ALLOCATION_DIFFERENCE > 0
;



--remove caching and test if this runs fast that means snowflake is able to apply filters as early as possible
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
SELECT * FROM DISCOUNTCHEQUEBYITEM_FACT_DEV_V2
WHERE
DW_ISCURRENTROW
AND LOCATION_DIM_FK = 26
AND CHEQUE_FACT_FK = 183828
AND DISCOUNT_ID = '547e90f5-5683-4de4-b44c-06f5d44c4878'
;


-- Compares total sum of discounts in entire dataset
WITH UniqueDiscounts AS (
    -- This CTE isolates the unique discount amounts to avoid double-counting.
    -- The view is at an item level, so each check-level discount is repeated for every item on the check.
    SELECT
        APPLIED_AMOUNT,
        CHEQUE_FACT_FK
    FROM DISCOUNTCHEQUEBYITEM_FACT_DEV_V2
    WHERE DW_ISCURRENTROW
    AND LOCATION_DIM_FK = 26
    -- We partition by the unique identifiers of a check discount instance.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CHEQUE_FACT_FK, DISCOUNT_ID, MTLN_CDC_SEQUENCE_NUMBER ORDER BY MTLN_CDC_SEQUENCE_NUMBER DESC ) = 1
),
TotalOriginalDiscount AS (
    -- This CTE sums up the unique discount amounts and counts distinct cheques to get the true total values.
    SELECT
        SUM(APPLIED_AMOUNT) AS TOTAL_APPLIED,
        COUNT(DISTINCT CHEQUE_FACT_FK) AS TOTAL_ORIGINAL_CHECKS
    FROM UniqueDiscounts
),
TotalAllocatedDiscount AS (
    -- This CTE sums up the allocated discount amounts across all items and counts the distinct cheques.
    -- This should equal the total original discount if the allocation logic is correct.
    SELECT
        SUM(ALLOCATED_ITEM_DISCOUNT) AS TOTAL_ALLOCATED,
        COUNT(DISTINCT CHEQUE_FACT_FK) AS TOTAL_ALLOCATED_CHECKS
    FROM DISCOUNTCHEQUEBYITEM_FACT_DEV_V2
    WHERE DW_ISCURRENTROW
    AND LOCATION_DIM_FK = 26
)
-- Final comparison
SELECT
    T1.TOTAL_APPLIED,
    T2.TOTAL_ALLOCATED,
    T1.TOTAL_APPLIED - T2.TOTAL_ALLOCATED AS DIFFERENCE,
    T1.TOTAL_ORIGINAL_CHECKS,
    T2.TOTAL_ALLOCATED_CHECKS,
    T1.TOTAL_ORIGINAL_CHECKS - T2.TOTAL_ALLOCATED_CHECKS AS CHECKS_DIFFERENCE
FROM TotalOriginalDiscount T1, TotalAllocatedDiscount T2;

-- TOTAL_APPLIED	TOTAL_ALLOCATED	DIFFERENCE	TOTAL_ORIGINAL_CHECKS	TOTAL_ALLOCATED_CHECKS	CHECKS_DIFFERENCE
-- 35119.32	35119.32	0	597	597	0
;
*/
;