create or replace view REFUNDSCHEQUEBYITEM_FACT(
	REFUNDSCHEQUEBYITEM_FACT_PK,
	REFUNDSCHEQUEBYITEM_FACT_NK,
	PAYMENTNUMBER,
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
	CCTRANSACTION_FACT_FK,
	CHEQUE_FACT_FK,
	DAYPART_DIM_FK,
	EMPLOYEE_DIM_FK_AS_CREATOR,
	EMPLOYEE_DIM_FK_AS_PAYEE,
	LOCATION_DIM_FK,
	PAYMENTMETHOD_DIM_FK,
	REVENUECENTER_DIM_FK,
	TRANSACTION_FACT_FK,
	TERMINAL_DIM_FK,
	ITEM_FACT_FK,
	REFUNDS_FACT_FK,
	MENUITEM_DIM_FK,
	MENUITEMNAME_DIM_FK,
	IS_TRAINING,
	OPENED_AT,
	PAID_AT,
	REFUNDED_AT,
	FISCALDATE,
	BATCHNUMBER,
	REFUNDED_BY,
	CARDBRAND,
	CARDHOLDERNAME,
	CHEQUENUMBER,
	CURRENCY_ID,
	FLOORPLAN_ID,
	LASTFOURCCNUMBER,
	NEXTFOURCCNUMBER,
	PAYMENTTYPE,
	PAYMENTSTATUS,
	REVENUECENTERNAME,
	TABLENAME,
	ITEM_ID,
	ITEMSTATUS,
	CHECK_TOTAL_AMOUNT,
	REFUND_AMOUNT,
	ITEM_QUANTITY,
	TOTAL_ITEMS_ON_CHECK,
	ITEM_ROW_NUMBER,
	SINGLE_ITEM_BASE_ALLOCATION,
	CHECK_REMAINDER,
	ALLOCATED_ITEM_REFUND
) as
SELECT
-----primary keys------------------------------------------------------------------------------
     REFUNDS_FACT_PK || '.' || ITM.ITEM_FACT_PK                                      AS REFUNDSCHEQUEBYITEM_FACT_PK
-----natural keys------------------------------------------------------------------------------
    ,REFUNDS_FACT_NK || '.' || ITM.ITEM_FACT_NK                                      AS REFUNDSCHEQUEBYITEM_FACT_NK
-----name---------------------------------------------------------------------------------------
    ,REF.PAYMENTNUMBER                                                               AS PAYMENTNUMBER
----data warehouse rows------------------------------------------------------------------------
    ,REF.DW_STARTDATE                                                                AS DW_STARTDATE
    ,REF.DW_ENDDATE                                                                  AS DW_ENDDATE
    ,REF.DW_ISDELETED                                                                AS DW_ISDELETED
    ,REF.DW_ISCURRENTROW                                                             AS DW_ISCURRENTROW
----CDC Meta data-------------------------------------------------------------------------------
    ,REF.MTLN_CDC_LAST_CHANGE_TYPE                                                   AS MTLN_CDC_LAST_CHANGE_TYPE
    ,REF.MTLN_CDC_LAST_COMMIT_TIMESTAMP                                              AS MTLN_CDC_LAST_COMMIT_TIMESTAMP
    ,REF.MTLN_CDC_SEQUENCE_NUMBER                                                    AS MTLN_CDC_SEQUENCE_NUMBER
    ,REF.MTLN_CDC_LOAD_BATCH_ID                                                      AS MTLN_CDC_LOAD_BATCH_ID
    ,REF.MTLN_CDC_LOAD_TIMESTAMP                                                     AS MTLN_CDC_LOAD_TIMESTAMP
    ,REF.MTLN_CDC_PROCESSED_DATE_HOUR                                                AS MTLN_CDC_PROCESSED_DATE_HOUR
    ,REF.MTLN_CDC_SRC_VERSION                                                        AS MTLN_CDC_SRC_VERSION
    ,REF.MTLN_CDC_FILENAME                                                           AS MTLN_CDC_FILENAME
    ,REF.MTLN_CDC_FILEPATH                                                           AS MTLN_CDC_FILEPATH
    ,REF.MTLN_CDC_SRC_DATABASE                                                       AS MTLN_CDC_SRC_DATABASE
    ,REF.MTLN_CDC_SRC_SCHEMA                                                         AS MTLN_CDC_SRC_SCHEMA
    ,REF.MTLN_CDC_SRC_TABLE                                                          AS MTLN_CDC_SRC_TABLE
----foreign keys-------------------------------------------------------------------------------
    ,REF.CCTRANSACTION_FACT_FK                                                       AS CCTRANSACTION_FACT_FK
    ,REF.CHEQUE_FACT_FK                                                              AS CHEQUE_FACT_FK
    ,REF.DAYPART_DIM_FK                                                              AS DAYPART_DIM_FK
    ,REF.EMPLOYEE_DIM_FK_AS_CREATOR                                                  AS EMPLOYEE_DIM_FK_AS_CREATOR
    ,REF.EMPLOYEE_DIM_FK_AS_PAYEE                                                    AS EMPLOYEE_DIM_FK_AS_PAYEE
    ,REF.LOCATION_DIM_FK                                                             AS LOCATION_DIM_FK
    ,REF.PAYMENTMETHOD_DIM_FK                                                        AS PAYMENTMETHOD_DIM_FK
    ,REF.REVENUECENTER_DIM_FK                                                        AS REVENUECENTER_DIM_FK
    ,REF.TRANSACTION_FACT_FK                                                         AS TRANSACTION_FACT_FK
    ,REF.TERMINAL_DIM_FK                                                             AS TERMINAL_DIM_FK
    ,ITM.ITEM_FACT_NK                                                                AS ITEM_FACT_FK
    ,REF.REFUNDS_FACT_NK                                                             AS REFUNDS_FACT_FK
    ,ITM.MENUITEM_DIM_FK                                                             AS MENUITEM_DIM_FK
    ,ITM.MENUITEMNAME_DIM_FK                                                         AS MENUITEMNAME_DIM_FK
----flags---------------------------------------------------------------------------------------
    ,REF.IS_TRAINING                                                                 AS IS_TRAINING
----Dates--------------.------------------------------------------------------------------------
    ,REF.OPENED_AT                                                                   AS OPENED_AT
    ,REF.PAID_AT                                                                     AS PAID_AT
    ,REF.REFUNDED_AT                                                                 AS REFUNDED_AT
    ,REF.FISCALDATE                                                                  AS FISCALDATE
-----names, options, etc----------------------------------------------------------- -------------
    ,REF.BATCHNUMBER                                                                 AS BATCHNUMBER
    ,REF.REFUNDED_BY                                                                 AS REFUNDED_BY
    ,REF.CARDBRAND                                                                   AS CARDBRAND
    ,REF.CARDHOLDERNAME                                                              AS CARDHOLDERNAME
    ,REF.CHEQUENUMBER                                                                AS CHEQUENUMBER
    ,REF.CURRENCY_ID                                                                 AS CURRENCY_ID
    ,REF.FLOORPLAN_ID                                                                AS FLOORPLAN_ID
    ,REF.LASTFOURCCNUMBER                                                            AS LASTFOURCCNUMBER
    ,REF.NEXTFOURCCNUMBER                                                            AS NEXTFOURCCNUMBER
    ,REF.PAYMENTTYPE                                                                 AS PAYMENTTYPE
    ,REF.PAYMENTSTATUS                                                               AS PAYMENTSTATUS
    ,REF.REVENUECENTERNAME                                                           AS REVENUECENTERNAME
    ,REF.TABLENAME                                                                   AS TABLENAME
    ,ITM.ITEM_ID                                                                     AS ITEM_ID
    ,ITM.ITEMSTATUS                                                                  AS ITEMSTATUS
-----Counts and Amounts--------------------------------------------------------------------------
    ,REF.CHECK_TOTAL_AMOUNT                                                          AS CHECK_TOTAL_AMOUNT
    ,REF.REFUND_AMOUNT                                                               AS REFUND_AMOUNT
    -- main logic starts
    ,ITM.QUANTITY                                                                    AS ITEM_QUANTITY
    ,SUM(ITM.QUANTITY) OVER (
        PARTITION BY REF.CHEQUE_FACT_FK, REF.REFUNDS_FACT_PK, REF.MTLN_CDC_SEQUENCE_NUMBER
     )                                                                                      AS TOTAL_ITEMS_ON_CHECK -- Calculate the total quantity of all items on the check for this specific REFUND record.
    ,ROW_NUMBER() OVER (
        PARTITION BY REF.CHEQUE_FACT_FK, REF.REFUNDS_FACT_PK, REF.MTLN_CDC_SEQUENCE_NUMBER
        ORDER BY ITM.ITEM_ID
     )                                                                                      AS ITEM_ROW_NUMBER -- Assign a row number to each item line within the check. This is used to assign the rounding remainder.
    ,ROUND(REF.REFUND_AMOUNT / TOTAL_ITEMS_ON_CHECK, 2)                                     AS SINGLE_ITEM_BASE_ALLOCATION  -- Calculate the base allocation for a single item (quantity of 1).
    ,REF.REFUND_AMOUNT - (SINGLE_ITEM_BASE_ALLOCATION * TOTAL_ITEMS_ON_CHECK)               AS CHECK_REMAINDER -- Calculate the total remainder for the entire check to handle rounding issues.
    ,(SINGLE_ITEM_BASE_ALLOCATION * ITM.QUANTITY) +
        CASE
            WHEN ITEM_ROW_NUMBER = 1 THEN CHECK_REMAINDER
            ELSE 0
        END                                                                                 AS ALLOCATED_ITEM_REFUND -- Calculate the final allocated REFUND for this specific item, adding the remainder to the first ITM.
FROM DATAADMIN.REFUNDS_FACT REF
    JOIN DATAADMIN.ITEM_FACT ITM
        ON REF.CHEQUE_FACT_FK = ITM.CHEQUE_FACT_FK
        AND REF.MTLN_CDC_SEQUENCE_NUMBER = ITM.MTLN_CDC_SEQUENCE_NUMBER
        AND REF.MTLN_CDC_SRC_VERSION = ITM.MTLN_CDC_SRC_VERSION
        AND REF.MTLN_CDC_FILENAME = ITM.MTLN_CDC_FILENAME
    WHERE
        ITM.ITEMSTATUS IN ('Added','Sent')

--===========================================================================================
--End of View
--============================================================================================
/*
;
-- USAGE AND VALIDATIONS
-- The query below should output a difference of 0 for most location-dates.
-- The few deltas that appear are because business rules have changed with time, but the sum of the deltas shoun't be higher than 200 dollars total.
DROP TABLE if exists REPORT_DATA_REFUND;
CALL PRD_HOSPENG_REPORTING.DATAADMIN.SP_REPORT_REFUNDS('2020-07-01T14:48:37.661Z','2029-07-26T14:48:37.661Z','[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]');
CREATE TEMP TABLE REPORT_DATA_REFUND AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


WITH RefundsByItemFactAgg AS (
    SELECT
        LOCATION_DIM_FK,
        FISCALDATE,
        SUM(ALLOCATED_ITEM_REFUND) AS TOTAL_ALLOCATED_ITEM_REFUND
    FROM
        DATAADMIN.REFUNDSCHEQUEBYITEM_FACT_DEV_V2
    WHERE
      DW_ISCURRENTROW
      AND NOT DW_ISDELETED
      -- AND LOCATION_DIM_FK = 26
    GROUP BY
        LOCATION_DIM_FK,
        FISCALDATE
),
ReportDataAgg AS (
    -- This new CTE aggregates the report data to the same level as RefundsByItemFactAgg
    SELECT
        "Location ID",
        "Fiscal Date",
        SUM("Refund Amount") AS total_report_refund_amount
    FROM
        REPORT_DATA_REFUND
    GROUP BY
        "Location ID",
        "Fiscal Date"
)
SELECT
    v.location_dim_fk,
    v.fiscaldate,
    v.total_allocated_item_refund,
    rdv.total_report_refund_amount,
    -- Calculate the difference using the new aggregated amount
    (v.total_allocated_item_refund - rdv.total_report_refund_amount) AS difference
FROM
    RefundsByItemFactAgg AS v
INNER JOIN
    -- Join to the newly created aggregated CTE
    ReportDataAgg AS rdv
ON
    v.location_dim_fk = rdv."Location ID" AND v.fiscaldate = rdv."Fiscal Date"
WHERE difference <> 0
ORDER BY
    abs(difference) DESC
;

*/;