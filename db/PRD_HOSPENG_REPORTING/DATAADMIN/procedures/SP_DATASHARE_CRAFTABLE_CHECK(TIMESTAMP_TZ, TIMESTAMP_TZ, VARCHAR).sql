CREATE OR REPLACE PROCEDURE "SP_DATASHARE_CRAFTABLE_CHECK"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
  reportSet resultset;
  -- startdate timestamp_tz      := ''2020-08-20T14:48:37.661Z'';
  -- enddate timestamp_tz        := ''2029-08-20T14:48:37.661Z'';
  -- locationid string           := ''[35]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today char(11)              := CURRENT_DATE()::date::VARCHAR(10);
-----------------------------------------------------------------------------------------------------------------------
  BEGIN
    --drop temp tables
      DROP TABLE if exists CHK_DATA_TEMP;
      DROP TABLE if exists ITEM_DATA_TEMP;
-----------------------------------------------------------------------------------------------------------------------
  CREATE OR REPLACE  TEMPORARY TABLE CHK_DATA_TEMP
    AS
SELECT
    ORG.ORGANIZATION                                                                              AS "Organization Name",
    ORG.ORGANIZATION_DIM_NK                                                                       AS "Organization ID",
    LOC.LOCATIONNAME                                                                              AS "Location Name",
    CHK.LOCATION_DIM_FK                                                                           AS "Location ID",
    CHK.FISCAL_DATE::DATE                                                                         AS "Business Day",
    CHK.CHEQUENUMBER                                                                              AS "Check Number",
    CHK.CHEQUE_FACT_NK                                                                            AS "Check ID",
    CHK.REVENUECENTERNAME                                                                         AS "Rev Center Name",
    CHK.REVENUECENTERID                                                                           AS "Rev Center ID",
    DPD.DAYPART                                                                                   AS "Meal Period/Day Part Name",
    CHK.DAYPART_DIM_FK                                                                            AS "Meal Period/Day Part ID",
    TO_CHAR(CONVERT_TIMEZONE(''UTC'', LOC.TZ_NAME, chk.OPENED_AT::timestamp_ntz)::timestamp)::DATE  AS "Ticket Open",
    TO_CHAR(CONVERT_TIMEZONE(''UTC'', LOC.TZ_NAME, chk.CLOSED_AT::timestamp_ntz)::timestamp)::DATE  AS "Ticket Closed",
    ORD.ORDER_TYPE                                                                                AS "Order Type Name",
    CHK.ORDERTYPE_DIM_FK                                                                          AS "Order Type ID",
    EMP.EMPLOYEE_NAME                                                                             AS "Server Name",
    CHK.EMPLOYEE_DIM_FK                                                                           AS "Server ID",
    CHK.TOTAL::DECIMAL(36, 2)                                                                     AS "Check Total",
    CHK.TIP::DECIMAL(36, 2)                                                                       AS "Check Total Tips",
    CHK.GRATUITIES::DECIMAL(36, 2)                                                                AS "Gratuity Total",
    --columns for debug
    -- CHK.DISCOUNT::DECIMAL(36, 2)                                                               AS "Discount Total",
    -- CHK.TAX::DECIMAL(36, 2)                                                                       AS "Tax Total",
    -- CHK.GROSS as "Gross" --this is not a req column but at the moment it is used for validations.
    -- ,CHK.MTLN_CDC_SEQUENCE_NUMBER   as "MTLN_CDC_SEQUENCE_NUMBER"

FROM DATAWAREHOUSE.CHEQUE_FACT                                                 CHK
  INNER JOIN DATAWAREHOUSE.LOCATION_DIM                                        LOC
    ON CHK.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
      AND LOC.DW_ISCURRENTROW
      AND CHK.DW_ISCURRENTROW
      AND NOT CHK.IS_TRAINING
      AND NOT CHK.DW_ISDELETED
      AND CHK.STATUS IN (''Closed'')
      AND (CHK.FISCAL_DATE::date >= :startdate::date
                  AND CHK.FISCAL_DATE::date  <= :enddate::date)
      AND CHK.LOCATION_DIM_FK IN (--351,352
          SELECT table1.value
            FROM table(split_to_table(:locationidS, '',''))  table1)
      -- AND NOT (CHK.STATUS = ''Opened'' AND CHK.FISCAL_DATE::date <= :today::date )
      -- AND CHK.FISCAL_DATE::date >= :lastYearEnd::date
      --         AND CHK.FISCAL_DATE::date <= :today::date
      --         AND CHK.LOCATION_DIM_FK in (
      --           SELECT table1.value
      --              FROM table(split_to_table(:locationidS, '',''))  table1)
  INNER JOIN DATAWAREHOUSE.DAYPART_DIM                                         DPD
    ON CHK.DAYPART_DIM_FK = DPD.DAYPART_DIM_NK
      AND DPD.DW_ISCURRENTROW
 LEFT JOIN DATAWAREHOUSE.ORDERTYPE_DIM                                        ORD
    ON CHK.ORDERTYPE_DIM_FK = ORD.ORDERTYPE_DIM_NK
      AND ORD.DW_ISCURRENTROW
LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                                        EMP
ON CHK.EMPLOYEE_DIM_FK = EMP.EMPLOYEE_DIM_NK
    AND EMP.DW_ISCURRENTROW
LEFT JOIN DATAWAREHOUSE.ORGANIZATION_DIM                                        ORG
ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
    AND ORG.DW_ISCURRENTROW
;
-----------------------------------------------------------------------------------------------------------------------
    --return values from the sproc with validated columns only
     reportSet := (
         SELECT * FROM CHK_DATA_TEMP
     );
--=====================================================================================================================
RETURN TABLE(reportSet);
END';