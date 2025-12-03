CREATE OR REPLACE PROCEDURE "SP_REPORT_TAX_DEBUG"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  reportSet resultset;
  -- startdate timestamp_tz := ''2024-07-01T14:48:37.661Z'';
  -- enddate timestamp_tz   := ''2029-07-31T14:48:37.661Z'';
  -- locationid string      := ''[351,352,353,361,379,382,385,387,388,389,390,399,408,421,433,480,574,680]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--========================================================================================================
BEGIN
  DROP TABLE IF EXISTS temp_itemtax;
  DROP TABLE IF EXISTS temp_feetax;

  CREATE TEMP TABLE temp_itemtax AS
  SELECT
      tax.TAX_FACT_NK                                       as "Support ID"
      ,tax.MTLN_CDC_SEQUENCE_NUMBER                         as "MTLN_CDC_SEQUENCE_NUMBER"
      , ''TAX-'' ||row_number() over (order by tax.TAX_FACT_NK)
                                                            as "Detail ID"
      ,tax.CHEQUE_FACT_FK                                   as "CHEQUE_FACT_FK"
      ,tax.ITEM_FACT_FK                                     as "ITEM_FACT_FK"
  --Status, category, level------------------------------------------------------------------------------------
      ,IFNULL(tax.CHECKSTATUS,''None'')                      as "Check Status"
      ,IFNULL(tax.ITEMSTATUS,''None'')                       as "Item Status"
      ,IFNULL(ccd.COGSCATEGORY,''None'')                     as "Category"
  --Geography----------------------------------------------------------------------------------------------
      ,IFNULL(org.ORGANIZATION,''None'')                     as "Organization"
      ,IFNULL(loc.LOCATIONNAME,''None'')                     as "Location"
      ,loc.LOCATION_DIM_NK                                  as "Location ID"
      ,IFNULL(tax.REVENUECENTERNAME,''None'')                as "Revenue Center"
  --Dates---------------------------------------------------------------------------------------------------
     ,LOC.TZ_NAME                                           as "Time Zone"
      ,to_char(LEFT(tax.FISCAL_DATE,4))                      as "Year"
      ,to_char(YEAR(tax.FISCAL_DATE)) || ''|''
          || TO_CHAR(RIGHT(''0'' || MONTH(tax.FISCAL_DATE),2))
                                                            as "Year and Month"
      ,IFNULL(dad.DAYPART,''None'')                          as "Daypart"
      ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,
         tax.CLOSED_AT::timestamp_ntz )::timestamp)
                                                            as "Closed At"
      ,to_char(tax.FISCAL_DATE)                             as "Fiscal Date"
      ,IFNULL(DAYNAME(tax.FISCAL_DATE),''None'')             as "Day of Week"
      ,CASE WHEN DAYNAME(tax.FISCAL_DATE) IN (''Sat'',''Sun'')
         THEN TRUE ELSE FALSE END                           as "Is Weekend"
  --Flags---------------------------------------------------------------------------------------------------
      ,tax.IS_TAX_INCLUDED::BOOLEAN                         as "Is Tax Included"
  --Descriptors----------------------------------------------------------------------------------------------
      ,IFNULL(tax.CHEQUENUMBER ,''None'')                    as "Check"
      ,IFNULL(med.MENUITEMNAME,'' None'')                    as "Menu Item"
      ,IFNULL(ord.ORDER_TYPE,'' None'')                      as "Order Type"
      ,IFNULL(tax.TAXRATENAME,'' None'')                     as "Rate Name"
  --Facts-----------------------------------------------------------------------------------------------------
      ,1::NUMBER(18,0)                                      as "Count"
      ,CASE WHEN split_part(tax.tax_fact_nk, ''.'',  3)  = 0
        THEN itf.GROSS::NUMBER(18,2)
        ELSE NULL::NUMBER(18,2)
        END
                                                            as "Gross"
      ,CASE WHEN split_part(tax.tax_fact_nk, ''.'',  3)  = 0
        THEN itf.APPLIEDAMOUNT::NUMBER(18,2)
        ELSE NULL::NUMBER(18,2)
        END
                                                            as "Applied Amount"
      ,(CASE WHEN ABS(tax.AMOUNT - (itf.GROSS * (tax.PERCENT/100))) < 0.009
        THEN itf.GROSS
        ELSE itf.APPLIEDAMOUNT END) ::NUMBER(18,2)          as "Tax Basis Amount"
      ,tax.AMOUNT::NUMBER(18,2)                             as "Tax Amount"
      ,tax.AMOUNT::NUMBER(18,2)                             as "Tax Total"
      ,tax.PERCENT::NUMBER(18,2)                            as "Tax Percent"
  ----------------------------------------------------------------------------------------------------------------
  FROM DATAWAREHOUSE.TAX_FACT                               tax
       INNER JOIN DATAWAREHOUSE.ITEM_FACT                   itf
          ON itf.ITEM_FACT_NK = tax.ITEM_FACT_FK
            AND itf.DW_ISCURRENTROW
            AND tax.ITEMSTATUS IN (''Added'',''Sent'')
            AND tax.CHECKSTATUS = ''Closed''
            AND tax.OPENED_AT is not null
            AND tax.DW_ISCURRENTROW
            AND NOT tax.DW_ISDELETED
            AND NOT tax.IS_TRAINING
            AND tax.FISCAL_DATE::date
                >= :startdate::date
            AND tax.FISCAL_DATE::date
                <= :enddate::date
            AND tax.LOCATION_DIM_FK IN (
               SELECT table1.value
                    FROM table(split_to_table(:locationidS, '',''))  table1)
        INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 loc
          ON tax.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
            AND loc.DW_ISCURRENTROW
        INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM             med
         ON med.MENUITEMNAME_DIM_NK = tax.MENUITEMNAME_DIM_FK
           AND med.DW_ISCURRENTROW
        INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                ord
          ON ord.ORDERTYPE_DIM_NK = tax.ORDERTYPE_DIM_FK
            AND ord.DW_ISCURRENTROW
        INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM             org
          ON org.ORGANIZATION_DIM_NK = loc.ORGANIZATION_DIM_FK
            AND org.DW_ISCURRENTROW
        INNER JOIN DATAWAREHOUSE.DAYPART_DIM                  dad
          ON tax.DAYPART_DIM_FK = dad.DAYPART_DIM_NK
            AND dad.DW_ISCURRENTROW
        INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM           meg
          ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
            AND meg.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM               ccd
          ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK
            AND ccd.DW_ISCURRENTROW
  -- This QUALIFY clause removes duplicates by keeping only the latest record for each tax entry
  QUALIFY ROW_NUMBER() OVER(PARTITION BY tax.CHEQUE_FACT_FK,tax.ITEM_FACT_FK ORDER BY tax.MTLN_CDC_SEQUENCE_NUMBER DESC) = 1;

-------------------------------------------------------------------------------------------------------------
SELECT
     FEETAX_FACT_NK                                          as "Support ID"
     ,tax.MTLN_CDC_SEQUENCE_NUMBER                           as "MTLN_CDC_SEQUENCE_NUMBER"
     , ''FEE-'' ||row_number() over (order by FEETAX_FACT_NK)
                                                             as "Detail ID"
    ,tax.CHEQUE_FACT_FK AS "CHEQUE_FACT_FK"
    ,null                                 as "ITEM_FACT_FK"
--Status, category, level------------------------------------------------------------------------------------
    ,IFNULL(chk.STATUS,''None'')                               as "Check Status"
    ,''None''                                                  as "Item Status"
    ,''None''                                                  as "Category"
--Geography----------------------------------------------------------------------------------------------
    ,IFNULL(org.ORGANIZATION,''None'')                         as "Organization"
    ,IFNULL(loc.LOCATIONNAME,''None'')                         as "Location"
    ,loc.LOCATION_DIM_NK                                     as "Location ID"
    ,IFNULL(chk.REVENUECENTERNAME,''None'')                    as "Revenue Center"
--Dates---------------------------------------------------------------------------------------------------
   ,LOC.TZ_NAME                                              as "Time Zone"
    ,to_char(LEFT(tax.FISCAL_DATE,4))                        as "Year"
    ,to_char(YEAR(tax.FISCAL_DATE)) || ''|''
        || TO_CHAR(RIGHT(''0'' || MONTH(tax.FISCAL_DATE),2))
                                                             as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                              as "Daypart"
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,
       chk.CLOSED_AT::timestamp_ntz )::timestamp)
                                                             as "Closed At"
    ,to_char(chk.FISCAL_DATE)                                as "Fiscal Date"
    ,IFNULL(DAYNAME(chk.FISCAL_DATE),''None'')                 as "Day of Week"
    ,CASE WHEN DAYNAME(chk.FISCAL_DATE) IN (''Sat'',''Sun'')
       THEN TRUE ELSE FALSE END                              as "Is Weekend"
--Flags---------------------------------------------------------------------------------------------------
    ,tax.IS_TAXINCLUDED::BOOLEAN                             as "Is Tax Included"
--Descriptors----------------------------------------------------------------------------------------------
    ,IFNULL(tax.CHEQUENUMBER ,''None'')                        as "Check"
    ,''None''                                                  as "Menu Item"
    ,IFNULL(ord.ORDER_TYPE,'' None'')                          as "Order Type"
    ,IFNULL(tax.TAXRATENAME,'' None'')                         as "Rate Name"
--Facts-----------------------------------------------------------------------------------------------------
    ,1::NUMBER(18,0)                                         as "Count"
    ,0::NUMBER(18,0)                                         as "Gross"
    ,tax.APPLIEDAMOUNT::NUMBER(18,2)                         as "Applied Amount"
    ,tax.TAXBASIS ::NUMBER(18,2)                             as "Tax Basis Amount"
    ,tax.TAX::NUMBER(18,2)                                   as "Tax Amount"
    ,tax.TAX::NUMBER(18,2)                                   as "Tax Total"
    ,tax.PERCENT::NUMBER(18,2)                               as "Tax Percent"
----------------------------------------------------------------------------------------------------------------
FROM DATAWAREHOUSE.FEETAX_FACT                               tax
     INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                    chk
        ON chk.CHEQUE_FACT_NK = tax.CHEQUE_FACT_FK
          AND chk.TAX > 0.000
          AND chk.STATUS = ''Closed''
          AND chk.OPENED_AT is not null
          AND chk.DW_ISCURRENTROW
          AND tax.DW_ISCURRENTROW
          AND NOT tax.DW_ISDELETED
          AND NOT tax.IS_TRAINING
          AND tax.FISCAL_DATE::date
              >= :startdate::date
          AND tax.FISCAL_DATE::date
              <= :enddate::date
          AND tax.LOCATION_DIM_FK IN (
             SELECT table1.value
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.LOCATION_DIM                 loc
        ON tax.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                ord
        ON ord.ORDERTYPE_DIM_NK = chk.ORDERTYPE_DIM_FK
          AND ord.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM             org
        ON org.ORGANIZATION_DIM_NK = loc.ORGANIZATION_DIM_FK
          AND org.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.DAYPART_DIM                  dad
        ON tax.DAYPART_DIM_FK = dad.DAYPART_DIM_NK
          AND dad.DW_ISCURRENTROW         
    ;

    CREATE TEMP TABLE temp_feetax AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
-------------------------------------------------------------------------------------------------------------
 reportSet:= (
select * from temp_itemtax
  union
select * from temp_feetax
--==============================================================================================================
);
RETURN TABLE(reportSet);
END;
';