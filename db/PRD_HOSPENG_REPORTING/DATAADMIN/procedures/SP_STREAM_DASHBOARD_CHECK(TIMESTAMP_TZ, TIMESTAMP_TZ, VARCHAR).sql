CREATE OR REPLACE PROCEDURE "SP_STREAM_DASHBOARD_CHECK"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet      resultset;
  -- startdate      date     := ''2025-05-07'';  
  -- enddate        date     := ''2025-05-07''; 
  -- locationid     string   := ''351'';
  locationidS string      :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today          char(11) := CURRENT_DATE()::DATE::VARCHAR(10);
  yesterdaydate  date     := dateadd(day,-1,:today);
  weekOffset     int      := (  
                              SELECT TOP 1 ORG.START_OF_BIZ_WEEK_INT  
                                FROM DATAWAREHOUSE.ORGANIZATION_DIM          ORG
                                  INNER JOIN DATAWAREHOUSE.LOCATION_DIM      LOC 
                                     ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
                                        AND LOC.DW_ISCURRENTROW
                                        AND LOC.DW_ISCURRENTROW
                                        AND LOC.LOCATION_DIM_NK in (
                                             SELECT table1.value 
                                          FROM table(split_to_table(:locationidS, '',''))  table1) 
                             );
  lastWeekStart  date     := (DATEADD(DAY,
                                ((DAYOFWEEK(:today::date) - (:weekOffset )  ) * -1)  -7
                                + case when DAYOFWEEK(:today::date)< :weekOffset then -0 else 0 end
                                 , :today)) ::date;
  lastWeekEnd    date     := dateadd(day,6,:lastWeekStart);
  lastMonthStart date     := left(dateadd(month,-1,:today)::date::string,7) || ''-01'';
  lastMonthEnd   date     := dateadd(day,-1,dateadd(month,1,lastMonthStart))::date;
  lastYearStart  date     := date_part(year,(dateadd(year,-1,:today))) || ''-01-01'';                  
  lastYearEnd    date     := dateadd(day,-1,dateadd(year,1,:lastYearStart));

---====================================================================================================================
BEGIN
  DROP TABLE if exists CHK_DATA_TEMP;
  DROP TABLE if exists ITEM_DATA_TEMP;  

-----------------------------------------------------------------------------------------------------------------------
--Get history data from the Warehouse. Faster than streaming data-the warehouse has already processed JSON, etc
SELECT TO_CHAR(CHK."id")                                                      AS "Support ID"
  ,''Stream''                                                                   AS "Origin"
  ,CHK."items"                                                                AS "items"
  ,CHK."status"                                                               AS "Check Status"
  ,''Check''                                                                    AS "Level"
  ,CHK."id"                                                                   AS "Check ID"
  ,TRY_PARSE_JSON(CHK."info"):shiftID::DECIMAL(36,0)                          AS "Shift ID"
  ,CHK."location_id"                                                          AS "Location ID"  
  ,TO_CHAR(REPLACE(TRY_PARSE_JSON(CHK."info"):revenueCenterName,''"'',''''))      AS "Revenue Center"
  ,CHK."employee_id"                                                          AS "Employee ID"
  ,CHK."day_part_id"                                                          AS "Daypart ID"
  ,CHK."order_type_id"                                                        AS "Order Type ID"   
  ,DPD.DAYPART                                                                AS "Daypart"
  ,ORD.ORDER_TYPE                                                             AS "Order Type"
  ,LOC.LOCATIONNAME                                                           AS "Location"
  ,null                                                                       AS "Menu Item"
  ,null                                                                       AS "Gets Paid Break"  
--   ---------------------------------------------------------------------------------------
  ,TO_CHAR(TRY_PARSE_JSON(CHK."info"):fiscalDate)::DATE::date                 AS "Fiscal Date" 
  ,TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
      ,chk."opened_at"::timestamp_ntz)::timestamp ) ::DATE                                                                                                                                                AS "Opened At"  
----------------------------------------      
  ,TO_CHAR(TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
       ,chk."opened_at"::timestamp_ntz)::timestamp ) ::timestamp 
  , ''HH24'')::NUMBER(18,0)                                                      AS "Hour"  
  ---------------
  -- ,TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
  --      ,chk."opened_at"::timestamp_ntz)::timestamp ) ::timestamp            AS hourcheck
  --------------
  ,NULL                                                                        AS "Rate"
  ,TRUE                                                                        AS "Is Current"
  ,TRUE                                                                        AS "Is Today"                         
  ,FALSE                                                                       AS "Is Yesterday"
  ,FALSE                                                                       AS "Is Last Week" 
  ,FALSE                                                                       AS "Is Last Month" 
  ,FALSE                                                                       AS "Is Last Year"
-- -----------------------------------------------------------------------------------------------------------------
-- --Facts CHECK LEVEL
 ,TRY_PARSE_JSON(chk."info"):partySize::number(36,0)                           AS "Guest Count"
 ,TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK."balance"):fees), 36,2)             AS "Fee Amount"
 ,(CASE WHEN CHK."status" = ''Voided''
     THEN 0 ELSE ((TRY_PARSE_JSON(CHK."balance"):gratuities) )  END)::DECIMAL(36,2)      
                                                                               AS "Gratuity Amount"
  ,DATEDIFF(MINUTE
  ,IFNULL(CHK."opened_at",IFNULL(CHK."closed_at",CURRENT_TIMESTAMP))
  ,IFNULL(CHK."closed_at",CURRENT_TIMESTAMP))::NUMBER(36,0)                   
                                                                               AS "Table Time"  
--   -----------------------------------------------------------------------------------------------------------------
--  --Facts  ITEM LEVEL (all item level amounts should be filtered for voids)
  ,0::NUMBER(36,0)                                                             AS "Shift Seconds" 
  ,0::NUMBER(36,0)                                                             AS "Shift Count"
  ,0::NUMBER(36,0)                                                             AS "Item Count"  
  ,1::NUMBER(36,0)                                                             AS "Check Count" 
  ,(CASE WHEN CHK."status" = ''Voided'' THEN 1 ELSE 0 END)::NUMBER(36,0)         AS "Void Count"
  ,((CASE WHEN CHK."status" = ''Voided'' THEN TRY_TO_NUMBER(TO_CHAR(TRY_PARSE_JSON(CHK."balance"):gross), 38,4) 
         ELSE 0 END)
         )
         ::DECIMAL(36,2)
                                                                               AS "Void Amount" 
  ,0::DECIMAL(36,2)                                                            AS "Net Amount"
  ,(CASE WHEN CHK."status" = ''Voided''
     THEN 0 ELSE TO_CHAR(TRY_PARSE_JSON(chk."balance"):discountCheck) END) ::DECIMAL(36,2)                                  
                                                                               AS "Discount Amount"
  ,0::DECIMAL(36,2)                                                            AS "Gross Amount"  
--------------------------------------------------------------------------------------------------------------- 
FROM  DATASTREAMING.POSAPI_PUBLIC_CHEQUE                                       CHK
  INNER JOIN DATAWAREHOUSE.LOCATION_DIM                                        LOC
    ON CHK."location_id" = LOC.LOCATION_DIM_NK
      AND LOC.DW_ISCURRENTROW
      AND NOT CHK."is_training"
      AND CHK."status" IN (''Closed'',''Voided'',''Opened'')  
      AND NOT (CHK."status" = ''Opened'' AND TO_CHAR(TRY_PARSE_JSON(CHK."info"):fiscalDate)::DATE <= :today::date )
      AND TO_CHAR(TRY_PARSE_JSON(CHK."info"):fiscalDate)::DATE = :today::date  
      AND LOC.LOCATION_DIM_NK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
  INNER JOIN DATAWAREHOUSE.DAYPART_DIM                                         DPD
    ON CHK."day_part_id" = DPD.DAYPART_DIM_NK
      AND DPD.DW_ISCURRENTROW
 LEFT JOIN DATAWAREHOUSE.ORDERTYPE_DIM                                        ORD
    ON CHK."order_type_id" = ORD.ORDERTYPE_DIM_NK
      AND ORD.DW_ISCURRENTROW          
;

CREATE TEMP TABLE CHK_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;

-- ====================================================================================================================
SELECT  CHK."Check ID" || ''.''  ||replace(ITM.VALUE:id,''"'','''')                AS "Support ID"
  ,''Stream''                                                                  AS "Origin"
  , CHK."Check Status"                                                       AS "Check Status"
  ,''Item''                                                                    AS "Level"
  ,CHK."Check ID"                                                            AS "Check ID"
  ,CHK."Shift ID"                                                            AS "Shift ID"
  ,CHK."Location ID"                                                         AS "Location ID"  
  ,CHK."Revenue Center"                                                      AS "Revenue Center"
  ,CHK."Employee ID"                                                         AS "Employee ID"
  ,CHK."Daypart ID"                                                          AS "Daypart ID"
  ,CHK."Order Type ID"                                                       AS "Order Type ID"   
  ,CHK."Daypart"                                                             AS "Daypart"
  ,CHK."Order Type"                                                          AS "Order Type"
  ,CHK."Location"                                                            AS "Location"
  ,replace(ITM.value:menuItem:name,'''','''')                                    AS "Menu Item"
  ,null                                                                      AS "Gets Paid Break" 
--   ---------------------------------------------------------------------------------------
  ,CHK."Fiscal Date"                                                         AS "Fiscal Date" 
  ,CHK."Opened At"                                                           AS "Opened At"  
  ,CHK."Hour"                                                                AS "Hour"  
  ,NULL::DECIMAL(18,2)                                                       AS "Rate"
  ,CHK."Is Current"                                                          AS "Is Current"
  ,CHK."Is Today"                                                            AS "Is Today"                            
  ,CHK."Is Yesterday"                                                        AS "Is Yesterday"
  ,CHK."Is Last Week"                                                        AS "Is Last Week"
  ,CHK."Is Last Month"                                                       AS "Is Last Month"
  ,CHK."Is Last Year"                                                        AS "Is Last Year"
-- -----------------------------------------------------------------------------------------------------------------
-- --Facts CHECK LEVEL
  ,0::DECIMAL(36,0)                                                           AS "Guest Count"
  ,0::DECIMAL(36,0)                                                           AS "Fee Amount"
  ,0::DECIMAL(36,0)                                                           AS "Gratuity Amount"
  ,0::DECIMAL(36,0)                                                           AS "Table Time"   
--   -----------------------------------------------------------------------------------------------------------------
--    --Facts ITEM LEVEL (all item level amounts should be filtered for voids)
  ,0::NUMBER(36,0)                                                            AS "Shift Seconds" 
  ,0::NUMBER(36,0)                                                            AS "Shift Count"
  ,CASE WHEN NOT replace(ITM.value:status,''"'','''') = ''Voided'' 
     AND NOT CHK."Check Status" = ''Voided''
      THEN CAST(ITM.value:quantity AS DECIMAL(36,0)) END                      AS "Item Count"  
  ,0::DECIMAL(36,0)                                                           AS "Check Count" 
  ,CASE WHEN  replace(ITM.value:status,''"'','''') = ''Voided'' 
     OR  CHK."Check Status" = ''Voided''  
      THEN CAST(ITM.value:quantity AS DECIMAL(36,0))  END                     AS "Void Count"
  ,CASE WHEN  replace(ITM.value:status,''"'','''') = ''Voided'' 
      THEN CAST(ITM.value:gross AS DECIMAL(36,0)) ELSE 0 END ::DECIMAL(36,2)                                                                                                                              AS "Void Amount" 
  ,CAST(CASE WHEN NOT replace(ITM.value:status,''"'','''') = ''Voided'' 
      AND NOT CHK."Check Status" = ''Voided''
    THEN CAST(ITM.value:gross AS DECIMAL(38,4)) ELSE 0.00 END AS DECIMAL(38,2))  
       - CAST(ITM.value:discountItem AS DECIMAL(38,4)) 
                                                                               AS "Net Amount"
  ,CAST(CASE WHEN NOT replace(ITM.value:status,''"'','''') = ''Voided'' 
      AND NOT CHK."Check Status" = ''Voided''
    THEN CAST(ITM.value:discountItem AS DECIMAL(38,4)) ELSE 0 END AS DECIMAL(36,2)) 
                                                                               AS "Discount Amount"
  ,CAST(CASE WHEN NOT replace(ITM.value:status,''"'','''') = ''Voided'' 
      AND NOT CHK."Check Status" = ''Voided''
    THEN CAST(ITM.value:gross AS DECIMAL(38,4))  ELSE 0 END AS DECIMAL(36,2))  AS "Gross Amount"  
-- ----------------------------------------------------------------------------------------------------------------- 
FROM CHK_DATA_TEMP                                                             CHK 
 ,LATERAL FLATTEN(INPUT => 
    TRY_PARSE_JSON( ''{ITEMS:'' || CHK."items" || ''}'' ), PATH => ''ITEMS'')        ITM                                   
 WHERE   replace(ITM.value:status,''"'','''') IN (''Added'',''Sent'',''Voided'')                                                               ;

CREATE TEMP TABLE ITEM_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;

ALTER TABLE CHK_DATA_TEMP DROP COLUMN "items";
-- ====================================================================================================================

 reportSet   := (
 SELECT * FROM ITEM_DATA_TEMP  
 UNION ALL
 SELECT * FROM CHK_DATA_TEMP 
  --  SELECT :weekOffset as "weekoffset"
  --  ,:today as "today"
  -- ,:yesterdaydate as "yesterdaydate"
  -- ,:lastWeekStart as "lastWeekStart"
  -- ,:lastWeekEnd as "lastWeekEnd" 
  -- ,:lastMonthStart as "lastMonthStart"
  -- ,:lastMonthEnd as "lastMonthEnd"
  -- ,:lastYearStart as "lastYearStart"
  -- ,:lastYearEnd as "lastYearEnd"
   
--===================================================================================================
); 
 RETURN TABLE(reportSet); 
END';