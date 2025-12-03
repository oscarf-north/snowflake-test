CREATE OR REPLACE PROCEDURE "SP_DATAWAREHOUSE_MANAPP_CHECK"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet      resultset;
  -- startdate      date     := ''2025-10-28'';  
  -- enddate        date     := ''2025-10-29''; 
  -- locationid     string   := ''[351]'';
  locationidS string      :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  today          char(11) := CURRENT_DATE()::DATE::VARCHAR(10);
  yesterdaydate  date     := dateadd(day,-1,:today);
  weekOffset     int      := (  
                              SELECT TOP 1 ORG.START_OF_BIZ_WEEK_INT  
                                FROM DATAWAREHOUSE.ORGANIZATION_DIM          ORG
                                  INNER JOIN DATAWAREHOUSE.LOCATION_DIM      LOC 
                                     ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
                                        AND LOC.DW_ISCURRENTROW
                                        AND ORG.DW_ISCURRENTROW
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

  thisWeekStart  date     := dateadd(day,1,:lastWeekEnd);
  thisMonthStart date     := dateadd(day,1,:lastMonthEnd);
  
  lastYearStart  date     := date_part(year,(dateadd(year,-1,:today))) || ''-01-01'';                  
  lastYearEnd    date     := dateadd(day,-1,dateadd(year,1,:lastYearStart));

---====================================================================================================================
BEGIN
  DROP TABLE if exists CHK_DATA_TEMP;
  DROP TABLE if exists ITEM_DATA_TEMP;  

-----------------------------------------------------------------------------------------------------------------------
--Get history data from the Warehouse. Faster than streaming data-the warehouse has already processed JSON, etc
SELECT TO_CHAR(CHK.CHEQUE_FACT_NK)                                            AS "Support ID"
  ,''DataWarehouse''                                                            AS "Origin"
  ,CHK.STATUS                                                                 AS "Check Status"
  ,''Check''                                                                    AS "Level"
  ,CHK.CHEQUE_FACT_NK                                                         AS "Check ID"
  ,CHK.SHIFT_DIM_FK                                                           AS "Shift ID"
  ,CHK.LOCATION_DIM_FK                                                        AS "Location ID"  
  ,CHK.REVENUECENTERNAME                                                      AS "Revenue Center"
  ,CHK.EMPLOYEE_DIM_FK                                                        AS "Employee ID"
  ,CHK.DAYPART_DIM_FK                                                         AS "Daypart ID"
  ,CHK.ORDERTYPE_DIM_FK                                                       AS "Order Type ID"   
  ,DPD.DAYPART                                                                AS "Daypart"
  ,ORD.ORDER_TYPE                                                             AS "Order Type"
  ,LOC.LOCATIONNAME                                                           AS "Location"
  ,null                                                                       AS "Menu Item"
  ,null                                                                       AS "Category" 
  ,null                                                                       AS "Gets Paid Break"  
  ---------------------------------------------------------------------------------------
  ,CHK.FISCAL_DATE::DATE                                                      AS "Fiscal Date" 
  ,TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
      ,chk.OPENED_AT::timestamp_ntz)::timestamp ) ::DATE                                                                                                                                                    AS "Opened At"  
  ,TO_CHAR(TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
       ,chk.OPENED_AT::timestamp_ntz)::timestamp ) ::timestamp 
  , ''HH24'') ::NUMBER(18,0)                                                    AS "Hour"  
  ,NULL/*(SHD.PAY_RATE/1000000)::DECIMAL(18,2)*/                              AS "Rate"
  ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    BETWEEN :startdate::DATE AND :enddate::DATE THEN TRUE ELSE FALSE END  
                                                                              AS "Is Current"
  ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    = :today::DATE THEN TRUE ELSE FALSE END                                   AS "Is Today"    
    
  ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    = :yesterdaydate::DATE THEN TRUE ELSE FALSE END                           AS "Is Yesterday"
    
  ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    BETWEEN :lastWeekStart::DATE AND :lastWeekEnd::DATE 
      THEN TRUE ELSE FALSE END                                                AS "Is Last Week"
      
  ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    BETWEEN :lastMonthStart::DATE AND :lastMonthEnd::DATE 
       THEN TRUE ELSE FALSE END                                               AS "Is Last Month"
       
 ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    >= :thisMonthStart::DATE 
       THEN TRUE ELSE FALSE END                                               AS "Is This Month"

 ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    >= :thisWeekStart::DATE 
       THEN TRUE ELSE FALSE END                                               AS "Is This Week"       
       
  ,CASE WHEN TO_CHAR(CHK.FISCAL_DATE)::DATE  
    BETWEEN :lastYearStart::DATE AND :lastYearEnd::DATE 
        THEN TRUE ELSE FALSE END                                              AS "Is Last Year"
-----------------------------------------------------------------------------------------------------------------
--Facts CHECK LEVEL
 ,(CASE WHEN CHK.IS_VOID THEN 0 ELSE CHK.PARTY_COUNT END)::DECIMAL(36,2)::DECIMAL(36,0)                                                                                                                      AS "Guest Count"
 ,(CASE WHEN CHK.IS_VOID THEN 0 ELSE CHK.FEES END)::DECIMAL(36,2)              AS "Fee Amount"
 ,(CASE WHEN CHK.IS_VOID THEN 0 ELSE CHK.GRATUITIES END) ::DECIMAL(36,2)       AS "Gratuity Amount"
 ,DATEDIFF(MINUTE
  ,IFNULL(CHK.OPENED_AT,IFNULL(CHK.CLOSED_AT,CURRENT_TIMESTAMP))
  ,IFNULL(CHK.CLOSED_AT,CURRENT_TIMESTAMP))::NUMBER(36,0)                   
                                                                               AS "Table Time"  
  -----------------------------------------------------------------------------------------------------------------
 --Facts  ITEM LEVEL (all item level amounts should be filtered for voids)
  ,0::NUMBER(36,0)                                                             AS "Shift Seconds" 
  ,0::NUMBER(36,0)                                                             AS "Shift Count"
  ,0::NUMBER(36,0)                                                             AS "Item Count"  
  ,(CASE WHEN CHK.STATUS = ''Voided'' THEN 0 ELSE 1 END)::NUMBER(36,0)           AS "Check Count" 
  ,(CASE WHEN CHK.STATUS = ''Voided'' THEN 1 ELSE 0 END)::NUMBER(36,0)           AS "Void Count"
  ,(CASE WHEN CHK.STATUS = ''Voided'' THEN CHK.GROSS ELSE 0 END)::NUMBER(36,2)   AS "Void Amount" 
  ,0::DECIMAL(36,2)                                                            AS "Net Amount"
  ,(CASE WHEN NOT CHK.STATUS = ''Voided'' THEN CHK.DISCOUNTCHECK ELSE 0 END)::DECIMAL(36,2)                                             
                                                                               AS "Discount Amount"
  ,0::DECIMAL(36,2)                                                            AS "Gross Amount"  

----------------------------------------------------------------------------------------------------------------- 
FROM DATAWAREHOUSE.CHEQUE_FACT                                                 CHK
  INNER JOIN DATAWAREHOUSE.LOCATION_DIM                                        LOC
    ON CHK.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
      AND LOC.DW_ISCURRENTROW
      AND CHK.DW_ISCURRENTROW
      AND NOT CHK.IS_TRAINING
      AND NOT CHK.DW_ISDELETED
      AND CHK.STATUS IN (''Closed'',''Opened'',''Voided'') 
      AND NOT (CHK.STATUS = ''Opened'' AND CHK.FISCAL_DATE::date <= :today::date )
      AND CHK.FISCAL_DATE::date >= :lastYearEnd::date 
              AND CHK.FISCAL_DATE::date <= :today::date  
              AND CHK.LOCATION_DIM_FK in (
                SELECT table1.value 
                   FROM table(split_to_table(:locationidS, '',''))  table1) 
  INNER JOIN DATAWAREHOUSE.DAYPART_DIM                                         DPD
    ON CHK.DAYPART_DIM_FK = DPD.DAYPART_DIM_NK
      AND DPD.DW_ISCURRENTROW
  INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                                       ORD
    ON CHK.ORDERTYPE_DIM_FK = ORD.ORDERTYPE_DIM_NK
      AND ORD.DW_ISCURRENTROW      
;
-- SELEcT "Check ID",COUNT(*) FROM CHK_DATA_TEMP GROUP BY "Check ID" ORDER BY COUNT(*) DESC;

CREATE TEMP TABLE CHK_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;
-- ====================================================================================================================
SELECT  TO_CHAR(ITM.ITEM_FACT_NK)                                            AS "Support ID"
  ,''DataWarehouse''                                                           AS "Origin"
  ,ITM.ITEMSTATUS                                                            AS "Check Status"
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
  ,ITM.NAME                                                                  AS "Menu Item"
  ,IFNULL(ccd.COGSCATEGORY,''None'')                                           AS "Category"  
  ,null                                                                      AS "Gets Paid Break" 
  ---------------------------------------------------------------------------------------
  ,CHK."Fiscal Date"                                                         AS "Fiscal Date" 
  ,CHK."Opened At"                                                           AS "Opened At"  
  ,CHK."Hour"                                                                AS "Hour"  
  ,NULL/*(SHD.PAY_RATE/1000000)::DECIMAL(18,2)*/                             AS "Rate"
  ,CHK."Is Current"                                                          AS "Is Current"
  ,CHK."Is Today"                                                            AS "Is Today"                            
  ,CHK."Is Yesterday"                                                        AS "Is Yesterday"
  ,CHK."Is This Week"                                                        AS "Is This Week"
  ,CHK."Is This Month"                                                       AS "Is This Month"
  ,CHK."Is Last Week"                                                        AS "Is Last Week"
  ,CHK."Is Last Month"                                                       AS "Is Last Month"
  ,CHK."Is Last Year"                                                        AS "Is Last Year"
-----------------------------------------------------------------------------------------------------------------
--Facts CHECK LEVEL
  ,0::DECIMAL(36,0)                                                           AS "Guest Count"
  ,0::DECIMAL(36,0)                                                           AS "Fee Amount"
  ,0::DECIMAL(36,0)                                                           AS "Gratuity Amount"
  ,0::DECIMAL(36,0)                                                           AS "Table Time"   
  -----------------------------------------------------------------------------------------------------------------
   --Facts ITEM LEVEL (all item level amounts should be filtered for voids)
  ,0::NUMBER(36,0)                                                            AS "Shift Seconds" 
  ,0::NUMBER(36,0)                                                            AS "Shift Count"
  ,CAST(CASE WHEN NOT ITM.ITEMSTATUS = ''Voided'' AND NOT CHK."Check Status" = ''Voided''
    THEN ITM.QUANTITY ELSE 0 END AS DECIMAL(36,0))                            AS "Item Count"  
 ,0:: DECIMAL(36,0)                                                           AS "Check Count"   
  ,CAST(CASE WHEN  ITM.ITEMSTATUS = ''Voided''
    THEN ITM.QUANTITY ELSE 0 END AS DECIMAL(36,0))                            AS "Void Count"
  ,(CASE WHEN  ITM.ITEMSTATUS = ''Voided''
    THEN
    CASE WHEN ITM.PRICE > 0.0000 THEN ITM.PRICE * ITM.QUANTITY ELSE ITM.baseprice END
     + (ifnull(IMF.PRICE,0)) * (ITM.QUANTITY) 
     ELSE 0 END )
      ::  DECIMAL(38,2)                            
                                                                              AS "Void Amount" 
  ,CAST(CASE WHEN NOT ITM.ITEMSTATUS = ''Voided'' AND NOT CHK."Check Status" = ''Voided''
    THEN ITM.APPLIEDAMOUNT ELSE 0 END AS DECIMAL(38,2))                       AS "Net Amount"
  ,CAST(CASE WHEN NOT ITM.ITEMSTATUS = ''Voided'' AND NOT CHK."Check Status" = ''Voided''
    THEN ITM.DISCOUNTITEM ELSE 0 END AS DECIMAL(36,2))                        AS "Discount Amount"
  ,CAST(CASE WHEN NOT ITM.ITEMSTATUS = ''Voided'' AND NOT CHK."Check Status" = ''Voided'' 
    THEN ITM.GROSS ELSE 0 END AS DECIMAL(36,2))                               AS "Gross Amount"  
----------------------------------------------------------------------------------------------------------------- 
FROM CHK_DATA_TEMP                                                            CHK 
  INNER JOIN DATAWAREHOUSE.ITEM_FACT                                          ITM
    ON CHK."Check ID" = ITM.CHEQUE_FACT_FK
      AND ITM.DW_ISCURRENTROW
      AND ITM.ITEMSTATUS IN (''Added'',''Sent'',''Voided'')
    INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                                  MED
       ON med.MENUITEMNAME_DIM_NK = itm.MENUITEMNAME_DIM_FK
          AND med.DW_ISCURRENTROW  
    INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM                                MEG
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW = TRUE
    INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                                  CCD
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW

      LEFT JOIN (SELECT ITEM_FACT_FK,SUM(PRICE) AS PRICE 
                    FROM DATAWAREHOUSE.ITEMMODIFIER_DIM 
                 WHERE DW_ISCURRENTROW
                 GROUP BY ITEM_FACT_FK
                                                                        )    IMF
              ON ITM.ITEM_FACT_NK = IMF.ITEM_FACT_FK 
;

CREATE TEMP TABLE ITEM_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;
--xxx
-- select "Support ID" from ITEM_DATA_TEMP;
-- select * From ITEM_DATA_TEMP where "Support ID" in (9095315,9098419);
-- ====================================================================================================================

 reportSet   := (
  SELECT ROW_NUMBER() OVER (ORDER BY "Location ID")                      AS "Support ID"
  ,"Location"                                                            AS "Location"
  ,"Location ID"                                                         AS "Location ID"  
  ,"Order Type"                                                          AS "Order Type"
  ,"Location"                                                            AS "Location"
  ,"Menu Item"                                                           AS "Menu Item"
  ,"Category"                                                            AS "Category"
  -----------------------------------------------------------------------------------------------------------------
  ,"Is Current"                                                          AS "Is Current"
  ,"Is Today"                                                            AS "Is Today"                            
  ,"Is Yesterday"                                                        AS "Is Yesterday"
  ,"Is Last Week"                                                        AS "Is Last Week"
  ,"Is Last Month"                                                       AS "Is Last Month"
  ,"Is This Month"                                                       AS "Is This Month"  
  ,"Is This Week"                                                        AS "Is This Week"   
  ,"Is Last Year"                                                        AS "Is Last Year"
-----------------------------------------------------------------------------------------------------------------
--Facts CHECK LEVEL
  ,SUM("Guest Count")::DECIMAL(36,0)                                     AS "Guest Count"    
  ,SUM("Fee Amount")::DECIMAL(36,2)                                      AS "Fee Amount"    
  ,SUM("Gratuity Amount")::DECIMAL(36,2)                                 AS "Gratuity Amount" 
  ,SUM("Table Time")::DECIMAL(36,0)                                      AS "Table Time"    
  -----------------------------------------------------------------------------------------------------------------
   --Facts ITEM LEVEL (all item level amounts should be filtered for voids)
  ,SUM("Check Count")::DECIMAL(36,0)                                     AS "Check Count"  
  ,SUM("Item Count")::DECIMAL(36,0)                                      AS "Item Count"  
  ,SUM("Void Amount")::DECIMAL(36,2)                                     AS "Void Amount"    
  ,SUM("Net Amount")::DECIMAL(36,2)                                      AS "Net Amount"    
  ,SUM("Discount Amount")::DECIMAL(36,2)                                 AS "Discount Amount"    
  ,SUM("Gross Amount")::DECIMAL(36,2)                                    AS "Gross Amount"  
FROM (
 SELECT * FROM ITEM_DATA_TEMP  --74
 UNION ALL
 SELECT * FROM CHK_DATA_TEMP 
 )  
 WHERE "Is Current"
   OR "Is Today"                            
   OR "Is Yesterday"
   OR "Is Last Week"
   OR "Is Last Month"
   OR "Is This Month"  
   OR "Is This Week"   
   OR "Is Last Year"
 GROUP BY  "Location"
  , "Location ID"  
  , "Order Type"
  , "Location"
  , "Menu Item"
  , "Category"
  -----------------------------------------------------------------------------------------------------------------
  , "Is Current"
  , "Is Today"                            
  , "Is Yesterday"
  , "Is Last Week"
  , "Is Last Month"
  , "Is This Month"  
  , "Is This Week"   
  , "Is Last Year"

-- SELECT :weekOffset as "weekoffset"
--   ,:today as "today"
--   ,:yesterdaydate as "yesterdaydate"
--   ,:lastWeekStart as "lastWeekStart"
--   ,:lastWeekEnd as "lastWeekEnd" 
--   ,:thisWeekStart as "thisWeekStart"   
--   ,:lastMonthStart as "lastMonthStart"
--   ,:lastMonthEnd as "lastMonthEnd"
--   ,:thisMonthStart as "thisMonthStart"
--   ,:lastYearStart as "lastYearStart"
--   ,:lastYearEnd as "lastYearEnd"
   
--===================================================================================================
); 
 RETURN TABLE(reportSet); 
END';