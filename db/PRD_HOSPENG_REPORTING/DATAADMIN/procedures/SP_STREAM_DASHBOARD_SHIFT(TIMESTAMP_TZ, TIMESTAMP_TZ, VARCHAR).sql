CREATE OR REPLACE PROCEDURE "SP_STREAM_DASHBOARD_SHIFT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet      resultset;
  -- startdate      date     := ''2020-03-26'';  
  -- enddate        date     := ''2029-04-10''; 
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
  DROP TABLE if exists SHIFT_DATA_TEMP; 
  DROP TABLE if exists BREAK_DATA_TEMP; 

-----------------------------------------------------------------------------------------------------------------------
SELECT TO_CHAR(SHD."id")                                                         AS "Support ID"
  ,''Stream''                                                                      AS "Origin"
  ,''None''                                                                        AS "Check Status"
  ,''Shift''                                                                       AS "Level"
  ,NULL                                                                          AS "Check ID"
  ,SHD."id"                                                                      AS "Shift ID"
  ,SHD."location_id"                                                             AS "Location ID"  
  ,NULL                                                                          AS "Revenue Center"
  ,SHD."employee_id"                                                             AS "Employee ID"
  ,NULL                                                                          AS "Daypart ID"
  ,NULL                                                                          AS "Order Type ID"   
  ,NULL                                                                          AS "Daypart"
  ,NULL                                                                          AS "Order Type"
  ,LOC.LOCATIONNAME                                                              AS "Location"
  ,NULL                                                                          AS "Menu Item"
  ,FALSE                                                                         AS "Gets Paid Break"
--   ---------------------------------------------------------------------------------------
  ,SHD."fiscal_date"::DATE                                                       AS "Fiscal Date" 
  ,NULL                                                                          AS "Opened At"  
  ,TO_CHAR(TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
       ,SHD."clock_in"::timestamp_ntz)::timestamp ) ::timestamp 
  , ''HH24'')::NUMBER(18,0)                                                                           
                                                                                 AS "Hour"  
  ,(SHD."pay_rate"/1000000)::DECIMAL(36,2)                                       AS "Rate"
  ,TRUE                                                                          AS "Is Current"
  ,TRUE                                                                          AS "Is Today"                         
  ,FALSE                                                                         AS "Is Yesterday"
  ,FALSE                                                                         AS "Is Last Week" 
  ,FALSE                                                                         AS "Is Last Month" 
  ,FALSE                                                                         AS "Is Last Year"
-- -----------------------------------------------------------------------------------------------------------------
-- --Facts CHECK LEVEL
 ,0::NUMBER(36,0)                                                                AS "Guest Count"
 ,0 ::NUMBER(36,0)                                                               AS "Fee Amount"
 ,0 ::NUMBER(36,0)                                                               AS "Gratuity Amount"
 ,0 ::NUMBER(36,0)                                                               AS "Table Time"   
--   -----------------------------------------------------------------------------------------------------------------
--  --Facts  ITEM LEVEL (all item level amounts should be filtered for voids)
 ,ROUND(TIMEDIFF(second
               ,SHD."clock_in"::timestamp_ntz 
               ,COALESCE(SHD."clock_out"::timestamp_ntz,CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP())::timestamp_ntz)
              )::Number(36,0),0)
                                                                               AS "Shift Seconds"  
  ,1::NUMBER(36,0)                                                             AS "Shift Count"
  ,0::NUMBER(36,0)                                                             AS "Item Count"  
  ,0::NUMBER(36,0)                                                             AS "Check Count" 
  ,0::NUMBER(36,0)                                                             AS "Void Count"
  ,0::DECIMAL(36,2)                                                            AS "Void Amount" 
  ,0::DECIMAL(36,2)                                                            AS "Net Amount"
  ,0::DECIMAL(36,2)                                                            AS "Discount Amount"
  ,0::DECIMAL(36,2)                                                            AS "Gross Amount"  
----------------------------------------------------------------------------------------------------------------- 
  FROM DATASTREAMING.POSAPI_PUBLIC_EMPLOYEE_SHIFT                                 SHD
          INNER JOIN DATAWAREHOUSE.LOCATION_DIM                                   LOC
             ON SHD."location_id" = LOC.LOCATION_DIM_NK
                AND LOC.DW_ISCURRENTROW
                AND  SHD."fiscal_date"::date = :today::date  
                -- AND   SHD."location_id" = 999999
                AND SHD."location_id" in (
                  SELECT table1.value 
                     FROM table(split_to_table(:locationidS, '',''))  table1)
          INNER JOIN DATAWAREHOUSE.JOBPOSITION_DIM                 JBP
            ON JBP.JOBPOSITION_DIM_NK = SHD."job_position_id" 
              AND JBP.DW_ISCURRENTROW
;

CREATE TEMP TABLE SHIFT_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;

-----------------------------------------------------------------------------------------------------------------------
SELECT SHD_1."Shift ID"                                         AS "Shift ID"
 ,SHD_1."Gets Paid Break"                                       AS "Gets Paid Break"
 ,SUM(IFF( BRK.IS_BREAKCOMPLETE ,BRK.BREAK_SECONDS , 
          ROUND(TIMEDIFF(SECOND
            ,BRK.START_AT::timestamp_ntz 
            ,CURRENT_TIMESTAMP::timestamp_ntz 
                )::Number(38,0),0)) )                                                  
                                                                AS "Break Seconds"

          FROM SHIFT_DATA_TEMP                                  SHD_1
            INNER JOIN DATAWAREHOUSE.SHIFTBREAK_FACT            BRK
              ON SHD_1."Shift ID" = BRK.SHIFT_DIM_FK
                 AND BRK.DW_ISCURRENTROW
                 AND NOT BRK.DW_ISDELETED
         GROUP BY SHD_1."Shift ID"
            ,SHD_1."Gets Paid Break"
;


CREATE TEMP TABLE BREAK_DATA_TEMP AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;

-- ----------------------------------------------------------------------------------------------------------------------
UPDATE SHIFT_DATA_TEMP  shd_2
  SET shd_2."Shift Seconds" = shd_2."Shift Seconds"
    - (IFNULL((CASE WHEN brk_1."Gets Paid Break" THEN 0 ELSE brk_1."Break Seconds" END),0))
  FROM BREAK_DATA_TEMP  brk_1
  WHERE shd_2."Shift ID" = brk_1."Shift ID"
    AND NOT brk_1."Gets Paid Break"
;

-- ====================================================================================================================
 reportSet   := (
     SELECT * FROM SHIFT_DATA_TEMP  
   
--===================================================================================================
); 
 RETURN TABLE(reportSet); 
END';