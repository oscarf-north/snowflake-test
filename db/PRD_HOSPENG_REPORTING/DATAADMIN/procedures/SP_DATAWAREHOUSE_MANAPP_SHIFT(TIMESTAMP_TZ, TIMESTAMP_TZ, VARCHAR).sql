CREATE OR REPLACE PROCEDURE "SP_DATAWAREHOUSE_MANAPP_SHIFT"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet      resultset;
  -- startdate      date     := ''2025-04-21'';  
  -- enddate        date     := ''2025-04-21''; 
  -- locationid     string   := ''[1,2,3,4,5,6,7,8,9,351]'';
  locationidS string      := REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
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
  
  thisWeekStart  date     := dateadd(day,1,dateadd(year,1,:lastWeekEnd));;
  ThisMonthStart date     :=dateadd(day,1,dateadd(year,1,:lastMonthEnd));;
  
  lastYearStart  date     := date_part(year,(dateadd(year,-1,:today))) || ''-01-01'';                  
  lastYearEnd    date     := dateadd(day,-1,dateadd(year,1,:lastYearStart));

---====================================================================================================================
BEGIN
  DROP TABLE if exists SHIFT_DATA_TEMP; 
  DROP TABLE if exists BREAK_DATA_TEMP; 

-----------------------------------------------------------------------------------------------------------------------
SELECT TO_CHAR(SHD.SHIFT_DIM_NK)                                                 AS "Support ID"
  ,''DataWarehouse''                                                               AS "Origin"
  ,''None''                                                                        AS "Check Status"
  ,''Shift''                                                                       AS "Level"
  ,SHD.SHIFT_DIM_NK                                                              AS "Shift ID"
  ,SHD.LOCATION_DIM_FK                                                           AS "Location ID"  
  ,SHD.EMPLOYEE_DIM_FK                                                           AS "Employee ID"
  ,LOC.LOCATIONNAME                                                              AS "Location"
  ,SHD.GETS_PAID_BREAK                                                           AS "Gets Paid Break"
  ---------------------------------------------------------------------------------------
  ,SHD.FISCAL_DAY::DATE                                                          AS "Fiscal Date"  
  ,TO_CHAR(TO_CHAR(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME
       ,SHD.CLOCKEDIN_AT::timestamp_ntz)::timestamp ) ::timestamp 
  , ''HH24'') ::NUMBER(5,0)                                                        AS "Hour"    
  ,SHD.REGULAR_RATE::DECIMAL(36,2)                                               AS "Rate"
  ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    BETWEEN :startdate::DATE AND :enddate::DATE THEN TRUE ELSE FALSE END         AS "Is Current"
  ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    = :today::DATE THEN TRUE ELSE FALSE END                                      AS "Is Today"                                                                              
  ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    = :yesterdaydate::DATE THEN TRUE ELSE FALSE END                              AS "Is Yesterday"
    
  ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    BETWEEN :lastWeekStart::DATE AND :lastWeekEnd::DATE 
      THEN TRUE ELSE FALSE END                                                   AS "Is Last Week"
      
  ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    BETWEEN :lastMonthStart::DATE AND :lastMonthEnd::DATE 
       THEN TRUE ELSE FALSE END                                                  AS "Is Last Month"

 ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    <= :thisMonthStart::DATE 
       THEN TRUE ELSE FALSE END                                                  AS "Is This Month"

 ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    <= :thisWeekStart::DATE 
       THEN TRUE ELSE FALSE END                                                  AS "Is This Week"         
       
  ,CASE WHEN TO_CHAR(SHD.FISCAL_DAY)::DATE  
    BETWEEN :lastYearStart::DATE AND :lastYearEnd::DATE 
        THEN TRUE ELSE FALSE END                                                 AS "Is Last Year"
-----------------------------------------------------------------------------------------------------------------
--Facts 
  ,IFF( SHD.IS_SHIFTCOMPLETE,SHD.SHIFT_SECONDS, 
          ROUND(TIMEDIFF(second
               ,SHD.CLOCKEDIN_AT::timestamp_ntz 
               ,CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP())::timestamp_ntz
              )::Number(36,0),0))
                                                                               AS "Shift Seconds"  
  ,1::NUMBER(36,0)                                                             AS "Shift Count"
  ,0::NUMBER(36,2)                                                             AS "Labor Cost"
----------------------------------------------------------------------------------------------------------------- 
  FROM DATAWAREHOUSE.SHIFT_DIM                                                 SHD
          INNER JOIN DATAWAREHOUSE.LOCATION_DIM                                LOC
             ON SHD.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                AND LOC.DW_ISCURRENTROW
                AND SHD.DW_ISCURRENTROW
                AND NOT SHD.DW_ISDELETED
                AND NOT SHD.IS_ARCHIVED
                AND SHD.FISCAL_DAY::date >= :lastYearEnd::date 
                AND SHD.FISCAL_DAY::date <= :today::date  
                AND SHD.LOCATION_DIM_FK in (
                  SELECT table1.value 
                     FROM table(split_to_table(:locationidS, '',''))  table1)
          INNER JOIN DATAWAREHOUSE.JOBPOSITION_DIM                 JBP
            ON JBP.JOBPOSITION_DIM_NK = SHD.JOBPOSITION_DIM_FK 
              AND JBP.DW_ISCURRENTROW
         ORDER BY SHD.CLOCKEDIN_AT DESC  
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

----------------------------------------------------------------------------------------------------------------------
UPDATE SHIFT_DATA_TEMP  shd_2
  SET shd_2."Shift Seconds" = shd_2."Shift Seconds"
    - (IFNULL( brk_1."Break Seconds" ,0)) 

  FROM BREAK_DATA_TEMP  brk_1
  WHERE shd_2."Shift ID" = brk_1."Shift ID"
    AND  brk_1."Gets Paid Break"
;

select "Rate","Shift Seconds","Labor Cost" from SHIFT_DATA_TEMP;

UPDATE SHIFT_DATA_TEMP  shd_2
  SET "Labor Cost" = ("Rate"/3600) * "Shift Seconds"
;  
  
--  ====================================================================================================================
 reportSet   := (
 
     SELECT "Location ID"
        -- ,"Fiscal Date"
        ,"Is Current"
        ,"Is Today"                            
        ,"Is Yesterday"
        ,"Is Last Week"
        ,"Is Last Month"
        ,"Is This Month"  
        ,"Is This Week"   
        ,"Is Last Year"
       ,COUNT (DISTINCT "Employee ID") AS "Employee Count"
       ,SUM("Labor Cost") AS "Labor Cost"
     FROM SHIFT_DATA_TEMP  
       WHERE "Is Current"
           OR "Is Today"                            
           OR "Is Yesterday"
           OR "Is Last Week"
           OR "Is Last Month"
           OR "Is This Month"  
           OR "Is This Week"   
           OR "Is Last Year"
     GROUP BY "Location ID"
        ,"Is Current"
        ,"Is Today"                            
        ,"Is Yesterday"
        ,"Is Last Week"
        ,"Is Last Month"
        ,"Is This Month"  
        , "Is This Week"   
        ,"Is Last Year"
    -- ORDER BY "Fiscal Date"     
   
--===================================================================================================
); 
 RETURN TABLE(reportSet); 
END';