CREATE OR REPLACE PROCEDURE "SP_REPORT_LABOR"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- --=============================================================================================
-- CALL DATAWAREHOUSE.SP_REPORTDATAGROOM(''SP_REPORT_LABOR'',365,351);
-- CALL DATAWAREHOUSE.SP_REPORT_LABOR(''2000-11-20T14:48:37.661Z'',''2026-11-20T14:48:37.661Z'',''[2,3,4]'');
-- GRANT usage ON procedure DATAWAREHOUSE.SP_REPORT_LABOR(timestamp_tz,timestamp_tz,string) TO ROLE DATA_REPLICATION_HOSPENG;
--=============================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2024-08-23T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2024-08-23T14:48:37.661Z''; 
  -- locationid string      := ''[2]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
-- =============================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_LABOR;
  DROP TABLE IF EXISTS TEMP_TIPGRAT;

  WITH SHIFT_CTE AS (
    SELECT SHD.SHIFT_DIM_PK --|| overtime_fact_nk || fiscal day and week or date                                 
                                                               AS "Support ID" 
        ,SHD.SHIFT_DIM_NK                                      AS "SHIFT_DIM_NK"
    --status, category, level------------------------------------------------------------------
        ,''Shift''                                               AS "Level"
    --geography--------------------------------------------------------------------------------
        ,IFNULL(loc.LOCATIONNAME,''None'')                       AS "Location"
        ,IFNULL(org.organization,''None'')                       AS "Organization"
    --dates-------------------------------------------------------------------------------------
        ,LOC.TZ_NAME                                            AS "Time Zone"
        ,TO_CHAR(DATE_PART(YEAR,SHD.CLOCKEDIN_AT::timestamp_ntz ))   
                                                               AS "Year"
        ,to_char(LEFT(SHD.CLOCKEDIN_AT::timestamp_ntz,7))                     
                                                               AS "Year and Month"
        ,SHD.CLOCKEDIN_AT::timestamp_ntz                             
                                                               AS "Clocked In At"
        ,SHD.CLOCKEDOUT_AT::timestamp_ntz                                    
                                                               AS "Clocked Out At"    
        ,IFNULL(DAYNAME(SHD.CLOCKEDIN_AT::timestamp_ntz),''None'')              
                                                               AS "Day of Week"
        ,CASE WHEN DAYNAME(SHD.CLOCKEDIN_AT::timestamp_ntz) IN (''Sat'',''Sun'')  
            THEN TRUE ELSE FALSE END                           
                                                               AS "Is Weekend"
        ,ORG.START_OF_PAYROLL_WEEK_INT                         AS "Week Offset"
                ,YEAR(TIMESTAMPADD(DAY,ORG.START_OF_PAYROLL_WEEK_INT,SHD.FISCAL_DAY::timestamp_ntz)) || ''-'' ||
         WEEKOFYEAR(TIMESTAMPADD(DAY,ORG.START_OF_PAYROLL_WEEK_INT,SHD.FISCAL_DAY::timestamp_ntz))                                                 
                                                               AS "Fiscal Week"                                                
        ,SHD.FISCAL_DAY                                        AS "Fiscal Day"  
    --flags--------------------------------------------------------------------------------------
        ,SHD.IS_SHIFTCOMPLETE                                  AS "Is Clocked Out"
        ,SHD.WAS_SYSTEM_CLOCKOUT                               AS "Was System Clock Out"
        ,SHD.GETS_PAID_BREAK                                   AS "Gets Paid Break"
    --people------------------------------------------------------------------------------------- 
        ,EMD.EMPLOYEE_NAME                                     AS "Employee"
        ,EMD.EMPLOYEE_DIM_NK                                   AS "Employee ID"
        ,SHD.JOBPOSITION_DIM_FK                                AS "JOBPOSITION_DIM_FK"
        ,SHD.LOCATION_DIM_FK                                   AS "LOCATION_DIM_FK"        
     --Descriptors-------------------------------------------------------------------------------- 
       ,JBP.JOB_POSITION                                       AS "Job Position"  --(i.e. Bartender, Cook, Server)    
       ,SHD.SHIFT                                              AS "Shift ID"  
       ,SHD.PAY_RATE_BASIS                                     AS "Pay Basis"
     --Facts-----------------------------------------------------------------------------------------
        ,NULL                                                  AS "Overtime Rule"
        ,NULL                                                  AS "Hours Per Day"
        ,NULL                                                  AS "Seconds Per Day"
        ,NULL                                                  AS "Hours Per Week"
        ,NULL                                                  AS "Seconds Per Week"
        ,to_number(1)                                          AS "Shift Count"
        ,NULL                                                  AS "Break Count"  
        ,COUNT(SHD.SHIFT) OVER (PARTITION BY SHD.LOCATION_DIM_FK,EMD.EMPLOYEE_DIM_NK,SHD.FISCAL_DAY)  
                                                               AS "Shift Per Day Count"
        -- ,IFF( SHD.IS_SHIFTCOMPLETE,SHD.SHIFT_SECONDS, 
        --   ROUND(TIMEDIFF(second
        --        ,CONVERT_TIMEZONE(''UTC'',loc.TZ_NAME,shd.CLOCKEDIN_AT::timestamp_ntz )::timestamp
        --     -- ,SHD.CLOCKEDIN_AT::timestamp_ntz 
        --        ,CONVERT_TIMEZONE(''UTC'',loc.TZ_NAME,CURRENT_TIMESTAMP::timestamp_ntz )::timestamp
        --     -- ,CURRENT_TIMESTAMP::timestamp_ntz 
        --       )::Number(38,0),0))
        --                                                        AS "Shift Seconds"


        ,IFF( SHD.IS_SHIFTCOMPLETE,SHD.SHIFT_SECONDS, 
          ROUND(TIMEDIFF(second
               ,SHD.CLOCKEDIN_AT::timestamp_ntz 
               ,CONVERT_TIMEZONE(''UTC'', CURRENT_TIMESTAMP())::timestamp_ntz
              )::Number(38,0),0))
                                                               AS "Shift Seconds"

                                                               
        ,NULL                                                  AS "Break Seconds"


        ,shd.REGULAR_RATE * 1.5::Number(18,2)             
                                                               AS "Overtime Rate"
        ,shd.REGULAR_RATE::Number(18,2)                        AS "Regular Rate"
 
        FROM DATAWAREHOUSE.SHIFT_DIM                               SHD
          INNER JOIN DATAWAREHOUSE.LOCATION_DIM                    LOC
             ON SHD.LOCATION_DIM_FK = LOC.LOCATION_DIM_NK
                AND LOC.DW_ISCURRENTROW
                AND SHD.DW_ISCURRENTROW
             
                AND NOT SHD.DW_ISDELETED
                AND NOT SHD.IS_ARCHIVED
                AND shd.fiscal_day::date
                   >= dateadd(DAY,-10,:startdate)::date --calculate data 10 days around selected dates so that  
                AND  shd.fiscal_day::date  
                  <= dateadd(DAY,10,:enddate)::date    --the data in the selected range calcs overtime for full fiscal week
                AND SHD.LOCATION_DIM_FK IN ( SELECT table1.value 
                           FROM table(split_to_table(:locationidS, '',''))  table1)
          INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM                ORG
            ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
              AND ORG.DW_ISCURRENTROW
          INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM                    EMD
            ON SHD.EMPLOYEE_DIM_FK = EMD.EMPLOYEE_DIM_NK
              AND EMD.DW_ISCURRENTROW
          INNER JOIN DATAWAREHOUSE.JOBPOSITION_DIM                 JBP
            ON JBP.JOBPOSITION_DIM_NK = SHD.JOBPOSITION_DIM_FK 
              AND JBP.DW_ISCURRENTROW
          -- INNER JOIN DATAWAREHOUSE.EMPLOYMENTPERIOD_DIM          EPD
          --   ON EPD.JOBPOSITION_DIM_FK = SHD.JOBPOSITION_DIM_FK
          --     AND EPD.LOCATION_DIM_FK = SHD.LOCATION_DIM_FK
          --     AND EPD.EMPLOYEE_DIM_FK = SHD.EMPLOYEE_DIM_FK
          --     AND EPD.DW_ISCURRENTROW
         ORDER BY SHD.CLOCKEDIN_AT DESC          )
    ,BREAK_CTE AS (
        SELECT SHD_1."Support ID"                               AS "Support ID" 
        ,MAX(SHD_1.SHIFT_DIM_NK)                                AS "SHIFT_DIM_NK"
    --status, category, level------------------------------------------------------------------
        ,''Break''                                                AS "Level"
    --geography--------------------------------------------------------------------------------
        ,NULL                                                   AS "Location"
        ,NULL                                                   AS "Organization"
    -- --dates-------------------------------------------------------------------------------------
        ,MAX(SHD_1."Time Zone")                                 AS "Time Zone"
        ,NULL                                                   AS "Year"
        ,NULL                                                   AS "Year and Month"
        ,NULL                                                   AS "Clocked In At"
        ,NULL                                                   AS "Clocked Out At"    
        ,NULL                                                   AS "Day of Week"
        ,NULL                                                   AS "Is Weekend"
        ,NULL                                                   AS "Week Offset"
        ,MAX(SHD_1."Fiscal Week")                               AS "Fiscal Week"                      
        ,NULL                                                   AS "Fiscal Day"  
    -- --flags--------------------------------------------------------------------------------------
        ,NULL                                                   AS "Is Clocked Out"
        ,NULL                                                   AS "Was System Clock Out"
        ,NULL                                                   AS "Gets Paid Break"
    -- --people------------------------------------------------------------------------------------- 
        ,NULL                                                   AS "Employee"
        ,NULL                                                   AS "Employee ID"
        ,NULL                                                   AS JOBPOSITION_DIM_FK
        ,NULL                                                   AS LOCATION_DIM_FK       
    --  --Descriptors-------------------------------------------------------------------------------- 
       ,NULL                                                    AS "Job Position"  --(i.e. Bartender, Cook, Server)  
       ,NULL                                                    AS "Pay Basis"
       ,NULL                                                    AS "Shift ID"  
    -- Facts-----------------------------------------------------------------------------------------
        ,NULL                                                   AS "Overtime Rule"
        ,NULL                                                   AS "Hours Per Day"
        ,NULL                                                   AS "Seconds Per Day"
        ,NULL                                                   AS "Hours Per Week"
        ,NULL                                                   AS "Seconds Per Week"
        ,NULL                                                   AS "Shift Count"
        ,1                                                      AS "Break Count"   
        ,NULL                                                   AS "Shift Per Day Count"
        ,0                                                      AS "Shift Seconds"
        ,SUM(IFF( BRK_1.IS_BREAKCOMPLETE ,BRK_1.BREAK_SECONDS , 
          ROUND(TIMEDIFF(SECOND
            ,BRK_1.START_AT::timestamp_ntz 
            ,CURRENT_TIMESTAMP::timestamp_ntz 
                )::Number(38,0),0)) )                                                  
                                                                AS "Break Seconds"
     
        ,NULL                                                   AS "Overtime Rate"
        ,NULL                                                   AS "Regular Rate"
          FROM SHIFT_CTE                                        SHD_1
            INNER JOIN DATAWAREHOUSE.SHIFTBREAK_FACT                BRK_1
              ON SHD_1.SHIFT_DIM_NK = BRK_1.SHIFT_DIM_FK
                 AND BRK_1.DW_ISCURRENTROW
                 AND NOT BRK_1.DW_ISDELETED
            GROUP BY SHD_1."Support ID"  
    )
    ,RULE_CTE AS (  ---NOTE:  THIS JOIN SHOULD NOT BE CURRENT ROW...IT SHOULD BE ROW WHERE TIME = SHIFT START
         SELECT SHD_2."Support ID"                              AS "Support ID" 
        ,SHD_2.SHIFT_DIM_NK                                     AS "SHIFT_DIM_NK"
    --status, category, level------------------------------------------------------------------
        ,''Rule''                                                 AS "Level"
    --geography--------------------------------------------------------------------------------
        ,NULL                                                   AS "Location"
        ,NULL                                                   AS "Organization"
    -- --dates-------------------------------------------------------------------------------------
        ,NULL                                                   AS "Time Zone"    
        ,NULL                                                   AS "Year"
        ,NULL                                                   AS "Year and Month"
        ,NULL                                                   AS "Clocked In At"
        ,NULL                                                   AS "Clocked Out At"    
        ,NULL                                                   AS "Day of Week"
        ,NULL                                                   AS "Is Weekend"
        ,NULL                                                   AS "Week Offset"
        ,SHD_2."Fiscal Week"                                    AS "Fiscal Week"                      
        ,NULL                                                   AS "Fiscal Day"    
    -- --flags--------------------------------------------------------------------------------------
        ,NULL                                                   AS "Is Clocked Out"
        ,NULL                                                   AS "Was System Clock Out"
        ,NULL                                                   AS "Gets Paid Break"
    -- --people------------------------------------------------------------------------------------- 
        ,NULL                                                   AS "Employee"
        ,NULL                                                   AS "Employee ID"        
        ,NULL                                                   AS JOBPOSITION_DIM_FK
        ,NULL                                                   AS LOCATION_DIM_FK       
    --  --Descriptors-------------------------------------------------------------------------------- 
       ,NULL                                                    AS "Job Position"  --(i.e. Bartender, Cook, Server) 
       ,NULL                                                    AS "Pay Basis"
       ,NULL                                                    AS "Shift ID"  
    -- Facts-----------------------------------------------------------------------------------------
        ,OLT.OVERTIMERULE                                       AS "Overtime Rule"
        ,CASE WHEN OLT.HOURS_PER_DAY = 0 or :locationidS in (''[3]'',''[4]'',''[5]'')
            THEN 99999 ELSE  OLT.HOURS_PER_DAY END                                      
                                                                AS "Hours Per Day"
        ,CASE WHEN OLT.HOURS_PER_DAY = 0 or :locationidS in (''[3]'',''[4]'',''[5]'')
            THEN 99999 ELSE  OLT.HOURS_PER_DAY END * 60 * 60                                 
                                                                AS "Seconds Per Day"
                                                                
        ,CASE WHEN OLT.HOURS_PER_DAY = 0 or :locationidS in (''[3]'',''[4]'',''[5]'')
           THEN 99999 ELSE OLT.HOURS_PER_WEEK  END                                   
                                                                AS "Hours Per Week"
        ,CASE WHEN OLT.HOURS_PER_DAY = 0 or :locationidS in (''[3]'',''[4]'',''[5]'') 
          THEN 99999 ELSE OLT.HOURS_PER_WEEK * 60 *60 END                         
                                                                AS "Seconds Per Week"
        
        ,NULL                                                   AS "Shift Count"
        ,0                                                      AS "Break Count"   
        ,NULL                                                   AS "Shift Per Day Count"
        ,0                                                      AS "Shift Seconds"
        ,NULL                                                   AS "Break Seconds"
     
        ,NULL                                                   AS "Overtime Rate"
        ,NULL                                                   AS "Regular Rate"
          FROM SHIFT_CTE                                        SHD_2
            INNER JOIN OVERTIMELABORRULE_JOBPOSITION_XREF       OJX
           ON OJX.JOBPOSITION_DIM_FK = SHD_2.JOBPOSITION_DIM_FK
             AND OJX.DW_ISCURRENTROW
         INNER JOIN DATAWAREHOUSE.OVERTIMELABORRULE_DIM             OLT
            ON SHD_2.LOCATION_DIM_FK = OLT.LOCATION_DIM_FK
              AND OLT.OVERTIMELABORRULE_DIM_PK = OJX.OVERTIMELABORRULE_DIM_FK
              AND OLT.DW_ISCURRENTROW  
              AND NOT OLT.DW_ISDELETED
              AND OLT.IS_ACTIVE
    )  --end of cte tables
    --=========================================================================================
    SELECT to_char(UNI_2."Support ID")           as "Support ID"
    , ''LAB-'' ||row_number() over (order by UNI_2."Shift ID") 
                                                 as "Detail ID"  
    ,UNI_2."Location"
    ,UNI_2."Time Zone"
    ,UNI_2."Year"
    ,UNI_2."Year and Month"
    ,CONVERT_TIMEZONE(''UTC'',UNI_2."Time Zone",UNI_2."Clocked In At"::timestamp_ntz )::timestamp 
    --,UNI_2."Clocked In At"
                                                     as "Clocked In At"
    ,CONVERT_TIMEZONE(''UTC'',UNI_2."Time Zone",UNI_2."Clocked Out At"::timestamp_ntz )::timestamp                                                      
    --,UNI_2."Clocked Out At"
                                                     as "Clocked Out At"

---------------------------------
    ,to_varchar(CONVERT_TIMEZONE(''UTC'',UNI_2."Time Zone",UNI_2."Clocked In At"::timestamp_ntz )::timestamp,''HH12:MI:SS'')
                                                      as "Clock In Time"
    ,to_varchar(CONVERT_TIMEZONE(''UTC'',UNI_2."Time Zone",UNI_2."Clocked Out At"::timestamp_ntz )::timestamp,''HH12:MI:SS'')
                                                      as "Clock Out Time"                                                      
---------------------------------
    ,UNI_2."Day of Week"
    ,UNI_2."Is Weekend"
    ,UNI_2."Fiscal Week"
    ,to_char(UNI_2."Fiscal Day")                      as "Fiscal Day"
    ,UNI_2."Is Clocked Out"
    ,UNI_2."Was System Clock Out"
    ,UNI_2."Employee"
    -- ,TO_VARCHAR(UNI_2."Employee ID")               as "Employee ID"
    ,UNI_2."Job Position"
    ,UNI_2."Pay Basis"
    ,''Shift '' ||UNI_2."Shift ID"                      as "Shift ID"
    ,UNI_2."Overtime Rule"
    ,UNI_2."Overtime Rate"::NUMBER(18,2)              as "Overtime Rate"
    ,UNI_2."Regular Rate" ::NUMBER(18,2)              as "Regular Rate"
    --=======================================================================  
    ,UNI_2."Shift Seconds"::NUMBER(18,0)              as "Shift Seconds"
    ,UNI_2."Break Seconds"::NUMBER(18,0)              as "Break Seconds"
    ,UNI_2."Seconds Per Week"::NUMBER(18,0)           as "Weekly Overtime Rule Seconds"
    ,UNI_2."Seconds Per Day" ::NUMBER(18,0)           as "Daily Overtime Rule Seconds"
--===================== ====================================================================================
   -- ,UNI_2."Shift Regular Seconds"::NUMBER(18,0)    as "Regular Seconds"

   --    ,IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
   --      AND UNI_2."Seconds Per Week" IS NOT NULL
   --          ,IFF(UNI_2."Seconds Per Week" <
   --               LAG(UNI_2."Day Regular Seconds Running Total") 
   --                 OVER (PARTITION BY  UNI_2."Employee", UNI_2."Fiscal Week"  ORDER BY UNI_2."Clocked In At")
   --              ,UNI_2."Shift Regular Seconds"
   --              ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
   --              )   
   --          ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0)                                                      
   --                                                    as "Overtime Seconds" 

   ,UNI_2."Shift Regular Seconds"::NUMBER(18,0)       
     -
    IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
        AND UNI_2."Seconds Per Week" IS NOT NULL
            ,IFF(UNI_2."Seconds Per Week" <
                 LAG(UNI_2."Day Regular Seconds Running Total") 
                   OVER (PARTITION BY  UNI_2."Employee", UNI_2."Fiscal Week"  ORDER BY UNI_2."Clocked In At")
                ,UNI_2."Shift Regular Seconds"
                ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
                )   
            ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0) 

     
       as "Regular Seconds"
--=====================  ===================================================================================
   -- ,UNI_2."Day Regular Seconds Running Total"
   -- -- -- ,UNI_2."Shift Day Overtime Seconds"
   -- -- -- ,UNI_2."Seconds Per Week"
   -- ,LAG(UNI_2."Day Regular Seconds Running Total") 
   --                 OVER (PARTITION BY  UNI_2."Employee", UNI_2."Fiscal Week"  ORDER BY UNI_2."Clocked In At")           
   --                                                 as "Last Running Total"
                                                   
   ,IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
        AND UNI_2."Seconds Per Week" IS NOT NULL
            ,IFF(UNI_2."Seconds Per Week" <
                 LAG(UNI_2."Day Regular Seconds Running Total") 
                   OVER (PARTITION BY  UNI_2."Employee", UNI_2."Fiscal Week"  ORDER BY UNI_2."Clocked In At")
                ,UNI_2."Shift Regular Seconds"
                ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
                )   
            ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0)                                                      
                                                      as "Overtime Seconds" 

--======================================================================================
 ,floor(UNI_2."Shift Regular Seconds"/60/60/24) || '' D '' ||
       floor(UNI_2."Shift Regular Seconds"/60/60%24) || '':'' ||
       floor(UNI_2."Shift Regular Seconds"/60%60) || '':'' ||
       floor(UNI_2."Shift Regular Seconds"%60)         as "Regular D H:M:S"


 ,floor(IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
                       AND UNI_2."Seconds Per Week" IS NOT NULL
                        ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
                        ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0)/60/60/24) || '' D '' ||
       floor(IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
                       AND UNI_2."Seconds Per Week" IS NOT NULL
                        ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
                        ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0)/60/60%24) || '':'' ||
       floor(IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
                       AND UNI_2."Seconds Per Week" IS NOT NULL
                        ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
                        ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0)/60%60) || '':'' ||
       floor(IFF(UNI_2."Day Regular Seconds Running Total" > UNI_2."Seconds Per Week" 
                       AND UNI_2."Seconds Per Week" IS NOT NULL
                        ,UNI_2."Day Regular Seconds Running Total" - UNI_2."Seconds Per Week"
                        ,UNI_2."Shift Day Overtime Seconds") ::NUMBER(18,0)%60)         as "Overtime D H:M:S"  
  ,null ::NUMBER(18,2)                                                                  as "Tips"           
  ,null ::NUMBER(18,2)                                                                  as "Gratuities"        
--======================================================================================
      FROM(
            SELECT UNI_1.* 
                   ,SUM(IFF(UNI_1."Shift Seconds" < UNI_1."Seconds Per Day"
                       OR UNI_1."Seconds Per Day" IS NULL
                        ,UNI_1."Shift Seconds" 
                        ,UNI_1."Seconds Per Day" )) 
                            OVER (PARTITION BY UNI_1.LOCATION_DIM_FK,UNI_1."Employee ID" ,UNI_1."Fiscal Week" ORDER BY UNI_1."Shift ID" 
                    )                                                             AS "Day Regular Seconds Running Total" 
                
                    ,IFF(UNI_1."Seconds Per Day" < UNI_1."Shift Seconds"  
                         AND UNI_1."Seconds Per Day" IS NOT NULL
                        ,UNI_1."Seconds Per Day"
                        ,UNI_1."Shift Seconds" 
                         )                                 AS "Shift Regular Seconds" 
                        
                    ,IFF(UNI_1."Shift Seconds" > UNI_1."Seconds Per Day" 
                       AND UNI_1."Seconds Per Day" IS NOT NULL
                        ,UNI_1."Shift Seconds" - UNI_1."Seconds Per Day" 
                        ,0)                                                        AS "Shift Day Overtime Seconds" 
                                                                                
              FROM (
              SELECT UNI_0.* 
               -- ,CASE WHEN UNI_0."Shift Per Day Count" > 1 
               --    THEN SUM(UNI_0."Shift - Break Seconds") 
               --       OVER (PARTITION BY LOCATION_DIM_FK,"Employee ID" ,"Fiscal Day"   --see "Running Total Each Fiscal Day Seconds"below
               --          ORDER BY "Shift ID") 
               --       ELSE UNI_0."Shift - Break Seconds"   END                        AS "Shift Seconds"

                      ,UNI_0."Shift - Break Seconds"                                    AS "Shift Seconds"    --mod 2024/09/09 yyy

                  ,SUM(UNI_0."Shift - Break Seconds") OVER (PARTITION BY LOCATION_DIM_FK,"Employee ID" ,"Fiscal Day" 
                        ORDER BY "Shift ID")                                                                                                                                                                                            AS "Running Total Each Fiscal Day Seconds"
                 FROM (
                      SELECT  UNI."Support ID"                                       AS "Support ID" 
                    --status, category, level------------------------------------------------------------------
                    --geography--------------------------------------------------------------------------------
                        ,MAX(UNI."Location")                                         AS "Location"
                        ,MAX(UNI."Organization")                                     AS "Organization"
                    -- --dates-----------------------------------------------------------------------------------
                        ,MAX(UNI."Time Zone")                                        AS "Time Zone"
                        ,MAX(UNI."Year")                                             AS "Year"
                        ,MAX(UNI."Year and Month")                                   AS "Year and Month"
                        ,MAX(UNI."Clocked In At")                                    AS "Clocked In At"
                        ,MAX(UNI."Clocked Out At")                                   AS "Clocked Out At"    
                        ,MAX(UNI."Day of Week")                                      AS "Day of Week"
                        ,MAX(UNI."Is Weekend")                                       AS "Is Weekend"
                        ,MAX(UNI."Week Offset")                                      AS "Week Offset"
                        ,MAX(UNI."Fiscal Week")                                      AS "Fiscal Week"                 
                        ,MAX(UNI."Fiscal Day")                                       AS "Fiscal Day"    
                    -- --flags--------------------------------------------------------------------------------------
                        ,MAX(UNI."Is Clocked Out")                                   AS "Is Clocked Out"
                        ,MAX(UNI."Was System Clock Out")                             AS "Was System Clock Out"
                        ,MAX(UNI."Gets Paid Break")                                  AS "Gets Paid Break"
                        ,CASE WHEN MAX(UNI."Overtime Rule") IS NULL THEN FALSE ELSE TRUE END                     
                                                                                     AS "Gets Overtime"        
                    -- --people------------------------------------------------------------------------------------- 
                        ,MAX(UNI."Employee")                                         AS "Employee"
                        ,MAX(UNI."Employee ID")                                      AS "Employee ID"
                        ,MAX(UNI.JOBPOSITION_DIM_FK)                                 AS JOBPOSITION_DIM_FK
                        ,MAX(UNI.LOCATION_DIM_FK)                                    AS LOCATION_DIM_FK       
                    --  --Descriptors-------------------------------------------------------------------------------- 
                        ,MAX(UNI."Job Position")                                     AS "Job Position"  --(i.e. Bartender, Cook, Server)   
                        ,MAX(UNI."Pay Basis")                                        AS "Pay Basis"
                        ,MAX(UNI."Shift ID")                                         AS "Shift ID"  
                    -- Facts-----------------------------------------------------------------------------------------
                        ,IFNULL(MAX(UNI."Overtime Rule"),''None'')                     AS "Overtime Rule"
                        ,MAX(UNI."Hours Per Day")                                    AS "Hours Per Day"
                        ,MAX(UNI."Hours Per Week")                                   AS "Hours Per Week"
                        ,MAX(UNI."Shift Count")                                      AS "Shift Count"
                        ,MAX(UNI."Break Count")                                      AS "Break Count" 

                        ,MAX(UNI."Shift Per Day Count")                              AS "Shift Per Day Count"
          
                        ,IFNULL(MAX(UNI."Break Seconds"),0)                          AS "Break Seconds"
                        ,MAX(UNI."Overtime Rate")                                    AS "Overtime Rate"
                        ,MAX(UNI."Regular Rate")                                     AS "Regular Rate"
                        ,MAX(UNI."Seconds Per Week")                                 AS "Seconds Per Week"
                        ,MAX(UNI."Seconds Per Day")                                  AS "Seconds Per Day"                    
                        ,MAX(UNI."Shift Seconds")                                    AS "Shift Total Seconds"
                        ,MAX(UNI."Shift Seconds") 
                           - IFF(MAX(UNI."Gets Paid Break"),IFNULL(MAX(UNI."Break Seconds"),0),0)                                    
                                                                                     AS "Shift - Break Seconds"
                        FROM (
                            SELECT * FROM SHIFT_CTE 
                              UNION
                            SELECT * FROM BREAK_CTE  
                              UNION
                            SELECT * FROM RULE_CTE
                
                        )                                                   UNI
                        GROUP BY "Support ID"
                                                                             ) UNI_0
                   
                                                                                )  UNI_1
                                                                                    
                                                                                        )UNI_2

  -- WHERE UNI_2."Fiscal Day"::date
  --     >= :startdate::date --return only selected dates with ot calculated for a full fiscal week
  --   AND UNI_2."Fiscal Day"::date
  --     <= :enddate::date 

;

--====================================================================================================================
CREATE TEMP TABLE TEMP_LABOR AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
             
--====================================================================================================================  
--get tip and gratuity data
SELECT 
    TO_CHAR(INLT1."Support ID")                                    AS "Support ID"
    ,''TGR-'' ||row_number() over (order by INLT1."Support ID")      AS "Detail ID"
    ,IFNULL(INLT1.LOCATIONNAME,''None'')                             AS "Location"
    ,INLT1.TZ_NAME                                                 AS "Time Zone"
    ,TO_CHAR(DATE_PART(YEAR,INLT1.FISCAL_DATE))                    AS "Year"
    ,LEFT(TO_CHAR(INLT1.FISCAL_DATE),7)                            AS "Year and Month"
    ,NULL                                                          AS "Clocked In At"
    ,NULL                                                          AS "Clocked Out At"
    ,NULL                                                          AS "Clock In Time"
    ,NULL                                                          AS "Clock Out Time"
    ,IFNULL(DAYNAME(INLT1.FISCAL_DATE::DATE),''None'')               AS "Day of Week"
    ,CASE WHEN DAYNAME(INLT1.FISCAL_DATE::DATE) IN (''Sat'',''Sun'')  
                THEN TRUE ELSE FALSE END                           AS "Is Weekend" 
    ,YEAR(TIMESTAMPADD(DAY,INLT1.START_OF_PAYROLL_WEEK_INT,INLT1.FISCAL_DATE::DATE)) || ''-'' ||
     WEEKOFYEAR(TIMESTAMPADD(DAY,INLT1.START_OF_PAYROLL_WEEK_INT,INLT1.FISCAL_DATE::DATE))    
                                                                   AS "Fiscal Week"
    ,INLT1.FISCAL_DATE                                             AS "Fiscal Day"
    ,NULL                                                          AS "Is Clocked Out"
    ,NULL                                                          AS "Was System Clock Out"
    ,INLT1.EMPLOYEE_NAME                                           AS "Employee"
    ,NULL                                                          AS "Job Position"
    ,NULL                                                          AS "Pay Basis"
    ,''Shift '' || to_char(INLT1.SHIFT)                              AS "Shift ID"
    ,NULL                                                          AS "Overtime Rule"
    ,NULL::NUMBER(18,2)                                            AS "Overtime Rate"
    ,NULL::NUMBER(18,2)                                            AS "Regular Rate"
    ,NULL::NUMBER(18,0)                                            AS "Shift Seconds"
    ,NULL::NUMBER(18,0)                                            AS "Break Seconds"
    ,NULL::NUMBER(18,0)                                            AS aily
    ,NULL::NUMBER(18,0)                                            AS "Daily Overtime Rule Seconds"
    ,NULL::NUMBER(18,0)                                            AS "Regular Seconds"
    ,NULL::NUMBER(18,0)                                            AS "Overtime Seconds"
    ,NULL                                                          AS "Regular D H:M:S"
    ,NULL                                                          AS "Overtime D H:M:S"
    ,INLT1.TIP::NUMBER(18,2)                                       AS "Tips"
    ,INLT1.GRATUITIES::NUMBER(18,2)                                AS "Gratuities"
    -------------------------------------------------------------------------------------------- 
    FROM (
    SELECT  TO_CHAR(MAX(chk.CHEQUE_FACT_NK))                      AS "Support ID"
           ,CHK.FISCAL_DATE                                       AS FISCAL_DATE
           ,EMD.EMPLOYEE_NAME                                     AS EMPLOYEE_NAME
           ,LOC.TZ_NAME                                           AS TZ_NAME
           ,LOC.LOCATIONNAME                                      AS LOCATIONNAME
           ,SHD.SHIFT                                             AS SHIFT
           ,MAX(ORG.START_OF_PAYROLL_WEEK_INT)                    AS START_OF_PAYROLL_WEEK_INT
           ,SUM(CHK.TIP)::NUMBER(18,2)                            AS TIP
           ,SUM(CHK.GRATUITIES)::NUMBER(18,2)                     AS GRATUITIES

        FROM DATAWAREHOUSE.CHEQUE_FACT                                chk
             INNER JOIN DATAWAREHOUSE.LOCATION_DIM                    loc
                ON chk.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
                    AND loc.DW_ISCURRENTROW
                    AND chk.STATUS = ''Closed''
                    AND NOT chk.IS_TRAINING
                    AND chk.DW_ISCURRENTROW  
                    AND NOT chk.DW_ISDELETED
                    AND NOT chk.IS_TRAINING
                    AND chk.FISCAL_DATE::date   >= :startdate::date
                    AND chk.FISCAL_DATE::date   <= :enddate::date
                    AND chk.LOCATION_DIM_FK IN ( SELECT table1.value 
                           FROM table(split_to_table(:locationidS, '',''))  table1)
              INNER JOIN DATAWAREHOUSE.ORGANIZATION_DIM                ORG
                    ON LOC.ORGANIZATION_DIM_FK = ORG.ORGANIZATION_DIM_NK
                      AND ORG.DW_ISCURRENTROW
              INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM                     emd
                    ON emd.EMPLOYEE_DIM_NK = chk.EMPLOYEE_DIM_FK
                       AND emd.DW_ISCURRENTROW  
              LEFT JOIN DATAWAREHOUSE.SHIFT_DIM                         shd
                    ON shd.SHIFT_DIM_NK = chk.shift_dim_fk
                        AND shd.dw_iscurrentrow
                        
             GROUP BY CHK.FISCAL_DATE
                   ,EMD.EMPLOYEE_NAME
                   ,LOC.TZ_NAME
                   ,LOC.LOCATIONNAME
                   ,SHD.SHIFT
    ) INLT1        
;
--====================================================================================================================
CREATE TEMP TABLE TEMP_TIPGRAT AS
             SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); 

--====================================================================================================================
reportSet := (    
  SELECT MIN("Support ID")                    AS "Support ID"
        ,MIN("Detail ID")                     AS "Detail ID"
        ,"Location"                           AS "Location"
        ,MAX("Time Zone")                     AS "Time Zone"
        ,MAX("Year")                          AS "Year"
        ,MAX("Year and Month")                AS "Year and Month"
        ,to_char(MAX("Clocked In At"))        AS "Clocked In At"
        ,to_char(MAX("Clocked Out At"))       AS "Clocked Out At"
        ,to_char(MAX("Clock In Time"))        AS "Clock In Time"
        ,to_char(MAX("Clock Out Time"))       AS "Clock Out Time"
        ,MAX("Day of Week")                   AS "Day of Week"
        ,MAX("Is Weekend")                    AS "Is Weekend"
        ,MAX("Fiscal Week")                   AS "Fiscal Week"
        ,to_char(MAX("Fiscal Day"))           AS "Fiscal Day"
        ,MAX("Is Clocked Out")                AS "Is Clocked Out"
        ,MAX("Was System Clock Out")          AS "Was System Clock Out"
        ,"Employee"                           AS "Employee"
        ,MAX("Job Position")                  AS "Job Position"
        ,MAX("Pay Basis")                     AS "Pay Basis"
        ,"Shift ID"                           AS "Shift ID"       
        ,MAX("Overtime Rule")                 AS "Overtime Rule"
        ,MAX("Overtime Rate")                 AS "Overtime Rate"
        ,MAX("Regular Rate")                  AS "Regular Rate"
        ,SUM("Shift Seconds")::NUMBER(18,0)   AS "Shift Seconds"
        ,SUM("Break Seconds")::NUMBER(18,0)   AS "Break Seconds"
        ,MAX("Weekly Overtime Rule Seconds")  AS "Weekly Overtime Rule Seconds"
        ,MAX("Daily Overtime Rule Seconds")   AS "Daily Overtime Rule Seconds" 
        ,SUM("Regular Seconds")::NUMBER(18,0) AS "Regular Seconds"
        ,SUM("Overtime Seconds")::NUMBER(18,0)AS "Overtime Seconds"
        ,MAX("Regular D H:M:S")               AS "Regular D H:M:S"
        ,MAX("Overtime D H:M:S")              AS "Overtime D H:M:S"
        ,SUM("Tips") ::NUMBER(18,2)           AS "Tips"
        ,SUM("Gratuities")::NUMBER(18,2)      AS "Gratuities" 
    FROM (
  SELECT * FROM TEMP_LABOR
    UNION
  SELECT * FROM TEMP_TIPGRAT where "Tips" <> 0.0 or "Gratuities" <> 0.00
  )  
  WHERE "Fiscal Day"::date
      >= :startdate::date --return only selected dates with ot calculated for a full fiscal week
    AND "Fiscal Day"::date
      <= :enddate::date 
  GROUP BY "Shift ID","Location","Employee"
  ORDER BY "Employee","Fiscal Day"
); 
----------------------------------------------------------------------------------------------------------------------
RETURN TABLE(reportSet);
--====================================================================================================================
END;
';