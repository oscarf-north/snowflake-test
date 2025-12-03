CREATE OR REPLACE PROCEDURE "SP_REPORT_VOID_0001_DEV"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2024-09-29T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-09-29T14:48:37.661Z''; 
  -- locationid string      := ''[26]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
-- --===================================================================================================================
BEGIN

-------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS TEMP_ITEM;

-------------------------------------------------------------------------------------------------------------------------
SELECT iaf.ITEM_FACT_NK                                             AS ITEM_FACT_NK
   ,iaf.CHEQUE_FACT_FK                                              AS CHEQUE_FACT_FK
   ,max(vdr_item.VOIDREASON)                                        AS VOIDREASON
   ,max(iaf.PRICE)                                                  AS ITEMPRICE
   ,sum(imf.PRICE)                                                  AS MODIFIERPRICE
   ,(max(case when iaf.PRICE > 0.0000 THEN iaf.PRICE ELSE iaf.baseprice END)
     + sum(ifnull(imf.PRICE,0))) * max(iaf.QUANTITY)                AS PRICE
        FROM (select * FROM DATAWAREHOUSE.ITEM_FACT where dw_iscurrentrow AND CHECKSTATUS <> ''MergeVoided'' AND IS_VOID AND NOT DW_ISDELETED AND NOT  IS_TRAINING  )                              iaf
           LEFT JOIN  DATAWAREHOUSE.ITEMMODIFIER_DIM                imf
              ON iaf.ITEM_FACT_NK = imf.ITEM_FACT_FK
                  -- AND iaf.LOCATION_DIM_fK = 2
                AND iaf.LOCATION_DIM_fK IN (
                    SELECT table1.value 
                      FROM table(split_to_table(:locationidS, '',''))  table1)
                -- AND iaf.FISCAL_DATE = ''2024-09-29''
                AND iaf.FISCAL_DATE::date
                    >= :startdate::date 
                AND iaf.FISCAL_DATE::date  
                   <= :enddate::date 
                AND iaf.IS_VOID
                AND iaf.CHECKSTATUS <> ''MergeVoided''
                AND iaf.DW_ISCURRENTROW 
                AND imf.DW_ISCURRENTROW 
                AND NOT iaf.DW_ISDELETED
                AND NOT iaf.IS_TRAINING
        LEFT JOIN DATAWAREHOUSE.VoidReason_DIM                        vdr_item
             ON vdr_item.VOIDREASON_DIM_NK = iaf.VoidReason_DIM_FK
               AND vdr_item.DW_ISCURRENTROW        
GROUP BY iaf.item_fact_nk,iaf.cheque_fact_fk 
;
-- select * from TEMP_ITEM where CHEQUE_FACT_FK = 243922; and item_fact_nk in (''243922.ac8be0dd-dbf7-475d-a6c5-30615561e208 = 29.95'',''243922.b8526d5c-d26c-422b-83e2-d50ea4fab464 = 59.80'');

     --item = 243922.ac8be0dd-dbf7-475d-a6c5-30615561e208 = 29.95
     --item = 243922.b8526d5c-d26c-422b-83e2-d50ea4fab464 = 59.80
-------------------------------------------------------------------------------------------------------------------------
 CREATE TEMP TABLE TEMP_ITEM AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  
-------------------------------------------------------------------------------------------------------------------------
 reportSet   := (
        SELECT to_char(act.activity_fact_nk)        AS "Support ID" 
          ,''VOID-'' ||row_number() over (order by act.activity_fact_nk) 
                                                    AS "Detail ID"        
        --status, category, level-----------------------------------------------------------------------------------------
        ,CASE act.TYPE 
          WHEN ''ItemVoided'' 
            THEN ''Item'' 
          WHEN ''Voided'' 
            THEN ''Check''
            ELSE ''None'' 
          END 
                                                 AS "Level"          --Values of “Check” or “Item”.
        --geography--------------------------------------------------------------------------------------------------------
        ,IFNULL(LOC.LOCATIONNAME ,''None'')        AS "Location"
        ,LOC.LOCATION_DIM_NK                     AS "Location ID"        
        ,IFNULL(CHK.revenueCenterName ,''None'')   AS "Revenue Center"
        --dates------------------------------------------------------------------------------------------------------------- 
        ,LOC.TZ_NAME                                                               AS "Time Zone"
        ,to_char(LEFT(chk.FISCAL_DATE,4))                                          AS "Year"
        ,to_char(YEAR(chk.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(chk.FISCAL_DATE),2))                                       
                                                                                   AS "Year and Month"
        ,IFNULL(dad.DAYPART,''None'')                                                AS "Daypart"
        ,to_char(chk.FISCAL_DATE)                                                  AS "Fiscal Date"
        
        --,chk.OPENED_AT::timestamp_ntz 
         ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHK.OPENED_AT::timestamp_ntz )::timestamp )
                                                                                   AS "Check Opened At"     
        --,chk.CREATED_AT::timestamp_ntz 
        ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,CHK.CREATED_AT::timestamp_ntz )::timestamp) 
                                                                                   AS "Item Ordered At"    
        -- ,act.PERFORMED_AT::timestamp_ntz 
        ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz )::timestamp) 
                                                                                   AS "Voided At"
        
        ,dad.DAYPART                                                               AS "Day Part"              
        ,IFNULL(DAYNAME(chk.FISCAL_DATE),''None'')                                   AS "Day of Week"
        ,CASE WHEN DAYNAME(chk.FISCAL_DATE) IN (''Sat'',''Sun'')  
           THEN TRUE ELSE FALSE END                                                AS "Is Weekend"
        
        ,TIMESTAMPDIFF(second ,chk.OPENED_AT,act.PERFORMED_AT)::NUMBER(18,0) 
                                                                                   AS "Seconds to Void"
        ,TIMESTAMPDIFF(minute ,chk.OPENED_AT,act.PERFORMED_AT) ::NUMBER(18,0)
                                                                                   AS "Minutes to Void"                                                 
        --Flags---------------------------------------------------------------------------------------------------------
        --People--------------------------------------------------------------------------------------------------------
        ,IFNULL(amd_asperf.EMPLOYEE_NAME,''None'') AS "Voider"
        ,IFNULL(amd_asapp.EMPLOYEE_NAME,''None'')  AS "Approver"   
        --Descriptors----------------------------------------------------------------------------------------------------
        ,chk.CHEQUENUMBER                        AS "Check"
        ,chk.CHEQUE_FACT_NK                      AS "Check ID"
        ,IFNULL(COALESCE(itf.VOIDREASON,
         vdr_check.VOIDREASON),''None'')           AS "Reason"
        
          
        ,IFNULL(act.MENUITEMNAME,''None'')         AS "Item"
        ,itf.ITEM_FACT_NK                        AS "Item ID"    

        --Facts-----------------------------------------------------------------------------------------------------------
        ,1::NUMBER(18,0)                         AS "Count"  
        ,CASE WHEN act.TYPE = ''ItemVoided'' 
            THEN itf.PRICE 
         -- ELSE chk.NET END::NUMBER(18,2)
          ELSE chk.GROSS END::NUMBER(18,2)
                                                 AS "Amount"                   
        FROM DATAWAREHOUSE.ACTIVITY_FACT                                  act
           INNER JOIN  DATAWAREHOUSE.LOCATION_DIM                         loc
              ON act.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
                AND act.LOCATION_DIM_fK IN (
                    SELECT table1.value 
                      FROM table(split_to_table(:locationidS, '',''))  table1)
                AND loc.DW_ISCURRENTROW 
                AND NOT loc.DW_ISDELETED
            INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                          chk
              ON act.CHEQUE_FACT_FK = chk.CHEQUE_FACT_NK
                AND act.IS_VOID
                AND act.DW_ISCURRENTROW 
                AND chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date <= :enddate::date  
                AND NOT act.DW_ISDELETED
                AND chK.DW_ISCURRENTROW 
                AND NOT chk.DW_ISDELETED
                AND NOT chk.IS_TRAINING
            INNER JOIN DATAWAREHOUSE.DAYPART_DIM                          dad
              ON chk.DAYPART_DIM_FK = dad.daypart_dim_Nk
                AND dad.DW_ISCURRENTROW 
                AND NOT dad.DW_ISDELETED
           
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                          amd_asperf
              ON act.EMPLOYEE_DIM_FK_AS_PERFORMING_EMPLOYEE 
                  = amd_asperf.EMPLOYEE_DIM_NK
                AND amd_asperf.DW_ISCURRENTROW 
                AND NOT amd_asperf.DW_ISDELETED 
            LEFT JOIN DATAWAREHOUSE.EMPLOYEE_DIM                          amd_asapp
              ON act.EMPLOYEE_DIM_FK_AS_APPROVING_EMPLOYEE 
                  = amd_asapp.EMPLOYEE_DIM_NK
                AND amd_asapp.DW_ISCURRENTROW 
                AND NOT amd_asapp.DW_ISDELETED   
            LEFT JOIN DATAWAREHOUSE.VoidReason_DIM                        vdr_check
             ON vdr_check.VOIDREASON_DIM_NK = CHK.VoidReason_DIM_FK
               AND vdr_check.DW_ISCURRENTROW 
               AND NOT vdr_check.DW_ISDELETED
            LEFT JOIN TEMP_ITEM                                              itf
              ON itf.Item_fact_NK = act.Item_fact_fk 
            -- LEFT JOIN DATAWAREHOUSE.ITEM_FACT                             itf
            --  ON itf.Item_fact_NK = act.Item_fact_fk
            --    AND itf.DW_ISCURRENTROW 
            --    AND NOT itf.DW_ISDELETED        
            -- LEFT JOIN DATAWAREHOUSE.VoidReason_DIM                        vdr_item
            --  ON vdr_item.VOIDREASON_DIM_NK = itf.VoidReason_DIM_FK
            --    AND vdr_item.DW_ISCURRENTROW 
            --    AND NOT vdr_item.DW_ISDELETED     
--=================================================================================================================
); 
 RETURN TABLE(reportSet); 
END';