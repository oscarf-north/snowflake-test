CREATE OR REPLACE PROCEDURE "SP_REPORT_VOID"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- ====================================================================================================================
--Example Call Statement
--CALL DATAADMIN.SP_REPORT_VOID(''2000-01-20T14:48:37.661Z'',''2023-11-20T14:48:37.661Z'',''[351,352]'');
-- ====================================================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--======================================================================================================
--ISSUE 1:  Activity price is void for item only...For a check void, is the void amount the net amount, gross amount???
--ISSUE 4:  How to Cast to local time?
--QUESTION 2:  If time passed between order/open and void...do we want to highlight that?  top 10 log discrepency?
--QUESTION 1:  Can we put the reason id onto the activity?  This would mean 3 fewer joins
-- --==========================================================================================
BEGIN
 reportSet   := (
        SELECT act.activity_fact_nk                AS "Support ID" 
        --status, category, level----------------------------------------------------------------------
        ,CASE act.TYPE 
          WHEN ''ItemVoided'' 
            THEN ''Item'' 
          WHEN ''Voided'' 
            THEN ''Check''
            ELSE ''None'' 
          END 
                                                 AS "Level"          --Values of “Check” or “Item”.
        --geography------------------------------------------------------------------------------------
        ,IFNULL(LOC.LOCATIONNAME ,''None'')        AS "Location"
        ,IFNULL(CHK.revenueCenterName ,''None'')   AS "Revenue Center"
        --dates----------------------------------------------------------------------------------------     
        ,to_char(LEFT(
        CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz )
        ,4))       AS "Year"
        ,to_char(LEFT(
        CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz )
        ,7))       AS "Year and Month"
        ,IFNULL(dad.DAYPART,''None'')              AS "Daypart"
        ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,chk.FISCAL_DATE::timestamp_ntz )       AS "Fiscal Date"
        ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,chk.OPENED_AT::timestamp_ntz )         AS "Check Opened At"     
        ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,chk.CREATED_AT::timestamp_ntz )        AS "Item Ordered At"    
        ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz )      AS "Voided At"          
       ,dad.DAYPART                                                                AS "Day Part"              
        ,IFNULL(DAYNAME(
        CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz )
        ),''None'')                               AS"Day of Week"
        ,CASE WHEN DAYNAME(
        CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz )
        ) IN (''Sat'',''Sun'')  
        THEN TRUE ELSE FALSE END                 AS "Is Weekend"
        
        ,TIMESTAMPDIFF(second ,chk.OPENED_AT,act.PERFORMED_AT) 
                                                 AS "Seconds to Void"
        ,TIMESTAMPDIFF(second ,chk.OPENED_AT,act.PERFORMED_AT) 
                                                 AS "Minutes to Void"                                                 
        --Flags-------------------------------------------------------------------------------------
        --People------------------------------------------------------------------------------------
        ,IFNULL(amd_asperf.EMPLOYEE_NAME,''None'') AS "Voider"
        ,IFNULL(amd_asapp.EMPLOYEE_NAME,''None'')  AS "Approver"   
        --Descriptors---------------------------------------------------------------------------------
        ,chk.CHEQUENUMBER                        AS "Check"
        ,IFNULL(COALESCE(vdr_item.VOIDREASON,
         vdr_check.VOIDREASON),''None'')           AS "Reason"
        
          
        ,IFNULL(act.MENUITEMNAME,''None'')         AS "Item"
        --Facts---------------------------------------------------------------------------------------
        ,1::NUMBER(18,0)                         AS "Count"                 
        ,CASE WHEN act.TYPE = ''ItemVoided'' 
            THEN act.PRICE 
          ELSE chk.TOTAL END::NUMBER(18,2)
                                                 AS "Amount"           
        FROM DATAADMIN.ACTIVITY_FACT                                  act
           INNER JOIN  DATAADMIN.LOCATION_DIM                         loc
              ON act.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
                AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz ) 
                    > CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:startdate::timestamp_ntz ) 
                AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,act.PERFORMED_AT::timestamp_ntz ) 
                    < CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:enddate::timestamp_ntz ) 
                AND act.LOCATION_DIM_fK IN (
                    SELECT table1.value 
                      FROM table(split_to_table(:locationidS, '',''))  table1)
                AND loc.DW_ISCURRENTROW 
                AND NOT loc.DW_ISDELETED
            INNER JOIN DATAADMIN.CHEQUE_FACT                          chk
              ON act.CHEQUE_FACT_FK = chk.CHEQUE_FACT_NK
                AND act.IS_VOID
                AND act.DW_ISCURRENTROW 
                AND NOT act.DW_ISDELETED
                AND chK.DW_ISCURRENTROW 
                AND NOT chk.DW_ISDELETED
                AND NOT chk.IS_TRAINING
            INNER JOIN DATAADMIN.DAYPART_DIM                           dad
              ON chk.DAYPART_DIM_FK = dad.daypart_dim_pk
                AND dad.DW_ISCURRENTROW 
                AND NOT dad.DW_ISDELETED
           
            LEFT JOIN EMPLOYEE_DIM                                    amd_asperf
              ON act.EMPLOYEE_DIM_FK_AS_PERFORMING_EMPLOYEE 
                  = amd_asperf.EMPLOYEE_DIM_NK
                AND amd_asperf.DW_ISCURRENTROW 
                AND NOT amd_asperf.DW_ISDELETED 
            LEFT JOIN EMPLOYEE_DIM                                    amd_asapp
              ON act.EMPLOYEE_DIM_FK_AS_APPROVING_EMPLOYEE 
                  = amd_asapp.EMPLOYEE_DIM_NK
                AND amd_asapp.DW_ISCURRENTROW 
                AND NOT amd_asapp.DW_ISDELETED   
            LEFT JOIN DATAADMIN.VoidReason_DIM                        vdr_check
             ON vdr_check.VOIDREASON_DIM_NK = CHK.VoidReason_DIM_FK
               AND vdr_check.DW_ISCURRENTROW 
               AND NOT vdr_check.DW_ISDELETED
            LEFT JOIN DATAADMIN.ITEM_FACT                             itf
             ON itf.Item_fact_NK = act.Item_fact_fk
               AND itf.DW_ISCURRENTROW 
               AND NOT itf.DW_ISDELETED        
            LEFT JOIN DATAADMIN.VoidReason_DIM                        vdr_item
             ON vdr_item.VOIDREASON_DIM_NK = itf.VoidReason_DIM_FK
               AND vdr_item.DW_ISCURRENTROW 
               AND NOT vdr_item.DW_ISDELETED     
--==========================================================================================
); 
 RETURN TABLE(reportSet); 
END;
';