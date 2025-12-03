CREATE OR REPLACE PROCEDURE "SP_REPORT_MODIFIER"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2020-08-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2029-08-20T14:48:37.661Z''; 
  -- locationid string      := ''[2,3,4,351]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
 reportSet:= (
SELECT mod.ITEMMODIFIER_DIM_NK                            as "Support ID" 
    , ''mod-'' ||row_number() over (order by mod.ITEMMODIFIER_DIM_NK) 
                                                          as "Detail ID"
--status, category, level-------------------------------------------------------------------
    ,IFNULL(itf.CHECKSTATUS,''None'')                       as "Check Status"
    ,IFNULL(itf.ITEMSTATUS,''None'')                        as "Item Status"
    ,IFNULL(itf.STATUSREASON,''None'')                      as "Status Reason"
    ,IFNULL(ccd.COGSCATEGORY,''None'')                      as "Category"
    -- ,IFNULL(ccd.COGSCATEGORY,''None'')                   as "COGS Category"
-- --geography--------------------------------------------------------------------------------
    ,IFNULL(loc.LOCATIONNAME,''None'')                      as "Location"
    ,chk.LOCATION_DIM_FK::DECIMAL(36,0)                   as "Location ID"    
    ,IFNULL(itf.REVENUECENTERNAME,''None'')                 as "Revenue Center"
-- --dates-------------------------------------------------------------------------------------
    ,LOC.TZ_NAME                                          as "Time Zone"
    ,to_char(LEFT(chk.FISCAL_DATE,4))                     as "Year"
    ,to_char(YEAR(chk.FISCAL_DATE)) || ''|'' || TO_CHAR(RIGHT(''0'' || MONTH(chk.FISCAL_DATE),2))                                       
                                                          as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                           as "Daypart"
    ,to_char(chk.FISCAL_DATE)                             as "Fiscal Date"
    ,to_char(CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,chk.OPENED_AT::timestamp_ntz )::timestamp )                                                                           
                                                          as "Opened At"                                                           
    ,IFNULL(DAYNAME(chk.FISCAL_DATE),''None'')              as "Day of Week"
    ,CASE WHEN DAYNAME(chk.FISCAL_DATE) IN (''Sat'',''Sun'')  
             THEN TRUE ELSE FALSE END                     as "Is Weekend"
-- --flags--------------------------------------------------------------------------------------
    ,IFNULL(itf.HASMODIFIERS,FALSE)                       as "Has Modifiers"
-- --people-------------------------------------------------------------------------------------
    ,IFNULL(emd.EMPLOYEE_NAME,''None'')                     as "Employee"
-- --Descriptors-------------------------------------------------------------------------------- 
    ,IFNULL(itf.chequenumber ,''None'')                     as "Check"
    ,itf.cheque_fact_fk                                   as "Check ID"    
    ,IFNULL(med.MENUITEMNAME,'' None'')                     as "Menu Item"
    ,itf.ITEM_FACT_NK                                     as "Item ID"
    ,IFNULL(mod.MODIFIER,''None'')                          as "Modifier"
    ,IFNULL(mod.MODIFIER_GROUP,''None'')                    as "Modifier Group"
    ,IFNULL(mod.MODIFIERGROUP_DIM_FK,''None'')              as "Modifier Group ID"    
    ,IFNULL(itf.NOTE,''None'')                              as "Note"
    ,IFNULL(itf.DESCRIPTION,''None'')                       as "Description"
    ,IFNULL(itf.COMBINEDNAME,''Regular'')                   as "Variant"     
-- --Facts---------------------------------------------------------------------------------------
    ,itf.QUANTITY::NUMBER(10,0)                           as "Count"
    ,mod.PRICE::NUMBER(18,2)                              as "Price"
    ,(itf.QUANTITY * mod.PRICE)::NUMBER(18,2)             as "Total Sales"
-------------------------------------------------------------------------------------------------   
FROM DATAWAREHOUSE.ITEM_FACT                                  itf
     INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                med
       ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND itf.CHECKSTATUS = ''Closed''
          AND itf.DW_ISCURRENTROW  
          AND med.DW_ISCURRENTROW  
          AND NOT itf.DW_ISDELETED
          AND NOT itf.IS_TRAINING
          AND itf.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                                chk
        ON chk.CHEQUE_FACT_NK = itf.CHEQUE_FACT_FK
          AND chk.DW_ISCURRENTROW
          AND chk.STATUS = ''Closed''
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND (chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date  <= :enddate::date)
                -- select * from itemmodifier_dim;
      INNER JOIN DATAWAREHOUSE.ITEMMODIFIER_DIM                           mod
        ON itf.ITEM_FACT_NK = mod.ITEM_FACT_FK
          AND mod.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.LOCATION_DIM                     loc
        ON itf.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
      INNER JOIN DATAWAREHOUSE.EMPLOYEE_DIM                     emd
        ON itf.employee_dim_fk = emd.employee_dim_Nk
          AND emd.DW_ISCURRENTROW 
      INNER JOIN DATAWAREHOUSE.DAYPART_DIM                      dad
        ON itf.daypart_dim_fk = dad.daypart_dim_NK
          AND dad.DW_ISCURRENTROW = TRUE         
      INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM               meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW = TRUE
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                  ccd
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW         
--==========================================================================================
);
RETURN TABLE(reportSet); 
END';