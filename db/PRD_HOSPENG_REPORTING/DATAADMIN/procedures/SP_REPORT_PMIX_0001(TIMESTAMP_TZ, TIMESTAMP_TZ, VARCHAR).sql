CREATE OR REPLACE PROCEDURE "SP_REPORT_PMIX_0001"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS '
-- -- =====================================================================================
--Example Call Statement
--CALL DATAADMIN.SP_REPORT_PMIX_0001(''2000-11-20T14:48:37.661Z'',''2023-11-20T14:48:37.661Z'',''[351,352]'');
-- =======================================================================================
-- --NOTE:  Convert to local timezone.
-- --NOTE:  Split checks - report quantity - what about an item that was split, but one split check was -- --=========================================================================================
DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz := ''2000-11-20T14:48:37.661Z'';  
  -- enddate timestamp_tz   := ''2023-11-20T14:48:37.661Z''; 
  -- locationid string      := ''[351,352]'';
  locationidS string     :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=========================================================================================
BEGIN
 reportSet:= (
SELECT itf.ITEM_FACT_NK                                   as "Support ID" 
    , ''PMIX-'' ||row_number() over (order by itf.ITEM_FACT_NK) 
                                                          as "Detail ID"
--status, category, level-------------------------------------------------------------------
    ,IFNULL(itf.CHECKSTATUS,''None'')                       as "Check Status"
    ,IFNULL(itf.ITEMSTATUS,''None'')                        as "Item Status"
    ,IFNULL(itf.STATUSREASON,''None'')                      as "Status Reason"
    ,IFNULL(meg.REPORTCATEGORY,''None'')                    as "Category"
-- --geography--------------------------------------------------------------------------------
    ,IFNULL(loc.LOCATIONNAME,''None'')                      as "Location"
    ,IFNULL(itf.REVENUECENTERNAME,''None'')                 as "Revenue Center"
-- --dates-------------------------------------------------------------------------------------
    ,LOC.TZ_NAME                                          as "Time Zone"
    ,to_char(LEFT(
    CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,itf.OPENED_AT::timestamp_ntz )
    ,4))                                                  as "Year"
    ,to_char(LEFT(
    CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,itf.OPENED_AT::timestamp_ntz )
    ,7))                                                  as "Year and Month"
    ,IFNULL(dad.DAYPART,''None'')                           as "Daypart"
    ,CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,itf.OPENED_AT::timestamp_ntz )                                        as "Opened At"
    ,IFNULL(DAYNAME(
    CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,itf.OPENED_AT::timestamp_ntz )
    ),'' None'') 
                                                          as "Day of Week"
    ,CASE WHEN DAYNAME(
    CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,itf.OPENED_AT::timestamp_ntz )
    ) IN (''Sat'',''Sun'')  
    THEN TRUE ELSE FALSE END                              as "Is Weekend"
-- --flags--------------------------------------------------------------------------------------
    ,IFNULL(itf.HASMODIFIERS,FALSE)                       as "Has Modifiers"
-- --people-------------------------------------------------------------------------------------
    ,IFNULL(emd.EMPLOYEE_NAME,''None'')                     as "Employee"
-- --Descriptors-------------------------------------------------------------------------------- 
    ,IFNULL(itf.chequenumber ,''None'')                     as "Check"
    ,IFNULL(med.MENUITEMNAME,'' None'')                     as "Menu Item"
    ,IFNULL(itf.NOTE,''None'')                              as "Note"
    ,IFNULL(itf.DESCRIPTION,''None'')                       as "Description"
    ,IFNULL(vad.VARIANT,''None'')                           as "Variant"     
    ,itf.SPLITBY                                          as "Split By"
-- --Facts-----------------------------------------------------------------------------------------
    ,1::NUMBER(18,0)                                      as "Count"
    ,itf.REPORTQUANTITY::NUMBER(18,2)                     as "Report Quantity"                   
    ,itf.BASEPRICE::NUMBER(18,2)                          as "Base Price"
    ,itf.INCLUSIVETAX::NUMBER(18,2)                       as "Inclusive Tax"
    ,itf.PRICE::NUMBER(18,2)                              as "Price"
    ,itf.GROSS::NUMBER(18,2)                              as "Gross"  
    ,itf.NET::NUMBER(18,2)                                as "Net"  
    ,itf.TAX::NUMBER(18,2)                                as "Tax"
    ,itf.TOTAL::NUMBER(18,2)                              as "Total" 
--------------------------------------------------------------------------------------------   
FROM DATAADMIN.ITEM_FACT                                  itf
     INNER JOIN DATAADMIN.MENUITEMNAME_DIM                med
       ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND NOT itf.CHECKSTATUS = ''Voided''
        AND itf.OPENED_AT is not null
          AND itf.DW_ISCURRENTROW  
          AND med.DW_ISCURRENTROW  
          AND NOT itf.DW_ISDELETED
          AND NOT itf.IS_TRAINING
          AND itf.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAADMIN.LOCATION_DIM                     loc
        ON itf.LOCATION_DIM_FK = loc.LOCATION_DIM_NK
          AND loc.DW_ISCURRENTROW
          AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,OPENED_AT::timestamp_ntz ) > CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:startdate::timestamp_ntz )
          AND CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,OPENED_AT::timestamp_ntz ) < CONVERT_TIMEZONE(''UTC'',LOC.TZ_NAME,:enddate::timestamp_ntz )
      INNER JOIN DATAADMIN.EMPLOYEE_DIM                     emd
        ON itf.employee_dim_fk = emd.employee_dim_pk
          AND emd.DW_ISCURRENTROW 
      INNER JOIN DATAADMIN.DAYPART_DIM                      dad
        ON itf.daypart_dim_fk = dad.daypart_dim_pk
          AND dad.DW_ISCURRENTROW = TRUE         
      INNER JOIN DATAADMIN.REPORTCATEGORY_DIM               meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND med.DW_ISCURRENTROW = TRUE
      INNER JOIN DATAADMIN.VARIANT_DIM                      vad
        ON itf.VARIANT_DIM_FK = vad.variant_dim_nk
          AND vad.dw_iscurrentrow
      ORDER BY itf.OPENED_AT
--==========================================================================================
);
RETURN TABLE(reportSet); 
END;
-- ';