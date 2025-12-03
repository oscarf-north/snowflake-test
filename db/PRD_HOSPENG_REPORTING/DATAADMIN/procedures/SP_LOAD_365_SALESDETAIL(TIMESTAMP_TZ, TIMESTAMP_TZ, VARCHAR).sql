CREATE OR REPLACE PROCEDURE "SP_LOAD_365_SALESDETAIL"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR(16777216))
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet resultset;
  -- startdate timestamp_tz   := ''2025-01-16'';
  -- enddate timestamp_tz     := ''2025-01-16'';
  -- locationid string        := ''[351]'';
  locationidS string          :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
--=================================================================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_salesdetail; 
  DROP TABLE IF EXISTS TEMP_header; 

----------------------------------------------------------------------------------------------------------------------------------
SELECT ''CheckNumber''
    ,''CloseTime'' 
    ,''ItemSale_TicketItemNumber'' 
    ,''ItemSale_ItemNumber'' 
    ,''ItemSale_GrossAmount''
    ,''ItemSale_Modifiers_ItemNumber'' 
    ,''ItemSale_Modifiers_Quantity''
    ,''ItemSale_Modfiers_GrossAmount''
    ,''ItemSale_Comp_CompName''
    ,''ItemSale_Comp_Amount''
    ,''ItemSale_Promo_PromoName''
    ,''ItemSale_Promo_Amount''
    ,''ItemSale_RevenueCenterName''
    ,''ItemSale_Quantity''
;

 CREATE TEMP TABLE TEMP_header AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  
----------------------------------------------------------------------------------------------------------------------------------
SELECT to_char(CHK.chequenumber) || ''.'' || to_char(CHK.cheque_fact_nk)               
                                                                   AS CheckNumber --* string
    ,TO_CHAR(TO_TIMESTAMP(CHK.CLOSED_AT),''MM/DD/YYYY HH24:MI:SS'')  AS CloseTime  --* DateTime(mm/dd/yyyy hh:mm:ss)
    ,TO_CHAR(row_number() OVER (PARTITION BY CHK.CHEQUE_FACT_NK ORDER BY itf.ITEM_FACT_NK)::NUMBER(18,0))                                             
                                                                   AS ItemSale_TicketItemNumber  --* integer  
    ,TO_CHAR(itf.MENUITEMNAME_DIM_FK)                              AS ItemSale_ItemNumber  --* string 
    ,TO_CHAR((itf.GROSS -  IFNULL(itf.INCLUSIVETAX,0)) ::DECIMAL(18,2))     
                                                                   AS ItemSale_GrossAmount --* decimal
    ,TO_CHAR(1)                                                    AS ItemSale_Modifiers_ItemNumber --**string this is an fk 
    ,TO_CHAR(0)                                                    AS ItemSale_Modifiers_Quantity --int
    ,TO_CHAR(0)                                                    AS ItemSale_Modfiers_GrossAmount --** int
    ,''None''                                                        AS ItemSale_Comp_CompName --* string name of discount  
    ,TO_CHAR(0.00)                                                 AS ItemSale_Comp_Amount  --*decimal amount subtracted from tota
    ,''None''                                                        AS ItemSale_Promo_PromoName --*string  promo name
    ,TO_CHAR(itf.DiscountItem::decimal(18,2))                      AS ItemSale_Promo_Amount --*amount subtracted from total 
    ,IFNULL(REPLACE(itf.REVENUECENTERNAME,'','',''''),''None'')          AS ItemSale_RevenueCenterName  --string
    ,TO_CHAR(IFNULL(itf.QUANTITY,0)::number(18,0))                 AS ItemSale_Quantity           --int

    FROM DATAWAREHOUSE.ITEM_FACT                                itf
     INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                  med
       ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND itf.CHECKSTATUS IN (''Closed'')
          AND itf.DW_ISCURRENTROW  
          AND med.DW_ISCURRENTROW  
          AND NOT itf.DW_ISDELETED
          AND NOT itf.IS_TRAINING
          AND itf.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                          chk
        ON chk.CHEQUE_FACT_NK = itf.CHEQUE_FACT_FK
          AND chk.DW_ISCURRENTROW
          AND chk.STATUS in (''Closed'')
          AND itf.ITEMSTATUS IN (''Added'',''Sent'')
          AND chk.UNPAID = 0
          AND chk.CLOSED_AT is not null
          AND (chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date  <= :enddate::date)
      INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM                    meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW = TRUE
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                      ccd
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW         
     ORDER BY CHK.CLOSED_AT DESC;

   CREATE TEMP TABLE TEMP_salesdetail AS
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
--=================================================================================================================================
reportSet := (  
  select * from TEMP_header
    UNION ALL
  select * from TEMP_salesdetail
--=================================================================================================================================
);
RETURN TABLE(reportSet); 
END';