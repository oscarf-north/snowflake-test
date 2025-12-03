CREATE OR REPLACE PROCEDURE "SP_REPORT_ACCOUNTSUMMARY_SALES"("STARTDATE" TIMESTAMP_TZ(9), "ENDDATE" TIMESTAMP_TZ(9), "LOCATIONID" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE 
  reportSet           resultset;
  -- startdate string    := ''2025-10-05'';  
  -- enddate string      := ''2025-10-09''; 
  -- locationid string   := ''[351]'';
  locationidS string  :=  REPLACE(REPLACE(:locationid,''['',''''),'']'','''');
  
--=========================================================================================
BEGIN
  DROP TABLE IF EXISTS TEMP_SALES;
  
--=========================================================================================== 
CREATE TEMP TABLE TEMP_SALES AS
SELECT 
   itf.LOCATION_DIM_FK                                        AS "Location ID"
  ,itf.CHEQUE_FACT_FK                                         AS "Check ID"
  ,itf.ITEM_FACT_NK                                           AS "Item ID"
  ,itf.FISCAL_DATE                                            AS "Fiscal Day"
  ------Slicing Dimensions
 , ccd.COGSCATEGORY                                           AS "By Category"
 , dad.DAYPART                                                AS "By DayPart"
 , otd.ORDER_TYPE                                             AS "By Order Type"
 , itf.REVENUECENTERNAME                                      AS "By Revenue Center"
  ------Facts
  ,IFNULL(vif.VOIDAMOUNT,0)::NUMBER(36,2)                     AS "Voids"
  ,IFNULL(CASE WHEN itf.ITEMSTATUS = ''Voided'' 
    OR itf.CHECKSTATUS = ''Voided'' 
    THEN 0 ELSE itf.GROSS END,0)::NUMBER(36,2)                          
                                                              AS "Sales" 
  ,IFNULL(CASE WHEN itf.ITEMSTATUS = ''Voided'' 
    OR itf.CHECKSTATUS = ''Voided'' 
    THEN 0 ELSE IFNULL(dif.DISCOUNTCHECKAMOUNT,0) 
    + IFNULL(itf.DISCOUNTITEM,0) END,0) ::NUMBER(36,2)        AS "Discounts"
    
  ,CASE WHEN itf.ITEMSTATUS = ''Voided'' 
    OR itf.CHECKSTATUS = ''Voided'' 
    THEN 0 ELSE dif.DISCOUNTCHECKAMOUNT END ::NUMBER(36,2)    AS "Check Discounts"

  ,IFNULL(CASE WHEN itf.ITEMSTATUS = ''Voided'' 
    OR itf.CHECKSTATUS = ''Voided'' 
    THEN 0 ELSE itf.DISCOUNTITEM END ,0)::NUMBER(36,2)                   
                                                              AS "Item Discounts"
                                                              
  ,IFNULL(CASE WHEN itf.ITEMSTATUS = ''Voided'' 
    OR itf.CHECKSTATUS = ''Voided'' 
    THEN 0 ELSE rif.REFUNDAMOUNT END,0)::NUMBER(36,2)         AS "Refunds"
    
  ,IFNULL(CASE WHEN itf.ITEMSTATUS = ''Voided'' 
    OR itf.CHECKSTATUS = ''Voided'' 
    THEN 0 ELSE itf.APPLIEDAMOUNT END,0)::NUMBER(36,2)        AS "Net Sales"
  FROM DATAWAREHOUSE.ITEM_FACT                                itf  
      INNER JOIN DATAWAREHOUSE.CHEQUE_FACT                    chk
        ON chk.CHEQUE_FACT_NK = itf.CHEQUE_FACT_FK
          AND chk.STATUS in (''Closed'',''Voided'')
          AND itf.ITEMSTATUS IN (''Added'',''Sent'',''Voided'')
          AND itf.DW_ISCURRENTROW  
          AND chk.DW_ISCURRENTROW  
          AND NOT itf.DW_ISDELETED
          AND NOT itf.IS_TRAINING
          AND (chk.FISCAL_DATE::date >= :startdate::date 
                AND chk.FISCAL_DATE::date  <= :enddate::date)
          AND itf.LOCATION_DIM_FK IN (--351,352
             SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
      INNER JOIN DATAWAREHOUSE.ORDERTYPE_DIM                     otd
        ON otd.ORDERTYPE_DIM_NK = itf.ORDERTYPE_DIM_FK
          AND otd.DW_ISCURRENTROW   
      INNER JOIN DATAWAREHOUSE.DAYPART_DIM                       dad
        ON itf.daypart_dim_fk = dad.daypart_dim_nk
          AND dad.DW_ISCURRENTROW 
      INNER JOIN DATAWAREHOUSE.MENUITEMNAME_DIM                  med
         ON med.MENUITEMNAME_DIM_NK = itf.MENUITEMNAME_DIM_FK
           AND med.DW_ISCURRENTROW  
      INNER JOIN DATAWAREHOUSE.REPORTCATEGORY_DIM                meg
        ON med.REPORTCATEGORY_DIM_FK = meg.REPORTCATEGORY_DIM_NK
          AND meg.DW_ISCURRENTROW 
      INNER JOIN DATAWAREHOUSE.COGSCATEGORY_DIM                  ccd
        ON ccd.COGSCATEGORY_DIM_NK = meg.COGSCATEGORY_DIM_FK    
          AND ccd.DW_ISCURRENTROW
      LEFT JOIN (
        SELECT dcf.ITEM_FACT_FK             AS ITEM_FACT_FK
          ,SUM(dcf.ALLOCATED_ITEM_DISCOUNT) AS DISCOUNTCHECKAMOUNT  
         FROM DATAWAREHOUSE.DISCOUNTCHEQUEBYITEM_FACT  dcf                      
          WHERE dcf.DW_ISCURRENTROW
            AND NOT dcf.STATUS  = ''Disabled''
            AND CHEQUESTATUS IN (''Closed'') 
            AND (dcf.FISCAL_DATE::date >= :startdate::date AND dcf.FISCAL_DATE::date  <= :enddate::date)
             AND dcf.LOCATION_DIM_FK IN (--351,352
               SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
            GROUP BY dcf.ITEM_FACT_FK
          )                                                       dif
                ON itf.ITEM_FACT_NK = dif.ITEM_FACT_FK  
      LEFT JOIN (
        SELECT rcf.ITEM_FACT_FK             AS ITEM_FACT_FK
          ,SUM(rcf.ALLOCATED_ITEM_REFUND)   AS REFUNDAMOUNT 
         FROM DATAWAREHOUSE.REFUNDSCHEQUEBYITEM_FACT        rcf                      
          WHERE rcf.DW_ISCURRENTROW
            AND (rcf.FISCALDATE::date >= :startdate::date AND rcf.FISCALDATE::date  <= :enddate::date)
             AND rcf.LOCATION_DIM_FK IN (--351,352
               SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
            GROUP BY rcf.ITEM_FACT_FK
            
          )                                                        rif
                ON itf.ITEM_FACT_NK = rif.ITEM_FACT_FK 
        LEFT JOIN (
        SELECT vcf.ITEM_FACT_FK                            AS ITEM_FACT_FK
          ,SUM(vcf.ALLOCATED_ITEM_VOID  * CASE WHEN vcf.VOID_LEVEL = ''item'' THEN vcf.QUANTITY ELSE 1 END)    AS VOIDAMOUNT 
         FROM DATAWAREHOUSE.VOIDCHECKBYITEM_FACT  vcf                   
          WHERE vcf.DW_ISCURRENTROW
            AND (vcf.FISCAL_DATE::date >= :startdate::date AND vcf.FISCAL_DATE::date  <= :enddate::date)
             AND vcf.LOCATION_DIM_FK IN (--351,352
               SELECT table1.value 
                  FROM table(split_to_table(:locationidS, '',''))  table1)
            GROUP BY vcf.ITEM_FACT_FK
            
          )                                                       vif
                ON itf.ITEM_FACT_NK = vif.ITEM_FACT_FK 
                
                       
ORDER BY "Item ID"         
    ;
    
--=========================================================================================== 
 reportSet:= (
SELECT ROW_NUMBER() OVER (ORDER BY INLT1."Location ID")   AS "Support ID" ,INLT1.* 
  FROM (
    SELECT ''By Category''          AS "Name" 
          ,TNS."By Category"      AS "Name Group"
          ,TNS."Location ID"      AS "Location ID"
          ,SUM(TNS."Voids")       AS "Voids"
          ,SUM(TNS."Sales")       AS "Sales"
          ,SUM(TNS."Discounts")   AS "Discounts"
          ,SUM(TNS."Refunds")     AS "Refunds"
          ,SUM(TNS."Net Sales")   AS "Net Sales"
      FROM TEMP_SALES             TNS
      GROUP BY "Name"
        ,"Name Group"
        ,"Location ID"
    UNION
    SELECT ''By DayPart''           AS "Name" 
          ,TNS."By DayPart"       AS "Name Group"
          ,TNS."Location ID"      AS "Location ID"
          ,SUM(TNS."Voids")       AS "Voids"
          ,SUM(TNS."Sales")       AS "Sales"
          ,SUM(TNS."Discounts")   AS "Discounts"
          ,SUM(TNS."Refunds")     AS "Refunds"
          ,SUM(TNS."Net Sales")   AS "Net Sales"
      FROM TEMP_SALES             TNS
      GROUP BY "Name"
        ,"Name Group"
        ,"Location ID"    
    UNION
    SELECT ''By Order Type''        AS "Name" 
          ,TNS."By Order Type"    AS "Name Group"
          ,TNS."Location ID"      AS "Location ID"
          ,SUM(TNS."Voids")       AS "Voids"
          ,SUM(TNS."Sales")       AS "Sales"
          ,SUM(TNS."Discounts")   AS "Discounts"
          ,SUM(TNS."Refunds")     AS "Refunds"
          ,SUM(TNS."Net Sales")   AS "Net Sales"
      FROM TEMP_SALES             TNS
      GROUP BY "Name"
        ,"Name Group"
        ,"Location ID"   
 UNION
    SELECT ''By Revenue Center''             AS "Name" 
          ,TNS."By Revenue Center"         AS "Name Group"
          ,TNS."Location ID"               AS "Location ID"
          ,SUM(TNS."Voids")                AS "Voids"
          ,SUM(TNS."Sales")                AS "Sales"
          ,SUM(TNS."Discounts")            AS "Discounts"
          ,SUM(TNS."Refunds")              AS "Refunds"
          ,SUM(TNS."Net Sales")            AS "Net Sales"
      FROM TEMP_SALES                      TNS
      GROUP BY "Name"
        ,"Name Group"
        ,"Location ID"           
 )  INLT1
);

--===========================================================================================
RETURN TABLE(reportSet); 

END';